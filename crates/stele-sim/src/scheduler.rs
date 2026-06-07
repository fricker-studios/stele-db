//! Cooperative deterministic scheduler ([STL-108]).
//!
//! A single-threaded executor that drives `Future`s in a *seed-determined* order.
//! There is no OS threading and no real time: tasks make progress only when the
//! scheduler polls them, and time only moves when [`VirtualClock`] is told to. On
//! each step the scheduler picks one *ready* task uniformly at random via the
//! [`SeededRng`] ([`Scheduler::select_random`]) and polls it once. When no task is
//! ready, it jumps the clock to the nearest sleeper's deadline and wakes everyone
//! due. Same seed ⇒ identical interleaving ⇒ byte-identical event trace; different
//! seeds explore different interleavings — exactly the property the
//! deterministic-simulation strategy is built on
//! ([docs/06-testing-strategy.md §5](../../../docs/06-testing-strategy.md),
//! [ADR-0010](../../../docs/adr/0010-deterministic-simulation-testing.md)).
//!
//! Tasks cooperate through three free functions, valid only while a task is being
//! polled inside [`Scheduler::run`]: [`yield_now`] (give other ready tasks a turn),
//! [`sleep`] (block until virtual time advances), and [`record`] (append to the
//! observable event log). They reach the running scheduler through a thread-local,
//! so the futures stay `'static` and capture no borrows.

use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use crate::chacha::SeededRng;
use crate::clock::VirtualClock;

/// Identifies a spawned task — its index in spawn order.
pub type TaskId = usize;

/// One entry in a run's observable trace: which task ran, the tag it recorded,
/// and the virtual time when it did. The sequence of these *is* the schedule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Event {
    /// The task that emitted this event.
    pub task: TaskId,
    /// A task-supplied tag (e.g. a step counter).
    pub tag: u64,
    /// Virtual time (microseconds) at which it was recorded.
    pub at: i64,
}

/// A task waiting for virtual time to reach `wake_at`.
struct Sleeper {
    wake_at: i64,
    task: TaskId,
}

/// Scheduler state reachable from inside a task poll (via the thread-local).
/// Deliberately does **not** hold the task futures themselves — those live in
/// [`Scheduler::tasks`] so a future can be taken out and polled without keeping a
/// borrow of this state alive (which the future would re-enter).
struct State {
    rng: SeededRng,
    clock: VirtualClock,
    ready: Vec<TaskId>,
    sleepers: Vec<Sleeper>,
    log: Vec<Event>,
    /// The task currently being polled — the implicit subject of the task-side
    /// primitives ([`yield_now`] / [`sleep`] / [`record`]).
    current: TaskId,
}

thread_local! {
    /// The scheduler active on this thread, installed for the duration of
    /// [`Scheduler::run`]. `None` outside a run.
    static ACTIVE: RefCell<Option<Rc<RefCell<State>>>> = const { RefCell::new(None) };
}

/// Run `f` against the active scheduler's state. Panics if called outside a poll
/// driven by [`Scheduler::run`].
fn with_state<R>(f: impl FnOnce(&mut State) -> R) -> R {
    let rc = ACTIVE.with(|cell| {
        cell.borrow()
            .as_ref()
            .cloned()
            .expect("sim primitive (yield_now/sleep/record) used outside Scheduler::run")
    });
    let mut st = rc.borrow_mut();
    f(&mut st)
}

/// Append an event to the run's trace, stamped with the current task and virtual
/// time. The ordering of these calls across tasks is the schedule under test.
pub fn record(tag: u64) {
    with_state(|st| {
        let event = Event {
            task: st.current,
            tag,
            at: st.clock.now_micros(),
        };
        st.log.push(event);
    });
}

/// Yield control: the current task becomes ready again and the scheduler is free
/// to run any other ready task first. Resolves on the next poll.
pub const fn yield_now() -> YieldNow {
    YieldNow { yielded: false }
}

/// Future returned by [`yield_now`].
#[must_use = "futures do nothing unless awaited"]
pub struct YieldNow {
    yielded: bool,
}

impl Future for YieldNow {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        if self.yielded {
            Poll::Ready(())
        } else {
            self.yielded = true;
            with_state(|st| st.ready.push(st.current));
            Poll::Pending
        }
    }
}

/// Sleep until virtual time has advanced by `micros`. The task parks until the
/// scheduler, finding nothing else ready, jumps the clock to its deadline.
pub const fn sleep(micros: i64) -> Sleep {
    Sleep {
        dur: micros,
        scheduled: false,
    }
}

/// Future returned by [`sleep`].
#[must_use = "futures do nothing unless awaited"]
pub struct Sleep {
    dur: i64,
    scheduled: bool,
}

impl Future for Sleep {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        if self.scheduled {
            // A parked task is only ever re-polled once the scheduler has woken
            // it, i.e. once virtual time has reached the deadline.
            Poll::Ready(())
        } else {
            self.scheduled = true;
            let dur = self.dur;
            with_state(|st| {
                let now = st.clock.now_micros();
                let wake_at = now.saturating_add(dur);
                if wake_at <= now {
                    // A non-positive sleep can't park: sleepers only wake once
                    // the ready set drains, so it could starve behind tasks that
                    // keep yielding. Treat it as an immediate re-queue instead.
                    st.ready.push(st.current);
                } else {
                    st.sleepers.push(Sleeper {
                        wake_at,
                        task: st.current,
                    });
                }
            });
            Poll::Pending
        }
    }
}

/// A cooperative, deterministic, single-threaded executor.
pub struct Scheduler {
    tasks: Vec<Option<Pin<Box<dyn Future<Output = ()>>>>>,
    state: Rc<RefCell<State>>,
}

/// What the run loop does next on a given turn.
enum Step {
    /// Poll this ready task.
    Poll(TaskId),
    /// The clock was advanced and sleepers woken; loop again.
    Advanced,
    /// Nothing ready and no sleepers — the run is complete.
    Done,
}

impl Scheduler {
    /// A fresh scheduler seeded with `seed`. The virtual clock starts at 0.
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self {
            tasks: Vec::new(),
            state: Rc::new(RefCell::new(State {
                rng: SeededRng::new(seed),
                clock: VirtualClock::new(0),
                ready: Vec::new(),
                sleepers: Vec::new(),
                log: Vec::new(),
                current: 0,
            })),
        }
    }

    /// A handle to the virtual clock, e.g. to hand to a system under test so it
    /// observes the same advancing time the scheduler drives.
    #[must_use]
    pub fn clock(&self) -> VirtualClock {
        self.state.borrow().clock.clone()
    }

    /// Spawn a task. It starts ready and is assigned the next [`TaskId`].
    pub fn spawn<F: Future<Output = ()> + 'static>(&mut self, fut: F) -> TaskId {
        let id = self.tasks.len();
        self.tasks.push(Some(Box::pin(fut)));
        self.state.borrow_mut().ready.push(id);
        id
    }

    /// Choose a ready task uniformly at random via the seeded RNG, removing it
    /// from the ready set. Precondition: the ready set is non-empty.
    fn select_random(st: &mut State) -> TaskId {
        let idx = st.rng.below_usize(st.ready.len());
        st.ready.swap_remove(idx)
    }

    /// Drive every spawned task to completion and return the event trace.
    ///
    /// Deterministic in the seed: the same seed produces a byte-identical trace
    /// (after [`crate::encode_events`]), and different seeds generally produce
    /// different ones.
    #[must_use]
    pub fn run(mut self) -> Vec<Event> {
        ACTIVE.with(|cell| *cell.borrow_mut() = Some(Rc::clone(&self.state)));
        // Clear the thread-local even if a task panics mid-poll — otherwise a
        // later run on this thread would see a stale "active scheduler".
        let _active = ActiveGuard;

        // A no-op waker: this executor decides re-polling from the ready/sleeper
        // sets a task registers before returning `Pending`, not from waker calls.
        let mut cx = Context::from_waker(Waker::noop());

        loop {
            let step = {
                let mut st = self.state.borrow_mut();
                if st.ready.is_empty() {
                    match st.sleepers.iter().map(|s| s.wake_at).min() {
                        None => Step::Done,
                        Some(wake_at) => {
                            st.clock.advance_to(wake_at);
                            let now = st.clock.now_micros();
                            // Wake every task due at `now`, in task-id order so a
                            // tie at the same deadline is resolved deterministically.
                            let mut due: Vec<TaskId> = st
                                .sleepers
                                .iter()
                                .filter(|s| s.wake_at <= now)
                                .map(|s| s.task)
                                .collect();
                            st.sleepers.retain(|s| s.wake_at > now);
                            due.sort_unstable();
                            st.ready.extend(due);
                            Step::Advanced
                        }
                    }
                } else {
                    Step::Poll(Self::select_random(&mut st))
                }
            };

            let tid = match step {
                Step::Done => {
                    // Nothing is ready and nothing is sleeping. If any task is
                    // still live it has parked itself without registering to be
                    // re-polled (returned `Pending` without `yield_now`/`sleep`,
                    // or is genuinely deadlocked) — fail loudly rather than
                    // return a silently truncated trace.
                    let stuck = self.tasks.iter().filter(|t| t.is_some()).count();
                    assert!(
                        stuck == 0,
                        "scheduler stalled with {stuck} unfinished task(s): a task returned \
                         Pending without yielding or sleeping"
                    );
                    break;
                }
                Step::Advanced => continue,
                Step::Poll(tid) => tid,
            };

            self.state.borrow_mut().current = tid;
            let mut fut = self.tasks[tid]
                .take()
                .expect("a ready task always has a live future");
            if fut.as_mut().poll(&mut cx).is_pending() {
                // Still running: put it back. It has already registered itself in
                // the ready set (yield) or the sleepers (sleep).
                self.tasks[tid] = Some(fut);
            }
        }

        std::mem::take(&mut self.state.borrow_mut().log)
    }
}

/// Clears the [`ACTIVE`] thread-local when [`Scheduler::run`] returns or unwinds,
/// so a panic in one run can't leave a stale scheduler visible to the next.
struct ActiveGuard;

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        ACTIVE.with(|cell| *cell.borrow_mut() = None);
    }
}

/// Serialize an event trace to bytes so two runs can be compared byte-for-byte
/// (the DoD's "byte-identical event logs"). Each event is 24 bytes: task, tag,
/// `at`, each a little-endian 64-bit field.
#[must_use]
pub fn encode_events(log: &[Event]) -> Vec<u8> {
    let mut out = Vec::with_capacity(log.len() * 24);
    for e in log {
        let task = u64::try_from(e.task).expect("task id fits u64");
        out.extend_from_slice(&task.to_le_bytes());
        out.extend_from_slice(&e.tag.to_le_bytes());
        out.extend_from_slice(&e.at.to_le_bytes());
    }
    out
}

/// Build the canonical demo trace for `seed`.
///
/// Four cooperating tasks, each taking six steps, interleaved by the seeded
/// scheduler. One step per task is a [`sleep`] (durations 1..=4, exercising the
/// virtual clock and the wake path); the rest are cooperative [`yield_now`]s.
#[must_use]
pub fn schedule_trace(seed: u64) -> Vec<Event> {
    const TASKS: usize = 4;
    const STEPS: u64 = 6;

    let mut sched = Scheduler::new(seed);
    for t in 0..TASKS {
        let dur = i64::try_from(t).expect("task count is small") + 1;
        sched.spawn(async move {
            for step in 0..STEPS {
                record(step);
                if step == STEPS / 2 {
                    sleep(dur).await;
                } else {
                    yield_now().await;
                }
            }
        });
    }
    sched.run()
}

/// Run the demo schedule for `seed` and return its byte-encoded trace — the unit
/// the determinism property is stated over.
#[must_use]
pub fn run_schedule_seed(seed: u64) -> Vec<u8> {
    encode_events(&schedule_trace(seed))
}

/// FNV-1a digest of an encoded schedule trace — a single `u64` to fold into the
/// sim sweep alongside the storage-scenario digests.
///
/// Callers that also need the raw trace (e.g. to count distinct schedules) should
/// run the demo once and pass its bytes here, rather than calling
/// [`run_schedule_seed_digest`] separately.
#[must_use]
pub fn trace_digest(trace: &[u8]) -> u64 {
    const OFFSET: u64 = 0xCBF2_9CE4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01B3;
    let mut hash = OFFSET;
    for &b in trace {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

/// FNV-1a digest of the demo schedule for `seed`.
#[must_use]
pub fn run_schedule_seed_digest(seed: u64) -> u64 {
    trace_digest(&run_schedule_seed(seed))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn same_seed_byte_identical_trace() {
        for seed in [0u64, 1, 42, 999, u64::MAX] {
            assert_eq!(
                run_schedule_seed(seed),
                run_schedule_seed(seed),
                "seed {seed} was not reproducible"
            );
        }
    }

    #[test]
    fn distinct_seeds_explore_distinct_schedules() {
        let distinct: HashSet<Vec<u8>> = (0..100u64).map(run_schedule_seed).collect();
        // DoD: 100 seeds yield many distinct event orderings. Interleaving four
        // tasks of six steps under random selection is highly diverse, so the
        // count is near 100; assert a comfortable lower bound to stay non-flaky.
        assert!(
            distinct.len() > 80,
            "only {} distinct schedules across 100 seeds",
            distinct.len()
        );
    }

    #[test]
    fn every_step_of_every_task_runs() {
        // 4 tasks * 6 steps, regardless of how they interleave.
        let trace = schedule_trace(7);
        assert_eq!(trace.len(), 24);
        for task in 0..4 {
            let steps: Vec<u64> = trace
                .iter()
                .filter(|e| e.task == task)
                .map(|e| e.tag)
                .collect();
            assert_eq!(
                steps,
                vec![0, 1, 2, 3, 4, 5],
                "task {task} ran out of order"
            );
        }
    }

    #[test]
    fn clock_advances_only_via_sleeps() {
        let trace = schedule_trace(3);
        // The clock starts at 0 and only moves when a task sleeps. By the end,
        // the four sleeps (durations 1..=4) must have pushed virtual time past 0.
        let max_at = trace.iter().map(|e| e.at).max().expect("non-empty trace");
        assert!(max_at > 0, "virtual clock never advanced");
        // Every timestamp is non-negative and non-decreasing per task is not
        // guaranteed across tasks, but the global max is bounded by the total
        // sleep budget (1+2+3+4 is not summed — sleeps overlap — so the bound is
        // the largest single deadline observed). Just assert sanity here.
        assert!(trace.iter().all(|e| e.at >= 0));
    }

    #[test]
    fn empty_scheduler_runs_clean() {
        let sched = Scheduler::new(123);
        assert!(sched.run().is_empty());
    }

    #[test]
    fn single_task_completes() {
        let mut sched = Scheduler::new(1);
        sched.spawn(async {
            record(10);
            yield_now().await;
            record(20);
            sleep(5).await;
            record(30);
        });
        let trace = sched.run();
        assert_eq!(
            trace.iter().map(|e| e.tag).collect::<Vec<_>>(),
            vec![10, 20, 30]
        );
        // The final record happened after a sleep(5) from t=0.
        assert_eq!(trace.last().unwrap().at, 5);
    }

    #[test]
    fn zero_sleep_does_not_starve() {
        // One task sleeps(0) while another keeps yielding. If sleep(0) parked in
        // `sleepers` it could starve (sleepers only wake when `ready` drains);
        // it must instead re-queue as ready and complete.
        let mut sched = Scheduler::new(2);
        sched.spawn(async {
            sleep(0).await;
            record(100);
        });
        sched.spawn(async {
            for _ in 0..3 {
                yield_now().await;
            }
        });
        let trace = sched.run();
        assert!(
            trace.iter().any(|e| e.tag == 100),
            "the sleep(0) task never ran"
        );
    }

    /// A future that parks forever without registering to be re-polled.
    struct Stuck;
    impl Future for Stuck {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
            Poll::Pending
        }
    }

    #[test]
    #[should_panic(expected = "scheduler stalled with 1 unfinished task")]
    fn stuck_task_fails_loudly() {
        let mut sched = Scheduler::new(1);
        sched.spawn(Stuck);
        let _ = sched.run();
    }

    #[test]
    fn panic_in_task_clears_active() {
        // A panicking task must not leave the thread-local scheduler installed.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut sched = Scheduler::new(1);
            sched.spawn(async {
                record(1);
                panic!("boom");
            });
            let _ = sched.run();
        }));
        assert!(result.is_err(), "the task panic should propagate");
        // A fresh run on the same thread must work — proving ACTIVE was cleared.
        assert_eq!(schedule_trace(5).len(), 24);
    }
}
