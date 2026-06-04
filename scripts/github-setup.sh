#!/usr/bin/env bash
# One-time GitHub repo setup for fricker-studios/stele-db.
# Requires the GitHub CLI (`gh`) authenticated with admin on the repo: `gh auth login`.
# Review, then run:  bash scripts/github-setup.sh
set -euo pipefail
REPO="fricker-studios/stele-db"

echo "==> Description, homepage, topics"
gh repo edit "$REPO" \
  --description "Append-only, bitemporal, audit-native analytical database engine — time-travel, provenance, and auditability as first-class. Rust, Postgres-wire compatible." \
  --homepage "https://steledb.com" \
  --add-topic database --add-topic rust --add-topic bitemporal \
  --add-topic temporal-database --add-topic audit --add-topic time-travel \
  --add-topic columnar --add-topic olap --add-topic analytics \
  --add-topic postgresql --add-topic append-only --add-topic provenance

echo "==> Merge strategy: squash-only + auto-delete branches; enable Discussions"
gh repo edit "$REPO" \
  --enable-squash-merge --enable-merge-commit=false --enable-rebase-merge=false \
  --delete-branch-on-merge --enable-discussions --enable-issues \
  --enable-projects=false --enable-wiki=false
# Squash commit uses the PR title (Conventional Commits) + PR body
gh api -X PATCH "repos/$REPO" \
  -f squash_merge_commit_title=PR_TITLE -f squash_merge_commit_message=PR_BODY >/dev/null

echo "==> Branch protection on main (solo-pragmatic)"
gh api -X PUT "repos/$REPO/branches/main/protection" --input - >/dev/null <<'JSON'
{
  "required_status_checks": null,
  "enforce_admins": false,
  "required_pull_request_reviews": {
    "required_approving_review_count": 0,
    "dismiss_stale_reviews": true,
    "require_code_owner_reviews": false
  },
  "restrictions": null,
  "required_linear_history": true,
  "allow_force_pushes": false,
  "allow_deletions": false,
  "required_conversation_resolution": true
}
JSON
echo "   NOTE: required status checks are NONE until CI exists. Once the workflows run, add e.g.:"
echo "   gh api -X PATCH repos/$REPO/branches/main/protection/required_status_checks -F strict=true -f 'contexts[]=quick' -f 'contexts[]=test'"

echo "==> Security: Dependabot alerts + fixes, secret scanning + push protection"
gh api -X PUT "repos/$REPO/vulnerability-alerts" >/dev/null
gh api -X PUT "repos/$REPO/automated-security-fixes" >/dev/null
gh api -X PATCH "repos/$REPO" \
  -f 'security_and_analysis[secret_scanning][status]=enabled' \
  -f 'security_and_analysis[secret_scanning_push_protection][status]=enabled' >/dev/null \
  || echo "   (secret scanning needs a public repo or GitHub Advanced Security on private repos)"

echo "==> Labels"
lbl(){ gh label create "$1" --color "$2" --description "$3" --force >/dev/null; }
lbl "bug" d73a4a "Something isn't working"
lbl "feature" 0e8a16 "New capability or enhancement"
lbl "good first issue" 7057ff "Good for newcomers"
lbl "help wanted" 008672 "Extra attention is welcomed"
lbl "docs" 0075ca "Documentation"
lbl "adr" 5319e7 "Architecture Decision Record"
lbl "storage-engine" 1d76db "Storage / on-disk format / WAL"
lbl "query-engine" 1d76db "Parser / planner / executor"
lbl "temporal" fbca04 "Bitemporality / as-of / lineage"
lbl "security" b60205 "Security & compliance"
lbl "performance" fef2c0 "Perf / benchmarks / regressions"
lbl "testing" c2e0c6 "Tests / sim / fuzzing"
lbl "blocked" 000000 "Blocked on something else"

echo "==> Done. Review: https://github.com/$REPO/settings"
