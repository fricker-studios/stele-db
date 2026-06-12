// JDBC driver gate (STL-184).
//
// Proves the official PostgreSQL JDBC driver (pgjdbc) runs a parameterized
// prepared query against Stele end-to-end: connect, create a table, insert rows
// through `?` placeholders, then execute a prepared `SELECT … WHERE id = ?`
// repeatedly and assert each returned value. This is one half of the v0.2
// milestone exit criterion ("a JDBC/psycopg driver can run a parameterized
// query"); the psycopg half is ci/psycopg-smoke.py.
//
// The SELECT loop runs past pgjdbc's `prepareThreshold` (default 5), so the
// driver switches from the unnamed statement to a *named* server-side prepared
// statement mid-loop — exercising the [STL-182] statement cache and, once
// named, pgjdbc's binary result transfer ([STL-183]).
//
// `assumeMinServerVersion=9.4` makes pgjdbc send its session defaults
// (extra_float_digits, application_name) as startup-packet parameters instead
// of a post-connect `SET …` round trip — Stele has no `SET` yet (v0.3 surface).
//
// Run via ci/jdbc-smoke.sh (which pins + verifies the pgjdbc jar), or directly:
//   java -cp postgresql-<ver>.jar ci/JdbcSmoke.java [host] [port]
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public final class JdbcSmoke {
    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : "localhost";
        String port = args.length > 1 ? args[1] : "5454";
        String url = "jdbc:postgresql://" + host + ":" + port + "/stele"
                + "?assumeMinServerVersion=9.4";

        try (Connection conn = connectWithRetry(url)) {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS driver_demo_jdbc");
                st.execute("CREATE TABLE driver_demo_jdbc (id INT PRIMARY KEY, label TEXT)"
                        + " WITH SYSTEM VERSIONING");
            }

            // Parameterized INSERT statements: pgjdbc binds the values over the
            // wire and expects the `INSERT 0 1` command tag for the update count.
            try (PreparedStatement ins =
                    conn.prepareStatement("INSERT INTO driver_demo_jdbc VALUES (?, ?)")) {
                insertRow(ins, 1, "alpha");
                insertRow(ins, 2, "beta");
            }

            // The exit-criterion query: a prepared parameterized SELECT, executed
            // more times than prepareThreshold so pgjdbc promotes it to a named
            // server-side statement (and binary transfer) partway through.
            try (PreparedStatement sel = conn.prepareStatement(
                    "SELECT label FROM driver_demo_jdbc WHERE id = ?")) {
                for (int round = 1; round <= 7; round++) {
                    int wantedId = (round % 2 == 0) ? 2 : 1;
                    String wantedLabel = (wantedId == 2) ? "beta" : "alpha";
                    sel.setInt(1, wantedId);
                    try (ResultSet rs = sel.executeQuery()) {
                        if (!rs.next()) {
                            fail("round " + round + ": WHERE id = " + wantedId
                                    + " returned no rows, expected '" + wantedLabel + "'");
                        }
                        String label = rs.getString(1);
                        if (!wantedLabel.equals(label)) {
                            fail("round " + round + ": WHERE id = " + wantedId
                                    + " returned '" + label + "', expected '" + wantedLabel + "'");
                        }
                        if (rs.next()) {
                            fail("round " + round + ": WHERE id = " + wantedId
                                    + " returned more than one row");
                        }
                    }
                }
            }

            System.out.println("PASS: pgjdbc "
                    + conn.getMetaData().getDriverVersion()
                    + " ran a parameterized prepared query");
        }
    }

    private static void insertRow(PreparedStatement ins, int id, String label)
            throws SQLException {
        ins.setInt(1, id);
        ins.setString(2, label);
        int n = ins.executeUpdate();
        if (n != 1) {
            fail("INSERT (" + id + ", '" + label + "') reported " + n + " rows, expected 1");
        }
    }

    /** Wait for the engine to accept connections (cold container boot). */
    private static Connection connectWithRetry(String url) throws Exception {
        long deadline = System.nanoTime() + 60L * 1_000_000_000L;
        while (true) {
            try {
                return DriverManager.getConnection(url, "stele", "");
            } catch (SQLException e) {
                if (System.nanoTime() >= deadline) {
                    throw e;
                }
                Thread.sleep(1000);
            }
        }
    }

    private static void fail(String message) {
        System.err.println("FAIL: " + message);
        System.exit(1);
    }
}
