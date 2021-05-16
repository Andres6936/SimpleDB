package simpleclient;

import java.sql.*;
import java.util.Scanner;

import simpledb.jdbc.embedded.EmbeddedDriver;
import simpledb.jdbc.network.NetworkDriver;

/**
 * SimpleIJ, is a simplified version of the Derby ij program. One difference
 * from ij is that you can only connect once, at the beginning of the session.
 * When you execute the program, it asks you for a connection string. The
 * syntax of the connection string is similar to that in ij. For example,
 * consider the following SimpleDB connection strings: <br/><br/>
 *
 * <code>
 *    <ul>
 *       <li>jdbc:simpledb:testij</li>
 *       <li>jdbc:simpledb://localhost</li>
 *       <li>jdbc:simpledb://cs.bc.edu</li>
 *    </ul>
 * </code>
 *
 * The first connection string specifies an embedded connection to the “testij”
 * database. Like Derby, the database will be located in the directory of the
 * executing program, which is the SimpleDB Clients project. Unlike Derby,
 * SimpleDB will create the database if it does not exist, so there is no need
 * for an explicit “create ¼ true” flag.
 *
 * The second and third connection strings specify a server-based connection to
 * a SimpleDB server running on the local machine or on cs.bc.edu. Unlike
 * Derby, the connection string does not specify a database. The reason is that
 * the SimpleDB engine can handle only one database at a time, which is
 * specified when the server is started.
 *
 * SimpleIJ repeatedly prints a prompt asking you to enter a single line of
 * text containing an SQL statement. Unlike Derby, the line must contain the
 * entire statement, and no semicolon is needed at the end. The program then
 * executes that statement. If the statement is a query, then the output table
 * is displayed. If the statement is an update command, then the number of
 * affected records is printed. If the statement is ill-formed, then an error
 * message will be printed. SimpleDB understands a very limited subset of SQL,
 * and SimpleIJ will throw an exception if given an SQL statement that the
 * engine does not understand.
 */
public class SimpleIJ {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        System.out.println("Connect> ");
        String s = sc.nextLine();
        Driver d = (s.contains("//")) ? new NetworkDriver() : new EmbeddedDriver();

        try (Connection conn = d.connect(s, null);
             Statement stmt = conn.createStatement()) {
            System.out.print("\nSQL> ");
            while (sc.hasNextLine()) {
                // process one line of input
                String cmd = sc.nextLine().trim();
                if (cmd.startsWith("exit"))
                    break;
                else if (cmd.startsWith("select"))
                    doQuery(stmt, cmd);
                else
                    doUpdate(stmt, cmd);
                System.out.print("\nSQL> ");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        sc.close();
    }

    private static void doQuery(Statement stmt, String cmd) {
        try (ResultSet rs = stmt.executeQuery(cmd)) {
            ResultSetMetaData md = rs.getMetaData();
            int numcols = md.getColumnCount();
            int totalwidth = 0;

            // print header
            for (int i = 1; i <= numcols; i++) {
                String fldname = md.getColumnName(i);
                int width = md.getColumnDisplaySize(i);
                totalwidth += width;
                String fmt = "%" + width + "s";
                System.out.format(fmt, fldname);
            }
            System.out.println();
            for (int i = 0; i < totalwidth; i++)
                System.out.print("-");
            System.out.println();

            // print records
            while (rs.next()) {
                for (int i = 1; i <= numcols; i++) {
                    String fldname = md.getColumnName(i);
                    int fldtype = md.getColumnType(i);
                    String fmt = "%" + md.getColumnDisplaySize(i);
                    if (fldtype == Types.INTEGER) {
                        int ival = rs.getInt(fldname);
                        System.out.format(fmt + "d", ival);
                    } else {
                        String sval = rs.getString(fldname);
                        System.out.format(fmt + "s", sval);
                    }
                }
                System.out.println();
            }
        } catch (SQLException e) {
            System.out.println("SQL Exception: " + e.getMessage());
        }
    }

    private static void doUpdate(Statement stmt, String cmd) {
        try {
            int howmany = stmt.executeUpdate(cmd);
            System.out.println(howmany + " records processed");
        } catch (SQLException e) {
            System.out.println("SQL Exception: " + e.getMessage());
        }
    }
}