package database;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class CreateDerby {
    static final String JDBC_DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";
    static final String DB_URL = "jdbc:derby:/home/ec2-user/derbytest;create=true;derby.storage.pageSize=600";
    static final String USER = "meshiruff";
    static final String PASS = "password";
    static final int MAX_THREADS = 2;

    public static void main(String args[]) {
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = Utils.connectToDB(JDBC_DRIVER, DB_URL, USER, PASS);

            stmt = conn.createStatement();
//            Utils.creteTablesWithIndexes(stmt);
//            conn.setAutoCommit(false);
//            for (int i = 0; i < 25; i++) {
//                System.out.println(i);
//                database.Utils.loadCsv(conn, i * 500000 + 1, "/home/ec2-user/person.csv", 100000);
//            }


        } catch (Exception e) {
            e.printStackTrace();
        }

    }


}


