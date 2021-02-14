package database;

import org.sqlite.SQLiteConfig;
import org.sqlite.javax.SQLiteConnectionPoolDataSource;

import java.sql.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CreateSQLiteDB {
    static final String JDBC_DRIVER = "org.sqlite.JDBC";
    static final String DB_URL = "jdbc:sqlite:test.db";
    static final String USER = "sa";
    static final String PASS = "";

    public static void main(String[] args) {
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = Utils.connectToDB(JDBC_DRIVER, DB_URL, USER, PASS);
//            org.sqlite.SQLiteConfig config = new org.sqlite.SQLiteConfig();
//            config.setJournalMode(SQLiteConfig.JournalMode.WAL);
//            conn = config.createConnection(DB_URL);
            stmt = conn.createStatement();
            Utils.creteTablesWithIndexes(stmt);
//            conn.setAutoCommit(false);
//            for (int i = 0; i < 10; i++) {
//                System.out.println(i);
//                database.Utils.loadCsv(conn, i * 500000 + 1, "/home/ec2-user/person.csv", 200000);
//            }
//            conn.setAutoCommit(true);
//

        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    static class Multi implements Runnable {
        Connection conn;
        int id;
        int start;

        public Multi(Connection conn, int id, int start) {
            this.conn = conn;
            this.id = id;
            this.start = start;
        }

        public void run() {
            try {
                try {
                    conn = Utils.connectToDB(JDBC_DRIVER, DB_URL, USER, PASS);
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }

                System.out.println(id + " thread is running...");
                Statement statement = conn.createStatement();
                if (id == 1) {
                    conn.setAutoCommit(false);
                    Utils.runQueryInsert(conn, false, id, start);

                } else {
                    Utils.queryAvg(statement, id);
                }
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }

    }


    public static void runThreads(Connection conn, int numThread, int startFrom) {
        ExecutorService pool = Executors.newFixedThreadPool(numThread);

        for (int i = 0; i < numThread; i++) {
            Runnable multi = new Multi(conn, i, startFrom);
            pool.execute(multi);
        }

        pool.shutdown();

    }

}



