package database;

import java.sql.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CreateFileH2 {
    static final String JDBC_DRIVER = "org.h2.Driver";
    static final String DB_URL = "jdbc:h2:~/testh33;CACHE_SIZE=5000;QUERY_CACHE_SIZE=0;";
    static final String USER = "sa";
    static final String PASS = "";
    static final int MAX_THREADS = 2;

    public static void main(String[] args) {
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = Utils.connectToDB(JDBC_DRIVER, DB_URL, USER, PASS);
            stmt = conn.createStatement();
//            Utils.queryRange(stmt);
//            Utils.queryReadGroupBy(stmt);
//            Utils.queryReadGJoin(stmt);

            Utils.creteTablesWithIndexes(stmt);
//            conn.setAutoCommit(false);
//            for (int i =0 ; i< 3; i++){
//                System.out.println(i);
//                database.Utils.loadCsv(conn, i* 500000 + 1, "/home/ec2-user/person.csv", 100000);
//            }
//            conn.setAutoCommit(true);

//            Utils.queryRead2indexes2(stmt);
//            conn.setAutoCommit(true);
//            Utils.queryRead2indexes(stmt);
//            Utils.queryRead1index(stmt);
//            Utils.queryReadNoIndexes(stmt);
//            Utils.queryRange(stmt);
//            Utils.queryReadGroupBy(stmt);
//            Utils.queryReadGJoin(stmt);

//            runThreads(conn,4, 20);
//            runThreads(conn,8, 50);
//            runThreads(conn,16, 90);
            stmt.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null) stmt.close();
                if (conn != null) conn.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        System.out.println("Goodbye!");
    }
}


