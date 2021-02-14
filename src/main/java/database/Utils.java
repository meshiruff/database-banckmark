package database;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.*;

public class Utils {
    public static Connection connectToDB(String jdbc_driver, String db_url, String user, String password) throws ClassNotFoundException, SQLException {
        Connection conn = null;
        Class.forName(jdbc_driver);
        System.out.println("Connecting to database...");
        conn = DriverManager.getConnection(db_url, user, password);
        System.out.println("Creating table in given database...");
        return conn;
    }


    public static void creteTablesWithIndexes(Statement stmt) throws SQLException {
        stmt.executeUpdate(database.Utils.createTable());
        stmt.executeUpdate(database.Utils.createCompanyIndex());
        stmt.executeUpdate(database.Utils.createCreditIndex());
        stmt.executeUpdate(database.Utils.createRateIndex());
        stmt.executeUpdate(database.Utils.createScoreIndex());

    }

    public static void creteTablesWithoutIndexes(Statement stmt) throws SQLException {
        stmt.executeUpdate(database.Utils.createTable());
    }

    public static void loadCsv(Connection conn, int counter, String csv, int batchSize) {
        BufferedReader br = null;
        String csvFile = csv;
        String line = "";
        String cvsSplitBy = ",";
        boolean HeadRowExists = true;
        int AcceptedNumberofColumns = 20;
        int IncorrectRecords = 0;
        String[] person = null;
        try {
            br = new BufferedReader(new FileReader(csvFile));
            if (HeadRowExists) {
                String HeadRow = br.readLine();

                if (HeadRow == null || HeadRow.isEmpty()) {
                    throw new FileNotFoundException(
                            "No columns defined in given CSV file." +
                                    "Please check the CSV file format.");
                }
            }
            int runner = counter;
            int counterBatch = 0;
            PreparedStatement stmt = conn.prepareStatement("insert into PEOPLE (id, first_name,last_name, email,university,company,credit_type,second_email,age,music_genre, mother_name, father_name,mother_email, father_email, country, address, shipping_address, mother_address, father_address, business_address, rate, score) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?)");

            while ((line = br.readLine()) != null) {
                person = line.split(cvsSplitBy);
                if (person.length > 0 && person.length == AcceptedNumberofColumns) {

                    Person e = new Person(Integer.parseInt(person[0]), person[1], person[2], person[3], person[4], person[5], person[6], person[7], Integer.parseInt(person[8]), person[9], person[10], person[11], person[12], person[13], person[14], person[15], person[16], person[17], person[18], person[19]);
                    stmt.setInt(1, runner);
                    stmt.setString(2, e.getFirstname());
                    stmt.setString(3, e.getLastname());
                    stmt.setString(4, e.getEmail());
                    stmt.setString(5, e.getUniversity());
                    stmt.setString(6, e.getCompany());
                    stmt.setString(7, e.getCredit_type());
                    stmt.setString(8, e.getSecond_email());
                    stmt.setInt(9, e.getAge());
                    stmt.setString(10, e.getMusic_genre());
                    stmt.setString(11, e.getMother_name());
                    stmt.setString(12, e.getFather_name());
                    stmt.setString(13, e.getMother_email());
                    stmt.setString(14, e.getFather_email());
                    stmt.setString(15, e.getCountry());
                    stmt.setString(16, e.getAddress());
                    stmt.setString(17, e.getShipping_address());
                    stmt.setString(18, e.getMother_address());
                    stmt.setString(19, e.getFather_address());
                    stmt.setString(20, e.getBusiness_address());
                    stmt.setInt(21, e.getRate());
                    stmt.setInt(22, e.getScore());
                    stmt.addBatch();
                    runner++;
                    counterBatch++;

                    if (counterBatch == batchSize) {
                        System.out.println("batch");
                        counterBatch = 0;
                        stmt.executeBatch();
                        conn.commit();
                        Thread.sleep(2000);
                    }
                } else {
                    IncorrectRecords++;
                }
            }
            stmt.executeBatch();
            conn.commit();
            Thread.sleep(500);

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }


    public static void queryRead2indexes(Statement stmt, int id) throws SQLException {
        System.out.println("queryRead2indexes");
        runQuery(stmt, "SELECT * FROM PEOPLE WHERE company='Ggigspaces' AND credit_type='Peper'", id);
    }

    public static void queryRead2indexes2(Statement stmt, int id) throws SQLException {
        System.out.println("queryRead2indexes");
        runSingleQuery(stmt, "SELECT * FROM PEOPLE WHERE company='Ggigspaces' AND credit_type='Peper'");
    }

    public static void queryRead1index(Statement stmt, int id) throws SQLException {
        System.out.println("queryRead1index");
        runQuery(stmt, "SELECT * FROM PEOPLE WHERE company='Ggigspaces'", id);
    }

    public static void queryRead1index2(Statement stmt) throws SQLException {
        System.out.println("queryRead1index");
        runSingleQuery(stmt, "SELECT * FROM PEOPLE WHERE company='Ggigspaces'");
    }

    public static void queryReadNoIndexes(Statement stmt, int id) throws SQLException {
        System.out.println("queryReadNoIndexes");
        runQuery(stmt, "SELECT * FROM PEOPLE WHERE last_name='Meshi'", id);
    }

    public static void queryReadNoIndexes2(Statement stmt) throws SQLException {
        System.out.println("queryReadNoIndexes");
        runSingleQuery(stmt, "SELECT * FROM PEOPLE WHERE last_name='Meshi'");
    }

    public static void queryReadGroupBy(Statement stmt, int id) throws SQLException {
        System.out.println("queryReadGroupBy");
        runQuery(stmt, "SELECT COUNT(company), company FROM PEOPLE GROUP BY company", id);
    }

    public static void queryAvg(Statement stmt, int id) throws SQLException {
        System.out.println("AVG");
        runQuery(stmt, "SELECT AVG(rate) AS rate_avg FROM PEOPLE WHERE rate > 5", id);
    }

    public static void queryReadGroupBy2(Statement stmt) throws SQLException {
        System.out.println("queryReadGroupBy");
        runSingleQuery(stmt, "SELECT COUNT(company), company FROM PEOPLE GROUP BY company");
    }

    public static void queryReadGJoin(Statement stmt, int id) throws SQLException {
        System.out.println("queryReadGJoin");
        runQuery(stmt, "SELECT p1.id FROM PEOPLE p1 JOIN PEOPLE p2 ON (p1.company = p2.company AND  p2.rate = p1.rate) LIMIT 100000", id);
    }

    public static void queryReadGJoin2(Statement stmt) throws SQLException {
        System.out.println("queryReadGJoin");
        runSingleQuery(stmt, "SELECT p1.id FROM PEOPLE p1 JOIN PEOPLE p2 ON (p1.company = p2.company AND  p2.rate = p1.rate) LIMIT 100000");
    }

    public static void queryRange(Statement stmt, int id) throws SQLException {
        System.out.println("queryRange");
        runQuery(stmt, "SELECT * FROM PEOPLE WHERE rate < 5 AND score < 15 LIMIT 100000", id);
    }

    public static void queryRange2(Statement stmt) throws SQLException {
        System.out.println("queryRange");
        runSingleQuery(stmt, "SELECT * FROM PEOPLE WHERE rate < 5 AND score < 15 LIMIT 100000");
    }



    public static PreparedStatement queryDeleteSingle(int id, Connection conn) throws SQLException {
        PreparedStatement stmt = conn.prepareStatement("DELETE FROM PEOPLE WHERE id=?;");
        stmt.setInt(1, id + 1);

        return stmt;
    }

    public static PreparedStatement queryDeleteBatch(int id, Connection conn) throws SQLException {
        PreparedStatement stmt = conn.prepareStatement("DELETE FROM PEOPLE WHERE id=?;");
        int batchSize = 100000;
        for (int i = 1 + batchSize * id; i <= batchSize + batchSize * id; i++) {
            stmt.setInt(1, i);
            stmt.addBatch();
        }
        return stmt;
    }


    public static PreparedStatement queryInsetSingle(int id, Connection conn) throws SQLException {
//        System.out.println("queryInsetSingle");
        PreparedStatement stmt = conn.prepareStatement("insert into PEOPLE (id, first_name,last_name, email,university,company,credit_type,second_email,age,music_genre, mother_name, father_name,mother_email, father_email, country, address, shipping_address, mother_address, father_address, business_address, rate, score) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?)");
        stmt.setInt(1, id + 1);
        stmt.setString(2, "aaaa");
        stmt.setString(3, "aaaa");
        stmt.setString(4, "aaaa");
        stmt.setString(5, "aaaa");
        stmt.setString(6, "aaaa");
        stmt.setString(7, "aaaa");
        stmt.setString(8, "aaaa");
        stmt.setInt(9, 5);
        stmt.setString(10, "aaaa");
        stmt.setString(11, "aaaa");
        stmt.setString(12, "aaaa");
        stmt.setString(13, "aaaa");
        stmt.setString(14, "aaaa");
        stmt.setString(15, "aaaa");
        stmt.setString(16, "aaaa");
        stmt.setString(17, "aaaa");
        stmt.setString(18, "aaaa");
        stmt.setString(19, "aaaa");
        stmt.setString(20, "aaaa");
        stmt.setInt(21, 5);
        stmt.setInt(22, 22);
        return stmt;
    }

    public static PreparedStatement queryInsertBatch(int id, Connection conn) throws SQLException {
//        System.out.println("queryInsetSingle");
        int batchSize = 1000;
        PreparedStatement stmt = conn.prepareStatement("insert into PEOPLE (id, first_name,last_name, email,university,company,credit_type,second_email,age,music_genre, mother_name, father_name,mother_email, father_email, country, address, shipping_address, mother_address, father_address, business_address, rate, score) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?)");
        for (int i = 1 + batchSize * id; i <= batchSize + batchSize * id; i++) {
            stmt.setInt(1, i);
            stmt.setString(2, "aaaa");
            stmt.setString(3, "aaaa");
            stmt.setString(4, "aaaa");
            stmt.setString(5, "aaaa");
            stmt.setString(6, "aaaa");
            stmt.setString(7, "aaaa");
            stmt.setString(8, "aaaa");
            stmt.setInt(9, 5);
            stmt.setString(10, "aaaa");
            stmt.setString(11, "aaaa");
            stmt.setString(12, "aaaa");
            stmt.setString(13, "aaaa");
            stmt.setString(14, "aaaa");
            stmt.setString(15, "aaaa");
            stmt.setString(16, "aaaa");
            stmt.setString(17, "aaaa");
            stmt.setString(18, "aaaa");
            stmt.setString(19, "aaaa");
            stmt.setString(20, "aaaa");
            stmt.setInt(21, 5);
            stmt.setInt(22, 22);
            stmt.addBatch();
        }
        return stmt;
    }


    public static void runQuery(Statement stmt, String query, int id) throws SQLException {
        long startTime = System.currentTimeMillis();
        boolean iterate = false;
        int counter = 0;
        int timeout = 3 * 60;
        int totalLatency = 0;
        int totalLatencyWithIterator = 0;
        int totalResultSet = 0;
        while ((System.currentTimeMillis() - startTime) < timeout * 1000) {
            long startTimeLatency = System.currentTimeMillis();
            ResultSet resultSet = stmt.executeQuery(query);

            totalLatency += System.currentTimeMillis() - startTimeLatency;
            int resultSetCounter = 0;
            while (resultSet.next()) {
                resultSetCounter++;
            }
            totalResultSet = resultSetCounter;

            totalLatencyWithIterator += System.currentTimeMillis() - startTimeLatency;
            counter++;
        }
        System.out.println("id " + id + " counter " + counter);
//            System.out.println("id " + id + " totalLatency" +totalLatency);
//            System.out.println("id " + id + " totalLatencyWithIterator" + totalLatencyWithIterator);
//            System.out.println("id " + id + " totalResultSet" + totalResultSet);
//            System.out.println("id " + id + " throughput : " + (double) counter / totalLatency);
//            System.out.println("id " + id + " latency : " +(double) totalLatency / counter);
        System.out.println("id " + id + " latency iterate : " + (double) totalLatencyWithIterator / counter);


    }

    public static void runSingleQuery(Statement stmt, String query) throws SQLException {
        int totalLatencyWithIterator = 0;
        long startTimeLatency = System.currentTimeMillis();
        ResultSet resultSet = stmt.executeQuery(query);
        int resultSetCounter = 0;
        while (resultSet.next()) {
            resultSetCounter++;
        }

        totalLatencyWithIterator += System.currentTimeMillis() - startTimeLatency;
        System.out.println("latency iterate : " + (double) totalLatencyWithIterator);
        System.out.println("resultSetCounter : " + resultSetCounter);

    }




    public static void runQueryInsert(Connection conn, boolean isSingle, int id, int start) throws SQLException {
        System.out.println("insert");
        long startTime = System.currentTimeMillis();
        int startFrom = start * 500000;
        int counter = 0;
        int timeout = 10;
        int totalLatency = 0;
        while ((System.currentTimeMillis() - startTime) < timeout * 1000) {
            long startTimeLatency = System.currentTimeMillis();
            if (isSingle) {
                PreparedStatement statement = queryInsetSingle(startFrom, conn);
                statement.execute();
            } else {
                PreparedStatement statement = queryInsertBatch(startFrom, conn);
                statement.executeBatch();
                conn.commit();
            }
            totalLatency += System.currentTimeMillis() - startTimeLatency;
            counter++;
            startFrom++;
        }

        System.out.println("id " + id + " counter " + counter);
//        System.out.println("id "+ id + " totalLatency " +totalLatency);
//        System.out.println("id "+ id +  " throughput : " + ((double) counter / timeout));
        System.out.println("id " + id + " latency : " + ((double) totalLatency / counter));
    }

    public static void runQueryDelete(Connection conn, boolean isSingle) throws SQLException {
        System.out.println("delete");
        long startTime = System.currentTimeMillis();
        int counter = 0;
        int timeout = 3 * 60;
        int totalLatency = 0;
        while ((System.currentTimeMillis() - startTime) < timeout * 1000) {
            long startTimeLatency = System.currentTimeMillis();
            if (isSingle) {
                PreparedStatement statement = queryDeleteSingle(counter, conn);
                statement.execute();
            } else {
                PreparedStatement statement = queryDeleteBatch(counter, conn);
                int[] ints = statement.executeBatch();
                System.out.println(ints.length);
            }
            totalLatency += System.currentTimeMillis() - startTimeLatency;
            counter++;
        }

        System.out.println(counter);
        System.out.println(totalLatency);

        System.out.println("throughput : " + ((double) counter / timeout));
        System.out.println("latency : " + ((double) totalLatency / counter));
    }

    public static String createTable() {
        return "CREATE TABLE PEOPLE " +
                "(id INTEGER not NULL, " +
                " first_name VARCHAR(255), " +
                " last_name VARCHAR(255), " +
                " email VARCHAR(255), " +
                " university VARCHAR(255), " +
                " company VARCHAR(255), " +
                " credit_type VARCHAR(255), " +
                " second_email VARCHAR(255), " +
                " age INTEGER, " +
                " music_genre VARCHAR(255), " +
                " mother_name VARCHAR(255), " +
                " father_name VARCHAR(255), " +
                " mother_email VARCHAR(255), " +
                " father_email VARCHAR(255), " +
                " country VARCHAR(255), " +
                " address VARCHAR(255), " +
                " shipping_address VARCHAR(255), " +
                " mother_address VARCHAR(255), " +
                " father_address VARCHAR(255), " +
                " business_address VARCHAR(255), " +
                " rate INTEGER, " +
                " score INTEGER, " +
                " PRIMARY KEY ( id ))";

    }

    public static String createCompanyIndex() {
        return "CREATE INDEX company_index1 ON PEOPLE(company)";
    }

    public static String createCreditIndex() {
        return "CREATE INDEX credit_type_index1 ON PEOPLE(credit_type)";
    }

    public static String createRateIndex() {
        return "CREATE INDEX rate_index1 ON PEOPLE(rate)";
    }

    public static String createScoreIndex() {
        return "CREATE INDEX score_index1 ON PEOPLE(score)";
    }


}


