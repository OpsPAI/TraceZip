package org.example.mysql;

import java.sql.*;
import java.util.Random;
import java.util.concurrent.*;

public class MySQLJDBCExample {

    public static void main(String[] args) {
        Random random = new Random();

        // 随机选择 host 和 port
        String host = "mock" + random.nextInt(10) + ".local";
        int port = 3306 + random.nextInt(3); // 3306, 3307, or 3308
        String url = "jdbc:mysql://" + host + ":" + port + "/testdb";

        String user = "root";
        String password = "password";

        int numOperations = 1000000;
        ExecutorService executor = Executors.newFixedThreadPool(10); // 使用 10 个线程的线程池
        CompletableFuture<Void>[] futures = new CompletableFuture[numOperations];

        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            if (conn != null) {
                System.out.println("Connected to the database");

                // 创建表
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("CREATE TABLE IF NOT EXISTS testtable (" +
                            "id INT AUTO_INCREMENT PRIMARY KEY, " +
                            "name VARCHAR(255), " +
                            "age INT, " +
                            "city VARCHAR(255))");
                }

                String[] names = new String[1000];
                for (int i = 0; i < 1000; i++) {
                    names[i] = "Person" + i;
                }

                // 提交 1000 个操作任务到线程池
                for (int i = 0; i < numOperations; i++) {
                    int finalI = i;
                    futures[i] = CompletableFuture.runAsync(() -> {
                        try (Connection connection = DriverManager.getConnection(url, user, password)) {
                            int operation = random.nextInt(6); // 0: 插入, 1: 查询, 2: 删除, 3: 更新, 4: 批量插入, 5: 复杂查询
                            String name = names[random.nextInt(names.length)];
                            int age = random.nextInt(100);
                            String city = "City" + random.nextInt(100);

                            switch (operation) {
                                case 0:
                                    // 插入
                                    String insertSQL = "INSERT INTO testtable (name, age, city) VALUES (?, ?, ?)";
                                    try (PreparedStatement pstmt = connection.prepareStatement(insertSQL)) {
                                        pstmt.setString(1, name);
                                        pstmt.setInt(2, age);
                                        pstmt.setString(3, city);
                                        pstmt.executeUpdate();
                                        System.out.println("Inserted: " + name);
                                    }
                                    break;
                                case 1:
                                    // 查询
                                    String querySQL = "SELECT * FROM testtable WHERE name = ?";
                                    try (PreparedStatement pstmt = connection.prepareStatement(querySQL)) {
                                        pstmt.setString(1, name);
                                        ResultSet rs = pstmt.executeQuery();
                                        if (rs.next()) {
                                            System.out.println("Found: " + rs.getString("name") + " [Query #" + finalI + "]");
                                        } else {
                                            System.out.println("No record found with name: " + name + " [Query #" + finalI + "]");
                                        }
                                    }
                                    break;
                                case 2:
                                    // 删除
                                    String deleteSQL = "DELETE FROM testtable WHERE name = ?";
                                    try (PreparedStatement pstmt = connection.prepareStatement(deleteSQL)) {
                                        pstmt.setString(1, name);
                                        int rowsAffected = pstmt.executeUpdate();
                                        if (rowsAffected > 0) {
                                            System.out.println("Deleted: " + name);
                                        } else {
                                            System.out.println("No record found to delete with name: " + name);
                                        }
                                    }
                                    break;
                                case 3:
                                    // 更新
                                    String updateSQL = "UPDATE testtable SET age = ?, city = ? WHERE name = ?";
                                    try (PreparedStatement pstmt = connection.prepareStatement(updateSQL)) {
                                        pstmt.setInt(1, age);
                                        pstmt.setString(2, city);
                                        pstmt.setString(3, name);
                                        int rowsAffected = pstmt.executeUpdate();
                                        if (rowsAffected > 0) {
                                            System.out.println("Updated: " + name);
                                        } else {
                                            System.out.println("No record found to update with name: " + name);
                                        }
                                    }
                                    break;
                                case 4:
                                    // 批量插入
                                    String batchInsertSQL = "INSERT INTO testtable (name, age, city) VALUES (?, ?, ?)";
                                    try (PreparedStatement pstmt = connection.prepareStatement(batchInsertSQL)) {
                                        connection.setAutoCommit(false);
                                        for (int j = 0; j < 10; j++) {
                                            pstmt.setString(1, names[random.nextInt(names.length)]);
                                            pstmt.setInt(2, random.nextInt(100));
                                            pstmt.setString(3, "City" + random.nextInt(100));
                                            pstmt.addBatch();
                                        }
                                        pstmt.executeBatch();
                                        connection.commit();
                                        connection.setAutoCommit(true);
                                        System.out.println("Batch inserted 10 records");
                                    }
                                    break;
                                case 5:
                                    // 复杂查询
                                    String complexQuerySQL = "SELECT name, COUNT(*) as count FROM testtable GROUP BY name HAVING count > 1";
                                    try (Statement stmt = connection.createStatement();
                                         ResultSet rs = stmt.executeQuery(complexQuerySQL)) {
                                        while (rs.next()) {
                                            System.out.println("Name: " + rs.getString("name") + ", Count: " + rs.getInt("count"));
                                        }
                                    }
                                    break;
                            }
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }, executor);
                }

                // 等待所有任务完成
                CompletableFuture.allOf(futures).join();
            } else {
                System.out.println("Failed to connect to the database");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            executor.shutdown();
        }
    }
}
