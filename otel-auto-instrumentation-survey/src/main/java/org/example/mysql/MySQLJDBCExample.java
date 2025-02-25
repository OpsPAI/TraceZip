package org.example.mysql;

import java.sql.*;
import java.util.Random;
import java.util.concurrent.*;

public class MySQLJDBCExample {

    public static void main(String[] args) {
        Random random = new Random();

        // 随机选择 host 和 port
        String host = "localhost";
        int port = 3306;
        String url = "jdbc:mysql://" + host + ":" + port + "/testdb?useSSL=false&allowPublicKeyRetrieval=true";

        String user = "root";
        String password = "password";

        int numOperations = 500000;
        ExecutorService executor = Executors.newFixedThreadPool(20); // 增大线程池
        CompletableFuture<Void>[] futures = new CompletableFuture[numOperations];

        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            if (conn != null) {
                System.out.println("Connected to the database");

                // 创建表结构
                createTables(conn);

                // 预填充基础数据
                prepopulateData(conn, random);

                // 提交操作任务到线程池
                for (int i = 0; i < numOperations; i++) {
                    int finalI = i;
                    futures[i] = CompletableFuture.runAsync(() -> {
                        try (Connection connection = DriverManager.getConnection(url, user, password)) {
                            executeRandomDatabaseOperation(connection, random, finalI);
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }, executor);
                }

                CompletableFuture.allOf(futures).join();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            executor.shutdown();
        }
    }

    private static void createTables(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            // 创建城市表
            stmt.execute("CREATE TABLE IF NOT EXISTS cities (" +
                    "city_id INT AUTO_INCREMENT PRIMARY KEY, " +
                    "city_name VARCHAR(255) NOT NULL UNIQUE)");

            // 创建产品表（包含库存）
            stmt.execute("CREATE TABLE IF NOT EXISTS products (" +
                    "product_id INT AUTO_INCREMENT PRIMARY KEY, " +
                    "product_name VARCHAR(255) NOT NULL, " +
                    "price DECIMAL(10,2) NOT NULL, " +
                    "stock INT DEFAULT 100)");

            // 创建用户表（关联城市）
            stmt.execute("CREATE TABLE IF NOT EXISTS users (" +
                    "user_id INT AUTO_INCREMENT PRIMARY KEY, " +
                    "name VARCHAR(255) NOT NULL, " +
                    "age INT, " +
                    "city_id INT, " +
                    "FOREIGN KEY (city_id) REFERENCES cities(city_id))");

            // 创建订单表
            stmt.execute("CREATE TABLE IF NOT EXISTS orders (" +
                    "order_id INT AUTO_INCREMENT PRIMARY KEY, " +
                    "user_id INT, " +
                    "product_id INT, " +
                    "quantity INT NOT NULL, " +
                    "order_date DATE, " +
                    "FOREIGN KEY (user_id) REFERENCES users(user_id), " +
                    "FOREIGN KEY (product_id) REFERENCES products(product_id))");

            // 创建索引
            stmt.execute("CREATE INDEX idx_users_city ON users(city_id)");
            stmt.execute("CREATE INDEX idx_orders_user ON orders(user_id)");
        }
    }

    private static void prepopulateData(Connection conn, Random random) throws SQLException {
        // 预填充100个城市
        try (PreparedStatement pstmt = conn.prepareStatement(
                "INSERT IGNORE INTO cities (city_name) VALUES (?)")) {
            for (int i = 0; i < 100; i++) {
                pstmt.setString(1, "City" + i);
                pstmt.addBatch();
            }
            pstmt.executeBatch();
        }

        // 预填充100个产品
        try (PreparedStatement pstmt = conn.prepareStatement(
                "INSERT IGNORE INTO products (product_name, price) VALUES (?, ?)")) {
            for (int i = 0; i < 100; i++) {
                pstmt.setString(1, "Product" + i);
                pstmt.setDouble(2, 10 + random.nextDouble() * 100);
                pstmt.addBatch();
            }
            pstmt.executeBatch();
        }

        // 预填充1000个用户
        try (PreparedStatement pstmt = conn.prepareStatement(
                "INSERT INTO users (name, age, city_id) VALUES (?, ?, ?)")) {
            for (int i = 0; i < 1000; i++) {
                pstmt.setString(1, "User" + i);
                pstmt.setInt(2, 18 + random.nextInt(60));
                pstmt.setInt(3, 1 + random.nextInt(100)); // 城市ID 1-100
                pstmt.addBatch();
                if (i % 500 == 0) pstmt.executeBatch(); // 分批提交
            }
            pstmt.executeBatch();
        }
    }

    private static void executeRandomDatabaseOperation(Connection connection, Random random, int operationId) throws SQLException {
        int operation = random.nextInt(9); // 0-8对应不同操作
        switch (operation) {
            case 0: insertUser(connection, random); break;
            case 1: queryUserOrders(connection, random); break;
            case 2: deleteInactiveUsers(connection, random); break;
            case 3: updateUserCity(connection, random); break;
            case 4: placeOrderWithTransaction(connection, random); break;
            case 5: executeComplexJoinQuery(connection); break;
            case 6: paginateUsersWithWindowFunction(connection, random); break;
            case 7: updateProductPrice(connection, random); break;
            case 8: analyzeCityDemographics(connection); break;
        }
    }

    private static void insertUser(Connection conn, Random random) throws SQLException {
        String sql = "INSERT INTO users (name, age, city_id) VALUES (?, ?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, "User_" + random.nextInt(1000000));
            pstmt.setInt(2, 18 + random.nextInt(60));
            pstmt.setInt(3, 1 + random.nextInt(100));
            pstmt.executeUpdate();
        }
    }

    private static void queryUserOrders(Connection conn, Random random) throws SQLException {
        String sql = "SELECT u.name, COUNT(o.order_id) AS order_count " +
                "FROM users u LEFT JOIN orders o ON u.user_id = o.user_id " +
                "WHERE u.user_id = ? GROUP BY u.user_id";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, 1 + random.nextInt(1000));
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                System.out.printf("User %s has %d orders%n",
                        rs.getString("name"), rs.getInt("order_count"));
            }
        }
    }

    private static void deleteInactiveUsers(Connection conn, Random random) throws SQLException {
        // 使用更高效的 NOT EXISTS 替代 NOT IN
        String sql = "DELETE FROM users WHERE NOT EXISTS " +
                "(SELECT 1 FROM orders WHERE orders.user_id = users.user_id) " +
                "AND age > ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, 30);
            int affected = pstmt.executeUpdate();
            System.out.println("Deleted " + affected + " inactive users");
        }
    }

    private static void updateUserCity(Connection conn, Random random) throws SQLException {
        String sql = "UPDATE users SET city_id = ? WHERE user_id = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, 1 + random.nextInt(100));
            pstmt.setInt(2, 1 + random.nextInt(1000));
            int affected = pstmt.executeUpdate();
            if (affected > 0) System.out.println("Updated user's city");
        }
    }

    private static void placeOrderWithTransaction(Connection conn, Random random) throws SQLException {
        conn.setAutoCommit(false);
        try {
            int productId = 1 + random.nextInt(100);
            int userId = 1 + random.nextInt(1000);
            int quantity = 1 + random.nextInt(5);

            // 检查库存（使用悲观锁）
            try (PreparedStatement checkStock = conn.prepareStatement(
                    "SELECT stock FROM products WHERE product_id = ? FOR UPDATE")) {
                checkStock.setInt(1, productId);
                ResultSet rs = checkStock.executeQuery();

                if (rs.next() && rs.getInt("stock") >= quantity) {
                    // 更新库存
                    try (PreparedStatement updateStock = conn.prepareStatement(
                            "UPDATE products SET stock = stock - ? WHERE product_id = ?")) {
                        updateStock.setInt(1, quantity);
                        updateStock.setInt(2, productId);
                        updateStock.executeUpdate();
                    }

                    // 创建订单
                    try (PreparedStatement insertOrder = conn.prepareStatement(
                            "INSERT INTO orders (user_id, product_id, quantity, order_date) " +
                                    "VALUES (?, ?, ?, CURRENT_DATE)")) {
                        insertOrder.setInt(1, userId);
                        insertOrder.setInt(2, productId);
                        insertOrder.setInt(3, quantity);
                        insertOrder.executeUpdate();
                    }

                    conn.commit();
                    System.out.println("Successfully placed order for " + quantity + " items");
                } else {
                    conn.rollback();
                    System.out.println("Insufficient stock for product " + productId);
                }
            }
        } catch (SQLException e) {
            conn.rollback(); // 显式回滚事务
            System.out.println("Transaction rolled back: " + e.getMessage());
            throw e; // 可选择重新抛出异常或处理
        } finally {
            conn.setAutoCommit(true); // 恢复自动提交
        }
    }

    private static void executeComplexJoinQuery(Connection conn) throws SQLException {
        String sql = "SELECT c.city_name, AVG(u.age) AS avg_age, SUM(o.quantity) AS total_sales " +
                "FROM cities c " +
                "LEFT JOIN users u ON c.city_id = u.city_id " +
                "LEFT JOIN orders o ON u.user_id = o.user_id " +
                "GROUP BY c.city_id " +
                "ORDER BY total_sales DESC LIMIT 5";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                System.out.printf("%s: Avg age %.1f, Sales %d%n",
                        rs.getString("city_name"),
                        rs.getDouble("avg_age"),
                        rs.getInt("total_sales"));
            }
        }
    }

    private static void paginateUsersWithWindowFunction(Connection conn, Random random) throws SQLException {
        String sql = "SELECT user_id, name, age, " +
                "ROW_NUMBER() OVER (ORDER BY user_id) AS row_num " +
                "FROM users ORDER BY user_id LIMIT 10 OFFSET ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, random.nextInt(100) * 10);
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                System.out.printf("Row %d: User %d - %s (Age %d)%n",
                        rs.getInt("row_num"),
                        rs.getInt("user_id"),
                        rs.getString("name"),
                        rs.getInt("age"));
            }
        }
    }

    private static void updateProductPrice(Connection conn, Random random) throws SQLException {
        String sql = "UPDATE products SET price = price * ? WHERE product_id = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setDouble(1, 0.9 + random.nextDouble() * 0.2); // 价格浮动10%
            pstmt.setInt(2, 1 + random.nextInt(100));
            int affected = pstmt.executeUpdate();
            if (affected > 0) System.out.println("Adjusted product price");
        }
    }

    private static void analyzeCityDemographics(Connection conn) throws SQLException {
        String sql = "WITH city_stats AS (" +
                "  SELECT c.city_name, " +
                "         COUNT(u.user_id) AS user_count, " +
                "         AVG(u.age) AS avg_age " +
                "  FROM cities c " +
                "  LEFT JOIN users u ON c.city_id = u.city_id " + "  GROUP BY c.city_id" +
                ") " +
                "SELECT * FROM city_stats WHERE user_count > (SELECT AVG(user_count) FROM city_stats)";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                System.out.printf("%s: %d users (Avg age %.1f)%n",
                        rs.getString("city_name"),
                        rs.getInt("user_count"),
                        rs.getDouble("avg_age"));
            }
        }
    }
}
