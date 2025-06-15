package org.example;

import java.io.*;
import java.sql.*;
import java.util.*;

public class App {

    public static void main(String[] args) throws SQLException {
        testDb();
    }

    static void testDb() throws SQLException {
        ClassLoader classLoader = App.class.getClassLoader();
        InputStream input = classLoader.getResourceAsStream("config.properties");
        Properties props = new Properties();
        try {
            props.load(input);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        String jdbcUrl = props.getProperty("db.url");
        String username = props.getProperty("db.user");
        String password = props.getProperty("db.password");
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password)) {
            System.out.println("Подключение установлено, введите SQL выражение");
            System.out.print("> ");
            Statement stmt = conn.createStatement();

            Scanner scanner = new Scanner(System.in);
            String command = scanner.nextLine();
            while (true) {
                if (command.equalsIgnoreCase("QUIT")) {
                    conn.close();
                    return;
                } else {
                    try {
                        executeCommand(stmt, command);
                    } catch (SQLException e) {
                        System.out.println(e.getMessage());
                    }
                }
                System.out.print("> ");
                command = scanner.nextLine();
            }
        }
    }

    static void executeCommand(Statement stmt, String command) throws SQLException {
        Set<String> DDL = new HashSet<>(Arrays.asList("CREATE", "ALTER", "DROP"));
        Set<String> DML = new HashSet<>(Arrays.asList("INSERT", "UPDATE", "DELETE", "SELECT"));
        String firstWord = command.trim().split("\\s+")[0].toUpperCase();
        boolean contains = DDL.contains(firstWord);
        if (contains) {
            stmt.execute(command);
        } else {
            contains = DML.contains(firstWord);
            if (contains) {
                if (firstWord.equals("SELECT")) {
                    executeSelect(stmt, command);
                } else {
                    stmt.executeUpdate(command);
                }
            } else {
                System.out.println("Неопознанная команда");
            }
        }
    }

    static void executeSelect(Statement stmt, String command) throws SQLException {
        char c = command.charAt(command.length() - 1);
        if (c == ';') {
            command = command.substring(0, command.length() - 1);
        }
        String queryCount = "SELECT COUNT(*) FROM ( " + command + " )";
        ResultSet rs = stmt.executeQuery(queryCount);
        rs.next();
        int totalCount = rs.getInt(1);
        System.out.println("Всего записей в БД: " + totalCount);
        String limitedQuery = "WITH ORIGINAL_QUERY AS (" + command + ") SELECT * FROM ORIGINAL_QUERY LIMIT 10";
        rs = stmt.executeQuery(limitedQuery);
        printResultSet(rs);
        if (totalCount >= 10) System.out.println("В БД есть еще записи");
    }

    public static void printResultSet(ResultSet rs) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        List<Object[]> rows = new ArrayList<>();
        Object[] headers = new Object[columnCount];
        for (int i = 1; i <= columnCount; i++) {
            headers[i - 1] = metaData.getColumnLabel(i);
        }
        rows.add(headers);
        while (rs.next()) {
            Object[] row = new Object[columnCount];
            for (int i = 1; i <= columnCount; i++) {
                row[i - 1] = rs.getObject(i);
            }
            rows.add(row);
        }
        int[] columnWidths = new int[columnCount];
        for (int i = 0; i < columnCount; i++)
            for (Object[] row : rows) {
                {
                    if (row[i] != null) {
                        columnWidths[i] = Math.max(columnWidths[i], row[i].toString().length());
                    }
                }
            }

        StringBuilder formatBuilder = new StringBuilder();
        for (int width : columnWidths) {
            formatBuilder.append("%-").append(width + 2).append("s");
        }
        String format = formatBuilder.toString();
        for (int i = 0; i < rows.size(); i++) {
            System.out.printf(format + "%n", rows.get(i));
            if (i == 0) {
                for (int width : columnWidths) {
                    System.out.print("-".repeat(width + 2));
                }
                System.out.println();
            }
        }
    }
}
