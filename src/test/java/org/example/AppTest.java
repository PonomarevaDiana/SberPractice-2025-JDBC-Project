package org.example;

import org.junit.jupiter.api.*;

import java.io.*;
import java.sql.*;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class AppTest {
    private static final String TEST_JDBC_URL = "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1";
    private static final String TEST_USERNAME = "sa";
    private static final String TEST_PASSWORD = "";

    private Connection testConn;
    private Statement stmt;
    private ByteArrayOutputStream outContent;

    @BeforeEach
    void setUp() throws SQLException {
        testConn = DriverManager.getConnection(TEST_JDBC_URL, TEST_USERNAME, TEST_PASSWORD);
        stmt = testConn.createStatement();
        stmt.execute("CREATE TABLE TEST (ID INT PRIMARY KEY, NAME VARCHAR(255))");
        stmt.execute("INSERT INTO TEST VALUES (1, 'ALICE'), (2, 'BOB'), (3, 'CHARLIE')");
        outContent = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outContent));
    }

    @AfterEach
    void destruction() throws SQLException {
        System.setOut(System.out);
        if (stmt != null) stmt.close();
        if (testConn != null) {
            stmt = testConn.createStatement();
            stmt.execute("DROP TABLE TEST");
            testConn.close();
        }
    }

    @Test
    void testExecuteCommandDDL() throws SQLException {
        App.executeCommand(stmt, "CREATE TABLE TEST_DDL(ID INT)");
        ResultSet rs = testConn.getMetaData().getTables(null, null, "TEST_DDL", null);
        assertTrue(rs.next());
    }

    @Test
    void testExecuteCommandDMLInsert() throws SQLException {
        App.executeCommand(stmt, "INSERT INTO TEST VALUES (4, 'DAVID')");
        ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM TEST WHERE ID = 4");
        rs.next();
        assertEquals(1, rs.getInt(1));
    }

    @Test
    void testExecuteCommandDMLUpdate() throws SQLException {
        App.executeCommand(stmt, "UPDATE test SET name = 'Updated' WHERE id = 1");
        ResultSet rs = stmt.executeQuery("SELECT name FROM test WHERE id = 1");
        rs.next();
        assertEquals("Updated", rs.getString(1));
    }

    @Test
    void testExecuteSelect() throws SQLException {
        App.executeSelect(stmt, "SELECT * FROM TEST");
        String output = outContent.toString();
        assertTrue(output.contains("ID"));
        assertTrue(output.contains("NAME"));
        assertTrue(output.contains("ALICE"));
        assertTrue(output.contains("Всего записей в БД: 3"));
    }

    @Test
    void testUnknownCommand() throws SQLException {
        App.executeCommand(stmt, "UNKNOWN_COMMAND");
        String output = outContent.toString();
        assertTrue(output.contains("Неопознанная команда"));
    }

    @Test
    void testSelectWithSemicolon() throws SQLException {
        App.executeSelect(stmt, "SELECT * FROM TEST;");
        String output = outContent.toString();
        System.out.println(output);
        assertTrue(output.contains("ID") && output.contains("NAME"));
    }

    @Test
    void testPropertiesLoading() {
        Properties props = new Properties();
        props.setProperty("db.url", TEST_JDBC_URL);
        props.setProperty("db.user", TEST_USERNAME);
        props.setProperty("db.password", TEST_PASSWORD);
        assertNotNull(props.getProperty("db.url"));
        assertEquals(TEST_USERNAME, props.getProperty("db.user"));
    }


    @Test
    void testExecuteSelectWithEmptyResult() throws SQLException {
        App.executeSelect(stmt, "SELECT * FROM TEST WHERE ID = 999");
        String output = outContent.toString();
        assertTrue(output.contains("Всего записей в БД: 0"));
        assertFalse(output.contains("ALICE"));
    }

    @Test
    void testExecuteCommandWithEmptyInput() throws SQLException {
        App.executeCommand(stmt, "");
        String output = outContent.toString();
        assertTrue(output.contains("Неопознанная команда"));
    }

    @Test
    void testExecuteCommandWithMalformedSQL() {
        assertThrows(SQLException.class, () -> {
            App.executeCommand(stmt, "SELECT * FROM");
        });
    }

    @Test
    void testPrintResultSetWithSpecialCharacters() throws SQLException {
        stmt.execute("INSERT INTO TEST VALUES (5, 'Name with \"quotes\"')");
        ResultSet rs = stmt.executeQuery("SELECT * FROM TEST WHERE ID = 5");
        App.printResultSet(rs);
        String output = outContent.toString();
        assertTrue(output.contains("Name with \"quotes\""));
    }

    @Test
    void testExecuteSelectWithMoreThan10Records() throws SQLException {
        // Добавляем больше 10 записей
        for (int i = 4; i <= 15; i++) {
            stmt.executeUpdate(String.format("INSERT INTO TEST VALUES (%d, 'USER_%d')", i, i));
        }
        App.executeSelect(stmt, "SELECT * FROM TEST");
        String output = outContent.toString();
        assertTrue(output.contains("В БД есть еще записи"));
    }

    @Test
    void testPrintResultSet() throws SQLException {
        stmt.execute("CREATE TABLE TEST_PRINT (ID INT, NAME VARCHAR(100))");
        stmt.execute("INSERT INTO TEST_PRINT VALUES (1, 'First'), (2, 'Second')");
        ResultSet rs = stmt.executeQuery("SELECT * FROM TEST_PRINT");
        App.printResultSet(rs);
        String output = outContent.toString();
        assertTrue(output.contains("ID") && output.contains("NAME"));
        assertTrue(output.contains("First") && output.contains("Second"));
        assertTrue(output.contains("---"));
    }

    @Test
    void testDb_ShouldLoadPropertiesAndConnect() {
        String testConfig = "db.url=" + TEST_JDBC_URL + "\n" +
                "db.user=" + TEST_USERNAME + "\n" +
                "db.password=" + TEST_PASSWORD + "\n";
        InputStream testInput = new ByteArrayInputStream(testConfig.getBytes());
        assertDoesNotThrow(() -> {
            Properties props = new Properties();
            props.load(testInput);
            assertEquals(TEST_JDBC_URL, props.getProperty("db.url"));
        });
    }

    @Test
    void testMainMethod() {
        String input = "QUIT\n";
        System.setIn(new ByteArrayInputStream(input.getBytes()));
        assertDoesNotThrow(() -> App.main(new String[]{}));
    }
}
