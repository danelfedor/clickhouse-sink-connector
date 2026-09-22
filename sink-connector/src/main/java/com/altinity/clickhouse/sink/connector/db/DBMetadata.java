package com.altinity.clickhouse.sink.connector.db;

import com.altinity.clickhouse.sink.connector.ClickHouseSinkConnectorConfig;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.util.*;

import static com.altinity.clickhouse.sink.connector.db.BaseDbWriter.SYSTEM_DB;


public class DBMetadata {

    private static final Logger log = LogManager.getLogger(DBMetadata.class);

    static int MAX_RETRIES = 2;

    public static void setMaxRetries(int maxRetries) {
        MAX_RETRIES = maxRetries;
    }

    public enum TABLE_ENGINE {
        COLLAPSING_MERGE_TREE("CollapsingMergeTree"),
        REPLACING_MERGE_TREE("ReplacingMergeTree"),

        REPLICATED_REPLACING_MERGE_TREE("ReplicatedReplacingMergeTree"),

        MERGE_TREE("MergeTree"),

        DEFAULT("default");

        private final String engine;

        public String getEngine() {
            return engine;
        }

        TABLE_ENGINE(String engine) {
            this.engine = engine;
        }
    }

    public static final String COLLAPSING_MERGE_TREE_SIGN_PREFIX = "CollapsingMergeTree(";
    public static final String REPLACING_MERGE_TREE_VER_PREFIX = "ReplacingMergeTree(";

    public static final String REPLACING_MERGE_TREE_VERSION_WITH_IS_DELETED = "23.2";
    public static final String REPLICATED_REPLACING_MERGE_TREE_VER_PREFIX = "ReplicatedReplacingMergeTree(";
    /**
     * Function to get table engine using system tables.
     * @param conn ClickHouse Connection
     * @param tableName Table Name.
     * @return TABLE_ENGINE type
     */
    public MutablePair<TABLE_ENGINE, String> getTableEngineUsingSystemTables(final Connection conn, final String database,
                                                        final String tableName) {
        MutablePair<TABLE_ENGINE, String> result = new MutablePair<>();
        try {
            Connection conn_inner = HikariDbSource.initiateNewConnectionIfClosed(conn, database);
            try(Statement stmt = conn_inner.createStatement()) {
                String showSchemaQuery = String.format("select engine_full from system.tables where name='%s' and database='%s'",
                        tableName, database);
                ResultSet rs = stmt.executeQuery(showSchemaQuery);
                if(rs.wasNull() == false && rs.next()) {
                    String response =  rs.getString(1);
                    result = getEngineFromResponse(response);
                } else {
                    log.debug("Error: Table not found in system tables:" + tableName + " Database:" + database);
                }
                rs.close();
                stmt.close();
                log.info("getTableEngineUsingSystemTables ResultSet" + rs);
            }
        } catch(Exception e) {
            log.debug("getTableEngineUsingSystemTables exception", e);
        }

        return result;
    }

    public MutablePair<TABLE_ENGINE, String> getEngineFromResponse(String response) {
        MutablePair<TABLE_ENGINE, String> result = new MutablePair<>();

        if(response.contains(TABLE_ENGINE.REPLICATED_REPLACING_MERGE_TREE.engine)) {
            result.left = TABLE_ENGINE.REPLICATED_REPLACING_MERGE_TREE;
            result.right = "_version";
        }
        else if(response.contains(TABLE_ENGINE.REPLACING_MERGE_TREE.engine)) {
            result.left = TABLE_ENGINE.REPLACING_MERGE_TREE;
            result.right = "_version";
        } else if(response.contains(TABLE_ENGINE.MERGE_TREE.engine)) {
            result.left = TABLE_ENGINE.MERGE_TREE;
        }  else {
            result.left = TABLE_ENGINE.DEFAULT;
        }

        return result;
    }

    /**
     * Function to get the column name and isNullable as key/value pair.
     */
    public Map<String, Boolean> getColumnsIsNullableForTable(String tableName,
                                                             Connection conn,
                                                             String database) throws SQLException {
        Map<String, Boolean> columnsIsNullable = new HashMap<>();

        // Execute the following query to get the column name and isNullable as key/value pair.
        String query = String.format("SELECT name AS column_name, type LIKE 'Nullable(%%' AS is_nullable FROM system.columns WHERE (table = '%s') AND (database = '%s')", tableName, database);
        try{
            MutablePair<ResultSet, PreparedStatement> result = executeQueryWithResultSet(query, conn, database);
            ResultSet rs = result.getLeft();
            PreparedStatement stmt = result.getRight();
            while (rs.next()) {
                String columnName = rs.getString("column_name");
                boolean isNullable = rs.getBoolean("is_nullable");
                columnsIsNullable.put(columnName, isNullable);
            }
            rs.close();
            stmt.close();
        } catch (Exception e) {
            log.error("Error getting columns is nullable", e);
        }
        return columnsIsNullable;
    }
  
    /**
     * Function that uses the DatabaseMetaData JDBC functionality
     * to get the column name and column data type as key/value pair.
     */
    public Map<String, String> getColumnsDataTypesForTable(String tableName,
                                                           Connection conn,
                                                           String database,
                                                           ClickHouseSinkConnectorConfig config) {

        // Add retry logic.
        int retryCount = 0;
        Set<String> aliasColumns = new HashSet<>();
            try {
                aliasColumns = getAliasAndMaterializedColumnsForTableAndDatabase(tableName, database, conn);
            } catch(Exception e) {
                log.error("Error getting alias columns", e);
            }
        LinkedHashMap<String, String> result = new LinkedHashMap<>();
        // Add retry logic.
        retryCount = 0;
        while (retryCount < MAX_RETRIES) {
            try {
                conn = HikariDbSource.initiateNewConnectionIfClosed(conn, database);
                // 库名必须放在 schemaPattern(第2个参数)位:
                // clickhouse-jdbc 把 database 映射为 schema(getCatalog() 返回 null),
                // 传在 catalog 位会被驱动忽略, schemaPattern=null 时等于不做库过滤,
                // 于是同一台 server 上所有同名表(跨库)的列会被合并成并集返回。
                ResultSet columns = conn.getMetaData().getColumns(null, database,
                    tableName, null);
                while (columns.next()) {
                    String columnName = columns.getString("COLUMN_NAME");
                    // should not have _sign column
                    if (columnName.equals("_sign")) {
                        continue;
                    }
                    log.debug("Skipping _sign column");
                    String typeName = columns.getString("TYPE_NAME");

                    String isGeneratedColumn = columns.getString("IS_GENERATEDCOLUMN");
              
                    // Skip generated columns.
                    if(isGeneratedColumn != null && isGeneratedColumn.equalsIgnoreCase("YES")) {
                        continue;
                    }
                    if(aliasColumns.contains(columnName)) {
                        log.debug("Skipping alias column: " + columnName);
                        continue;
                    }
                    result.put(columnName, typeName);
                }
                columns.close();
                break;
            } catch (SQLException sq) {
                log.error("Exception retrieving Column Metadata", sq);
                retryCount++;
            }
        }
        return result;
    }

    /**
     * Function to get the column names which are
     * @return
     */
    public Set<String> getAliasAndMaterializedColumnsForTableAndDatabase(String tableName, String databaseName,
                                                                         Connection conn) throws SQLException {

        // Add retry logic.
        int retryCount = 0;
        Set<String> aliasColumns = new HashSet<>();
        try {
            String query = "SELECT name FROM system.columns WHERE (table = '%s') AND (database = '%s') and " +
            "(default_kind='ALIAS' or default_kind='MATERIALIZED')";
            String formattedQuery = String.format(query, tableName, databaseName);
            // Execute query
            MutablePair<ResultSet, PreparedStatement> result = executeQueryWithResultSet(query, conn, databaseName);
            ResultSet rs = result.getLeft();
            PreparedStatement stmt = result.getRight();
            // Get the list of columns from rs.
            if(rs != null) {
                while (rs.next()) {
                    String response = rs.getString(1);
                    aliasColumns.add(response);
                }
            }
            rs.close();
            stmt.close();
        } catch(Exception e) {
            log.error("Error getting alias columns", e);
        }
        return aliasColumns;
    }


    /**
     * Function to execute query.
     * @param sql
     * @return
     * @throws SQLException
     */
    public MutablePair<ResultSet, PreparedStatement> executeQueryWithResultSet(String sql, Connection conn, String database) throws Exception {
        int retryCount = 0;
        ResultSet rs = null;
        PreparedStatement ps = null;
        while (retryCount < MAX_RETRIES) {
            try {
                conn = HikariDbSource.initiateNewConnectionIfClosed(conn, database);
                ps = conn.prepareStatement(sql);
                rs = ps.executeQuery();
                break;
            } catch(SQLException sqle) {
                log.error("Error executing query: Retrying: #" + retryCount + ", SQL: " + sql, sqle);
                try {
                    Thread.sleep(1000 * (retryCount + 1));
                } catch(Exception e) {
                    log.error("Error initiating DB connection during retry #" + retryCount, e);
                }
            } catch(Exception e) {
                if (ps != null) {
                    try {
                        ps.close();
                    } catch (SQLException se) {
                        log.warn("Failed to close PreparedStatement", se);
                    }
                }
                log.error("Error executing query", e);
                retryCount++;
            }
        }
        return new MutablePair<>(rs, ps);
    }

    private boolean shouldIgnoreDDLError(String errorMessage) {
        // column with this name already exists.
        if (errorMessage.contains("cannot find column") ) {
            return true;
        }
        // cannot find column `innerinsttype` to drop.
        if (errorMessage.contains("Cannot add column") && errorMessage.contains("to drop")) {
            return true;
        }
        return false;
    }
    /**
     * Function to execute DDL query
     * @parm sql
     * @return
     * @throws SQLException
     **/
    public void executeDDLQuery(Connection conn, String sql) throws SQLException {
        int retryCount = 0;
        String trimmedSql = sql.trim().toLowerCase();
        // 过滤非DDL语句
        if (!(trimmedSql.startsWith("create") || trimmedSql.startsWith("alter") ||
                trimmedSql.startsWith("drop") || trimmedSql.startsWith("truncate"))) {
            log.error("Not execute sql" + trimmedSql + "Do not use this func to execute");
            return;
        }
        while (retryCount < MAX_RETRIES) {
            try {
                conn = HikariDbSource.initiateNewConnectionIfClosed(conn, SYSTEM_DB);
                Statement stmt = conn.createStatement();
                stmt.execute(sql);
                break;
            } catch(SQLException sqle) {
                String errorMessage = sqle.getMessage().toLowerCase();
                if (shouldIgnoreDDLError(errorMessage)) {
                    log.info("Ignoring DDL error: " + errorMessage);
                    break;
                }
                log.error("Error executing query: Retrying: #" + retryCount + ", SQL: " + sql, sqle);
                retryCount++;
            } catch (Exception e) {
                log.error("Unexpected error executing query: " + sql, e);
                break;
            }
        }
    }


    /**
     * Function to execute query.
     * @param sql
     * @return
     * @throws SQLException
     */
    public String executeSystemQuery(Connection conn, String sql) throws SQLException {
        String result = null;
        try {
            MutablePair<ResultSet, PreparedStatement> res = executeQueryWithResultSet(sql, conn, SYSTEM_DB);
            ResultSet rs = res.getLeft();
            PreparedStatement stmt = res.getRight();
            if (rs != null && rs.next()) {
                result = rs.getString(1);
            }
            rs.close();
            stmt.close();
        } catch(Exception e) {
            log.error("Unexpected error executing System query: " + sql, e);
        }
        return result;
    }

    public Map<String, String> getColumnsDataTypesForTable(Connection conn, String tableName, String database ) {

        // Add retry logic. 
        int retryCount = 0;
        LinkedHashMap<String, String> result = new LinkedHashMap<>();
        while (retryCount < MAX_RETRIES) {
            try {
                conn = HikariDbSource.initiateNewConnectionIfClosed(conn, database);
                ResultSet columns = conn.getMetaData().getColumns(null, database,
                        tableName, null);
                while (columns.next()) {
                    String columnName = columns.getString("COLUMN_NAME");
                    String typeName = columns.getString("TYPE_NAME");
                    result.put(columnName, typeName);
            }
            break;
            } catch (Exception sq) {
                log.error("Exception retrieving Column Metadata", sq);
                retryCount++;
            }
        }
        return result;
    }

    public void truncateTable(Connection conn, String databaseName, String tableName) throws SQLException {
        int retryCount = 0;
        PreparedStatement ps = null;
        while(retryCount < MAX_RETRIES) {
            try {
                conn = HikariDbSource.initiateNewConnectionIfClosed(conn, databaseName);
                ps = conn.prepareStatement("TRUNCATE TABLE " + databaseName + "." + tableName);
                ps.execute();
                break;
            } catch (SQLException sqle) {
                log.error("*** Error: Truncate table statement error, retry attempt: " + retryCount, sqle);
                retryCount++;
            } catch (Exception e) {
                log.error("*** Error: Truncate table statement error, retry attempt: " + retryCount, e);
            }
        }
    }

    public PreparedStatement getPreparedStatement(Connection conn, String sql) throws SQLException {

        int retryCount = 0;
        PreparedStatement ps = null;
        while(retryCount < MAX_RETRIES) {
            try {
                conn = HikariDbSource.initiateNewConnectionIfClosed(conn, SYSTEM_DB);
                ps = conn.prepareStatement(sql);
                break;
            } catch (SQLException e) {
                log.error("Error getting prepared statement, retry attempt: " + retryCount, e);
                retryCount++;
            }
        }
        return ps;
    }
}
