package com.altinity.clickhouse.sink.connector.db;

import static com.altinity.clickhouse.sink.connector.db.BaseDbWriter.SYSTEM_DB;
import static com.altinity.clickhouse.sink.connector.db.ClickHouseDbConstants.CHECK_DB_EXISTS_SQL;

import com.altinity.clickhouse.sink.connector.ClickHouseSinkConnectorConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.maven.artifact.versioning.DefaultArtifactVersion;

import java.sql.*;
import java.time.ZoneId;
import java.util.*;


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

    /**
     * Function to check if database exists by querying the information schema tables.
     * @param conn
     * @param databaseName
     * @return
     */
    public boolean checkIfDatabaseExists(Connection conn, String databaseName) throws SQLException {

        int retryCount = 0;
        boolean result = false;

        while (!result && retryCount < MAX_RETRIES) {
            try {
                conn = HikariDbSource.initiateNewConnectionIfClosed(conn, databaseName);
                log.info("Retrying checkIfDatabaseExists, attempt {}", retryCount);
                try (Statement retryStmt = conn.createStatement()) {
                    String showSchemaQuery = String.format(CHECK_DB_EXISTS_SQL, databaseName);
                    ResultSet retryRs = retryStmt.executeQuery(showSchemaQuery);
                    if (retryRs != null && retryRs.next()) {
                        String response = retryRs.getString(1);
                        if (response.equalsIgnoreCase(databaseName)) {
                            result = true;
                        }
                    }
                    retryRs.close();
                }
            } catch (Exception retryException) {
                log.error("Retry attempt {} failed" + retryCount, retryException);
                retryCount++;
            }
        }
        

        return result;
    }

    public static final String COLLAPSING_MERGE_TREE_SIGN_PREFIX = "CollapsingMergeTree(";
    public static final String REPLACING_MERGE_TREE_VER_PREFIX = "ReplacingMergeTree(";

    public static final String REPLACING_MERGE_TREE_VERSION_WITH_IS_DELETED = "23.2";
    public static final String REPLICATED_REPLACING_MERGE_TREE_VER_PREFIX = "ReplicatedReplacingMergeTree(";
    /**
     * Function to extract the sign column for CollapsingMergeTree
     * @param createDML
     * @return Sign column
     */
    public String getSignColumnForCollapsingMergeTree(String createDML) {

        String signColumn = "sign";

        if(createDML.contains(TABLE_ENGINE.COLLAPSING_MERGE_TREE.getEngine())) {
            signColumn = StringUtils.substringBetween(createDML, COLLAPSING_MERGE_TREE_SIGN_PREFIX, ")");
        } else {
            log.error("Error: Trying to retrieve sign from table that is not CollapsingMergeTree");
        }

        return signColumn;
    }

    /**
     * Function to extract the version column for ReplacingMergeTree
     * @param createDML
     * @return Sign column
     */
    public String getVersionColumnForReplacingMergeTree(String createDML) {

        String versionColumn = "ver";

        if(createDML.contains(TABLE_ENGINE.REPLICATED_REPLACING_MERGE_TREE.getEngine())) {
            String parameters = StringUtils.substringBetween(createDML, REPLICATED_REPLACING_MERGE_TREE_VER_PREFIX, ")");
            if(parameters != null) {
                String[] parameterArray = parameters.split(",");
                if(parameterArray != null && parameterArray.length == 3) {
                    versionColumn = parameterArray[2].trim();
                } else if(parameterArray != null && parameterArray.length == 4) {
                    versionColumn = parameterArray[2].trim() + "," + parameterArray[3].trim();
                }
            }
        }
        else if(createDML.contains(TABLE_ENGINE.REPLACING_MERGE_TREE.getEngine())) {
            if(createDML != null && createDML.indexOf("(") != -1 && createDML.indexOf(")") != -1) {
                String subString = StringUtils.substringBetween(createDML, REPLACING_MERGE_TREE_VER_PREFIX, ")");
                if(subString != null) {
                    versionColumn = subString.trim();
                }
            }
        } else {
            log.error("Error: Trying to retrieve ver from table that is not ReplacingMergeTree");
        }

        return versionColumn;
    }
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
            if (conn == null) {
                log.error("Error with DB connection");
                return result;
            }
            try(Statement stmt = conn.createStatement()) {
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

        if(response.contains(TABLE_ENGINE.COLLAPSING_MERGE_TREE.engine)) {
            result.left = TABLE_ENGINE.COLLAPSING_MERGE_TREE;
            result.right = getSignColumnForCollapsingMergeTree(response);
        }
        else if(response.contains(TABLE_ENGINE.REPLICATED_REPLACING_MERGE_TREE.engine)) {
            result.left = TABLE_ENGINE.REPLICATED_REPLACING_MERGE_TREE;
            result.right = getVersionColumnForReplacingMergeTree(response);
        }
        else if(response.contains(TABLE_ENGINE.REPLACING_MERGE_TREE.engine)) {
            result.left = TABLE_ENGINE.REPLACING_MERGE_TREE;
            result.right = getVersionColumnForReplacingMergeTree(response);
        } else if(response.contains(TABLE_ENGINE.MERGE_TREE.engine)) {
            result.left = TABLE_ENGINE.MERGE_TREE;
        }  else {
            result.left = TABLE_ENGINE.DEFAULT;
        }

        return result;
    }


    /**
     * Function to check if Replacing mergetree is supported
     * based on ClickHouse version.
     * @return true, if RMT is supported, false otherwise
     * @throws SQLException
     */
    public boolean checkIfNewReplacingMergeTree(String currentClickHouseVersion) throws SQLException {

        boolean result = true;

        DefaultArtifactVersion supportedVersion = new DefaultArtifactVersion(REPLACING_MERGE_TREE_VERSION_WITH_IS_DELETED);
        DefaultArtifactVersion currentVersion = new DefaultArtifactVersion(currentClickHouseVersion);

        if (currentVersion.compareTo(supportedVersion) < 0) {
            result = false;
        }

        return result;
    }

    public String getClickHouseVersion(Connection connection) throws SQLException {
        return this.executeSystemQuery(connection, "SELECT VERSION()");
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

        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                String columnName = rs.getString("column_name");
                boolean isNullable = rs.getBoolean("is_nullable");
                columnsIsNullable.put(columnName, isNullable);
            }
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
                conn = HikariDbSource.initiateNewConnectionIfClosed(conn, database);
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
                ResultSet columns = conn.getMetaData().getColumns(database, null,
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
     * Function to get the ClickHouse server timezone(Defaults to UTC)
     */
    public ZoneId getServerTimeZone(Connection conn)  {
        ZoneId result = ZoneId.of("UTC");
        if(conn != null) {
            try {
                // Perform a query to get the server timezone
                ResultSet rs = conn.prepareStatement("SELECT timezone()").executeQuery();
                if (rs.next()) {
                    String serverTimeZone = rs.getString(1);
                    result = ZoneId.of(serverTimeZone);
                }
                rs.close();
            } catch (Exception e) {
                log.error("Error retrieving server timezone", e);
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
        while (retryCount < MAX_RETRIES) {
            try {
                conn = HikariDbSource.initiateNewConnectionIfClosed(conn, databaseName);
                String query = "SELECT name FROM system.columns WHERE (table = '%s') AND (database = '%s') and " +
                "(default_kind='ALIAS' or default_kind='MATERIALIZED')";
                String formattedQuery = String.format(query, tableName, databaseName);

                // Execute query
                ResultSet rs = conn.createStatement().executeQuery(formattedQuery);

                // Get the list of columns from rs.
                if(rs != null) {
                    while (rs.next()) {
                        String response = rs.getString(1);
                        aliasColumns.add(response);
                    }
                }
                rs.close();
                break;
            } catch(Exception e) {
                log.error("Error getting alias columns", e);
                retryCount++;
            }
        }
        return aliasColumns;
    }


    /**
     * Function to execute query.
     * @param sql
     * @return
     * @throws SQLException
     */
    public ResultSet executeQueryWithResultSet(String sql, Connection conn) throws SQLException {
        // Add retry logic.
        int retryCount = 0;
        ResultSet rs = null;
        while (retryCount < MAX_RETRIES) {
            try {
                conn = HikariDbSource.initiateNewConnectionIfClosed(conn, SYSTEM_DB);
                rs = conn.prepareStatement(sql).executeQuery();
                break;
            } catch(Exception e) {
                log.error("Error executing query", e);
                retryCount++;
            }
        }
        return rs;
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
                log.error("Error executing query: Retrying: #" + retryCount + ", SQL: " + sql, errorMessage);
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
        // 原有的查询逻辑
        int retryCount = 0;
        String result = null;

        while(retryCount < MAX_RETRIES) {
            try {
                conn = HikariDbSource.initiateNewConnectionIfClosed(conn, SYSTEM_DB);
                PreparedStatement ps = conn.prepareStatement(sql);
                ResultSet rs = ps.executeQuery();
                if (rs != null && rs.next()) {
                    result = rs.getString(1);
                }
                break;
            } catch(SQLException sqle) {
                log.error("Error executing query: Retrying: #" + retryCount + ", SQL: " + sql, sqle);
                try {
                    Thread.sleep(1000 * (retryCount + 1));
                } catch(Exception e) {
                    log.error("Error initiating DB connection during retry #" + retryCount, e);
                }
                retryCount++;
            } catch(Exception e) {
                log.error("Unexpected error executing query: " + sql, e);
                break;
            }
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
