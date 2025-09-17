package com.altinity.clickhouse.sink.connector.db;

import com.altinity.clickhouse.sink.connector.ClickHouseSinkConnectorConfig;
import com.altinity.clickhouse.sink.connector.ClickHouseSinkConnectorConfigVariables;
import com.altinity.clickhouse.sink.connector.common.Metrics;
import com.clickhouse.jdbc.ClickHouseDataSource;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

// Singleton class(one per database)
public class HikariDbSource {
    private static Map<String, HikariDataSource> instance = new HashMap<>();

    private static Map<String, Connection> connectionPool = new HashMap<>();
    //private static HikariDbSource instance;

    private static final Logger log = LogManager.getLogger(HikariDbSource.class);
    //private HikariDataSource dataSource;
    private String databaseName;

    // private constructor
    private HikariDbSource(ClickHouseDataSource dataSource, String databaseName) {
        // this.createConnectionPool(dataSource, databaseName);
    }

    public static Connection initiateNewConnectionIfClosed(Connection conn, String databaseName) throws SQLException {
        if (conn != null && !conn.isClosed()) {
            return conn;
        }
        if (conn != null) {
            conn.close();
            conn = null;
        }
        log.warn("Connection is null or closed, reconnect");
        HikariDataSource dbSource = instance.get(databaseName);
        HikariDbSource.printConnectionInfo();
        conn = dbSource.getConnection();
        int retry_count = 0;
        while(conn == null || conn.isClosed()) {
            try {
                conn = dbSource.getConnection();
                retry_count++;
                long sleepTime = 1000L * retry_count;
                Thread.sleep(sleepTime);
            } catch(Exception e) {
                log.error("Error connecting to ClickHouse " + e + " Retry Count: " + retry_count);
            }
        }
        return conn;
    }

    public static HikariDataSource getInstance(SinkConnectorDataSource dataSource, String databaseName,
                                               ClickHouseSinkConnectorConfig config) {
        if(instance.containsKey(databaseName)) {
            return instance.get(databaseName);
        } else {
            HikariDataSource hikariDataSource = createConnectionPool(dataSource, databaseName, config);
            instance.put(databaseName, hikariDataSource);
        }
        return instance.get(databaseName);
    }

    public static HikariDataSource getInstance(String databaseName) {
        return instance.get(databaseName);
    }

    private static HikariDataSource createConnectionPool(SinkConnectorDataSource chDataSource,
        String databaseName, ClickHouseSinkConnectorConfig config)  {
        // pass the clickhouse config to create the datasource

        int maxPoolSize = config.getInt(ClickHouseSinkConnectorConfigVariables.CONNECTION_POOL_MAX_SIZE.toString());
        long poolConnectionTimeout = config.getLong(ClickHouseSinkConnectorConfigVariables.CONNECTION_POOL_TIMEOUT.toString());
        long maxLifetime = config.getLong(ClickHouseSinkConnectorConfigVariables.CONNECTION_POOL_MAX_LIFETIME.toString());

        HikariConfig poolConfig = new HikariConfig();
        poolConfig.setPoolName("clickhouse" + "-" + databaseName);
        // socket_timeout = 30000ms (30s)
        // connect_timeout = 10000ms (10s)
        String jdbcUrl = String.format("jdbc:ch:{hostname}:{port}/%s", databaseName);
        poolConfig.setJdbcUrl(jdbcUrl);
        poolConfig.setDriverClassName("com.clickhouse.jdbc.ClickHouseDriver"); // Ensure driver is set
        poolConfig.setConnectionTimeout(poolConnectionTimeout);
        poolConfig.setMaximumPoolSize(maxPoolSize);
        //poolConfig.setMinimumIdle(minIdle);
        //poolConfig.setIdleTimeout(2_000L);
        poolConfig.setMaxLifetime(maxLifetime);
        poolConfig.setDataSource(chDataSource);

        HikariDataSource dataSource = new HikariDataSource(poolConfig);

        PrometheusMeterRegistry meterRegistry = Metrics.meterRegistry();

        if(meterRegistry != null) {
            dataSource.setMetricRegistry(meterRegistry);
        }
        return dataSource;
    }

    public static void close() {

        if(instance != null) {
            for(HikariDataSource hikariDataSource: instance.values()) {
                try {
                    hikariDataSource.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            instance.clear();
        }
    }

    public static void closeDatabaseConnection(String databaseName) {
        if(instance.containsKey(databaseName)) {
            try {
                instance.get(databaseName).close();
            } catch (Exception e) {
                e.printStackTrace();
                log.error("Error closing database connection pool", e);
            }
        }
    }

    public static void printConnectionInfo() {
        for(HikariDataSource hikariDataSource: instance.values()) {
            log.debug("Connection Pool Info: " + hikariDataSource.getPoolName() + " Max Size: " + hikariDataSource.getMaximumPoolSize() + " Active Connections: " + hikariDataSource.getHikariPoolMXBean().getActiveConnections());
        }
    }
}
