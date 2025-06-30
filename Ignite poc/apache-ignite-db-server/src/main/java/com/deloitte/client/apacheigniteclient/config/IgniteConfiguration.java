package com.deloitte.client.apacheigniteclient.config;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

import com.deloitte.client.apacheigniteclient.model.Employee;

@Configuration
public class IgniteConfiguration {

    @Bean
    @Lazy
    public Ignite ignite() throws Exception {
        org.apache.ignite.configuration.IgniteConfiguration cfg = new org.apache.ignite.configuration.IgniteConfiguration();
        cfg.setDataStorageConfiguration(getDataStorageConfiguration());

        String workDir = System.getProperty("user.dir") + "/ignite-work";
        java.io.File dir = new java.io.File(workDir);
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                throw new RuntimeException("Failed to create Ignite work directory: " + workDir);
            }
        }
        cfg.setWorkDirectory(workDir);
        Ignite ignite = Ignition.start(cfg);
        ignite.cluster().state(org.apache.ignite.cluster.ClusterState.ACTIVE);

        return ignite;
    }

    @org.springframework.context.event.EventListener(org.springframework.boot.context.event.ApplicationReadyEvent.class)
    public void onApplicationReady(org.springframework.boot.context.event.ApplicationReadyEvent event) {
        System.out.println("Apache Ignite Client Application is ready.");

        Ignite ignite = event.getApplicationContext().getBean(Ignite.class);
        
        ignite.cluster().state(org.apache.ignite.cluster.ClusterState.ACTIVE);
        createSqlTable(ignite);
        loadMySQLDataToIgnite(ignite);
    }
 
    private @NotNull DataStorageConfiguration getDataStorageConfiguration() {
        DataStorageConfiguration storageCfg = new DataStorageConfiguration();
 
        DataRegionConfiguration dataRegionCfg = new DataRegionConfiguration();
        dataRegionCfg.setName("Default_Region");
        dataRegionCfg.setPersistenceEnabled(true);
        dataRegionCfg.setMaxSize(512L * 1024 * 1024);
 
        storageCfg.setDefaultDataRegionConfiguration(dataRegionCfg);
        return storageCfg;
    }
 
private void createSqlTable(Ignite ignite) {
    ignite.destroyCache("EmployeeCache");
    ignite.destroyCache("SQL_PUBLIC_EMPLOYEE");
    IgniteCache<String, Employee> cache = ignite.getOrCreateCache(
        new org.apache.ignite.configuration.CacheConfiguration<String, Employee>("EmployeeCache")
            .setSqlSchema("PUBLIC")
            .setQueryParallelism(32)
            .setOnheapCacheEnabled(true)
            .setIndexedTypes(String.class, Employee.class)
            .setQueryParallelism(32)
    );

    // cache.query(new SqlFieldsQuery(
    //     "CREATE TABLE IF NOT EXISTS Employee (" +
    //     "id VARCHAR PRIMARY KEY, " +
    //     "name VARCHAR, " +
    //     "country VARCHAR)")).getAll();

    System.out.println("SQL Table Employee created.");
}
 
// private void importDataFromMySQL(Ignite ignite) {
//     long start=System.nanoTime();
//         try {
//             String mysqlUrl = "jdbc:mysql://localhost:3306/poc_db";
//             String mysqlUser = "root";
//             String mysqlPassword = "root03";
//             Connection mysqlConn = DriverManager.getConnection(mysqlUrl, mysqlUser, mysqlPassword);

//             Statement mysqlStmt = mysqlConn.createStatement();
//             ResultSet rs = mysqlStmt.executeQuery("SELECT id, name, country FROM employee");

//             IgniteCache<String, Employee> cache = ignite.cache("EmployeeCache");
//             int count=0;
//             while (rs.next()) {
//                 String id = rs.getString("id");
//                 String name = rs.getString("name");
//                 String country = rs.getString("country");
        
//                 SqlFieldsQuery checkQuery = new SqlFieldsQuery(
//                         "SELECT id FROM employee WHERE id = ?"
//                 ).setArgs(id);
//                 boolean exists = !cache.query(checkQuery).getAll().isEmpty();

//                 if (!exists) {
//                     cache.query(new SqlFieldsQuery(
//                             "INSERT INTO employee (id, name, country) VALUES (?, ?, ?)")
//                             .setArgs(id, name, country)
//                     ).getAll();
//                     count++;
//                 } else {
//                     count++;
//                        }
//             }
//             rs.close();
//             mysqlStmt.close();
//             mysqlConn.close();
//             System.out.println( count  +"Data rows loaded from MySQL into Ignite SQL Table.");
//             long end=System.nanoTime();
//             System.out.println("Time To Load Data:" + (end-start));
//         } catch (Exception e) {
//             e.printStackTrace();
//         }
//     }
    // private static @NotNull DataStorageConfiguration getDataStorageConfiguration() {
    //     DataRegionConfiguration drc = new DataRegionConfiguration();
    //     drc.setName("My DataRegion");
    //     drc.setInitialSize(10 * 1024 * 1024);
    //     drc.setMaxSize(40 * 1024 * 1024);
    //     drc.setPageEvictionMode(DataPageEvictionMode.RANDOM_2_LRU);

    //     DataStorageConfiguration dsc = new DataStorageConfiguration();
    //     dsc.setDefaultDataRegionConfiguration(drc);
    //     return dsc;
    // }

    private static void loadMySQLDataToIgnite(Ignite ignite) {
        String url = "jdbc:mysql://localhost:3306/poc_db";
        String user = "root";
        String password = "root03";
        
        System.out.println("[Start] Loading MySQL data to Ignite");
        long start = System.currentTimeMillis();
 
        try (
            Connection conn = DriverManager.getConnection(url, user, password);
            Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        ) {
            stmt.setFetchSize(Integer.MIN_VALUE);
            ResultSet rs = stmt.executeQuery("SELECT id, name, country FROM employee");
            IgniteCache<String, Employee> cache = ignite.getOrCreateCache("EmployeeCache");
            try (IgniteDataStreamer<String, Employee> streamer = ignite.dataStreamer("EmployeeCache")) {
                streamer.allowOverwrite(true);
                streamer.perNodeBufferSize(10240);
                streamer.perNodeParallelOperations(8);
 
                int count = 0;
                while (rs.next()) {
                    String id = rs.getString("id");
                    String name = rs.getString("name");
                    String country = rs.getString("country");
 
                    Employee emp = new Employee(id, name, country);
                    streamer.addData(id, emp);
                    count++;
                }
                streamer.flush();
                System.out.println("[Count] Records loaded: " + cache.sizeLong(CachePeekMode.PRIMARY));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
 
        long end = System.currentTimeMillis();
        System.out.println("[Time] MySQL to Ignite loading completed in " + (end - start) / 1000.0 + " seconds");
    }
}