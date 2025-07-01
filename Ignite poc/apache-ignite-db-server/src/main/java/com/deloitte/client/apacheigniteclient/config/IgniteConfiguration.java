package com.deloitte.client.apacheigniteclient.config;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.jetbrains.annotations.NotNull;
import java.sql.Connection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

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
        CacheConfiguration<String, BinaryObject> cfg = new org.apache.ignite.configuration.CacheConfiguration<String, BinaryObject>(
                "EmployeeCache")
                .setSqlSchema("PUBLIC")
                .setQueryParallelism(32)
                .setOnheapCacheEnabled(true);

        QueryEntity entity = new QueryEntity();
        entity.setKeyType("jave.lang.String");
        entity.setValueType("Employee");

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put("id", "java.lang.String");
        fields.put("name", "java.lang.String");
        fields.put("country", "java.lang.String");

        entity.setFields(fields);
        entity.setTableName("Employee");
        cfg.setQueryEntities(List.of(entity));
        @SuppressWarnings("unused")
        IgniteCache<String, BinaryObject> cache = ignite.getOrCreateCache(cfg);

        System.out.println("SQL Table Employee created.");
    }

    private static void loadMySQLDataToIgnite(Ignite ignite) {
        String url = "jdbc:mysql://localhost:3306/poc_db";
        String user = "root";
        String password = "root03";

        System.out.println("[Start] Loading MySQL data to Ignite");
        long start = System.currentTimeMillis();

        try (
                Connection conn = DriverManager.getConnection(url, user, password);
                Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);) {
            stmt.setFetchSize(Integer.MIN_VALUE);
            ResultSet rs = stmt.executeQuery("SELECT id, name, country FROM employee");
            IgniteCache<String, BinaryObject> cache = ignite.getOrCreateCache("EmployeeCache");
            IgniteBinary binary = ignite.binary();
            try (IgniteDataStreamer<String, BinaryObject> streamer = ignite.dataStreamer("EmployeeCache")) {
                streamer.allowOverwrite(true);
                streamer.perNodeBufferSize(10240);
                streamer.perNodeParallelOperations(8);

                // int count = 0;
                while (rs.next()) {
                    BinaryObjectBuilder builder = binary.builder("Employee");
                    builder.setField("id", rs.getString("id"));
                    builder.setField("name", rs.getString("name"));
                    builder.setField("coutry", rs.getString("country"));

                    BinaryObject emp = builder.build();
                    String id = rs.getString("id");
                    streamer.addData(id, emp);
                    // count++;
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