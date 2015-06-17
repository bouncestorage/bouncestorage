/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Serie;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BounceStats {
    public static final String DATABASE = "bounce";
    public static final String ENDPOINT = "http://localhost:8086";
    public static final String USER = "bounce";
    public static final String PASSWORD = "bounce";

    private static final int SUBMIT_LIMIT = 100;
    private static final int STATS_INTERVAL = 30;

    private InfluxDB db;
    private Logger logger = LoggerFactory.getLogger(BounceStats.class);
    private final Queue<StatsQueueEntry> queue;
    private final ScheduledExecutorService scheduler;

    public BounceStats() {
        db = InfluxDBFactory.connect(ENDPOINT, USER, PASSWORD);
        queue = new LinkedList<>();
        scheduler = new ScheduledThreadPoolExecutor(1);
    }

    public void start() {
        logger.debug("Starting the stats service");
        scheduler.scheduleAtFixedRate(this::submitValues, 0, STATS_INTERVAL, TimeUnit.SECONDS);
    }

    public void shutdown() {
        if (!scheduler.isShutdown()) {
            scheduler.shutdown();
        }
    }

    public void logOperation(String opName, int providerId, String containerName, String objectName, Long size,
                             Long startTime) {
        synchronized (queue) {
            Long timeStamp = new Date().getTime();
            StringBuilder nameBuilder = new StringBuilder(DBSeries.OPS_SERIES)
                    .append(".provider.").append(Integer.toString(providerId))
                    .append(".container.").append(containerName)
                    .append(".op.").append(opName);
            ArrayList<Object> values = new ArrayList<>();
            values.add(timeStamp);
            values.add(objectName);
            values.add(size);
            values.add(timeStamp - startTime);
            queue.add(StatsQueueEntry.create(new DBSeries(nameBuilder.toString(), DBSeries.OPS_COLUMNS), values));
        }
    }

    public void logObjectStoreStats(int providerId, String containerName, long totalSize, long objectCount) {
        synchronized (queue) {
            Long timeStamp = new Date().getTime();
            ArrayList<Object> values = new ArrayList<>();
            values.add(timeStamp);
            values.add(totalSize);
            values.add(objectCount);
            StringBuilder nameBuilder = new StringBuilder(DBSeries.OBJECT_STORE_SERIES);
            nameBuilder.append(".provider.").append(Integer.toString(providerId))
                    .append(".container.").append(containerName);
            queue.add(StatsQueueEntry.create(new DBSeries(nameBuilder.toString(), DBSeries.OBJECT_STORE_COLUMNS),
                    values));
        }
    }

    @VisibleForTesting
    public Queue<StatsQueueEntry> getQueue() {
        return queue;
    }

    private void submitValues() {
        logger.debug("Pushing to influxdb");
        try {
            List<Serie> series = prepareSeries();
            if (series != null) {
                for (Serie serie : series) {
                    db.write(DATABASE, TimeUnit.MILLISECONDS, serie);
                    removeProcessedValues(serie);
                }
            }
        } catch (Throwable e) {
            logger.error("Exception while pushing stats: " + e.getMessage());
            logger.error(Throwables.getStackTraceAsString(e));
        }
    }

    List<Serie> prepareSeries() {
        Map<String, Serie.Builder> builderMap = new HashMap<>();
        synchronized (queue) {
            ListIterator<StatsQueueEntry> iterator = ((LinkedList<StatsQueueEntry>) queue).listIterator();
            for (int i = 0; i < SUBMIT_LIMIT; i++) {
                StatsQueueEntry entry;
                if (!iterator.hasNext()) {
                    break;
                }
                entry = iterator.next();
                DBSeries dbSeries = entry.getDbSeries();
                if (!builderMap.containsKey(dbSeries.getName())) {
                    builderMap.put(dbSeries.getName(),
                            new Serie.Builder(dbSeries.getName()).columns(dbSeries.getColumns().toArray(
                                    new String[dbSeries.getColumns().size()])));
                }
                builderMap.get(dbSeries.getName()).values(entry.getValues().toArray());
            }
        }
        if (builderMap.isEmpty()) {
            return Collections.<Serie>emptyList();
        }
        return builderMap.values().stream().map(Serie.Builder::build).collect(Collectors.toList());
    }

    void removeProcessedValues(Serie serie) {
        synchronized (queue) {
            for (int i = 0; i < serie.getRows().size(); i++) {
                queue.remove();
            }
        }
    }

    public static final class DBSeries {
        public static final String OPS_SERIES = "ops";
        public static final List<String> OPS_COLUMNS = ImmutableList.of("time", "object", "size", "duration");
        public static final String OBJECT_STORE_SERIES = "object_store_stats";
        public static final List<String> OBJECT_STORE_COLUMNS = ImmutableList.of("time", "size", "objects");

        private final List<String> columns;
        private final String name;

        public DBSeries(String name, List<String> columns) {
            this.columns = columns;
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public List<String> getColumns() {
            return columns;
        }
    }
}
