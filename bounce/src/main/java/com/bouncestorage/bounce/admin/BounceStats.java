/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import java.util.Date;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.Queue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Serie;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BounceStats {
    public static final String DATABASE = "bounce";
    public static final String OPS_SERIES = "ops";
    public static final String[] OPS_COLUMNS = {"time", "op", "provider", "container", "object", "size", "duration"};
    public static final String ENDPOINT = "http://localhost:8086";
    public static final String USER = "bounce";
    public static final String PASSWORD = "bounce";

    private static final int SUBMIT_LIMIT = 100;
    private static final int STATS_INTERVAL = 30;

    private InfluxDB db;
    private Logger logger = LoggerFactory.getLogger(BounceStats.class);
    private final Queue<Object[]> opsQueue;
    private final ScheduledExecutorService scheduler;

    public BounceStats() {
        db = InfluxDBFactory.connect(ENDPOINT, USER, PASSWORD);
        opsQueue = new LinkedList<>();
        scheduler = new ScheduledThreadPoolExecutor(0);
    }

    public void start() {
        logger.debug("Starting the stats service");
        scheduler.scheduleAtFixedRate(() -> submitValues(), 0, STATS_INTERVAL, TimeUnit.SECONDS);
    }

    public void shutdown() {
        if (!scheduler.isShutdown()) {
            scheduler.shutdown();
        }
    }

    public void logOperation(String opName, String providerName, String containerName, String objectName, Long size,
                             Long startTime) {
        synchronized (opsQueue) {
            Long timeStamp = new Date().getTime();
            Object[] values = {timeStamp, opName, providerName, containerName, objectName, size, timeStamp - startTime};
            opsQueue.add(values);
        }
    }

    @VisibleForTesting
    public Queue<Object[]> getOpsQueue() {
        return opsQueue;
    }

    private void submitValues() {
        logger.debug("Pushing to influxdb");
        try {
            Serie serie = prepareSerie();
            if (serie != null) {
                db.write(DATABASE, TimeUnit.MILLISECONDS, serie);
                removeProcessedValues(serie);
            }
        } catch (Throwable e) {
            logger.error("Exception while pushing stats: " + e.getMessage());
            logger.error(Throwables.getStackTraceAsString(e));
        }
    }

    Serie prepareSerie() {
        Serie.Builder serieBuilder = null;
        synchronized (opsQueue) {
            ListIterator<Object[]> iterator = ((LinkedList<Object[]>) opsQueue).listIterator();
            for (int i = 0; i < SUBMIT_LIMIT; i++) {
                Object[] entry;
                if (!iterator.hasNext()) {
                    break;
                }
                entry = iterator.next();
                if (serieBuilder == null) {
                    serieBuilder = new Serie.Builder(OPS_SERIES).columns(OPS_COLUMNS);
                }
                serieBuilder.values(entry);
            }
        }
        if (serieBuilder == null) {
            return null;
        }
        return serieBuilder.build();
    }

    void removeProcessedValues(Serie serie) {
        synchronized (opsQueue) {
            for (int i = 0; i < serie.getRows().size(); i++) {
                opsQueue.remove();
            }
        }
    }
}
