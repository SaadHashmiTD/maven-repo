package com.thinkbiganalytics.veon.util;

import org.apache.spark.sql.SparkSession;

public class SparkSessionBuilder {

    /**
     * Create or retrieve spark session
     */
    public static SparkSession getOrCreateSparkSession(String appName) {
        return SparkSession
                .builder()
                .appName(appName)
                .enableHiveSupport()
                .getOrCreate();
    }
}
