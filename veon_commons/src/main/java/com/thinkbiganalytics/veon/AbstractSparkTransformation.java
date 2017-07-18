package com.thinkbiganalytics.veon;

import com.thinkbiganalytics.veon.util.SparkSessionBuilder;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.lit;

/**
 * Base class for transformation jobs
 */
public abstract class AbstractSparkTransformation {

    protected SparkSession spark;
    protected String sourceTable;
    protected String processedTimestamp;
    protected String whereCondition = "";
    private String targetTable;
    private boolean isIncrementalLoad = false;
    private String tempView;

    /**
     * Initialize input args, spark and other actions required to execute the transformation
     */
    protected abstract void init(String[] args);

    /**
     * Perform the transformation from source table to target table
     */
    protected abstract Dataset<Row> executeTransform();
    
    /**
     * Perform the loading of source table data
     */
    protected abstract void loadRequiredTables();
    
    /**
     * Register User Defined Functions required for transformation
     */
    protected abstract void registerUDFs(SparkSession spark);

    /**
     * Individual transformation job will call this method to orchestrate the sequence of
     * function calls
     *
     * @param args input arguments required for the transformation
     */
    public void doTransform(String[] args, Logger logger) {
        init(args);
        logger.info(">>-->> Source Data Frame is ready");
        Dataset<Row> transformedDataset = executeTransform();
        logger.info(">>-->> Target Data Frame is ready with transformations");
        doMerge(transformedDataset);
        logger.info(">>-->> Insertion to Hive Table got completed");
    }

    /**
     * Validate and initializes input job arguments
     *
     * @param args          job input args
     * @param tempViewName Temp table for transformed dataset
     */
    protected void initInputs(String[] args, String tempViewName, Logger logger) {
        if (args.length < 3) {
            System.out.println("Usage: spark-submit ... source_table target_table timestamp [incremental] [input_partition]");

            System.exit(1);
        }

        sourceTable = args[0];
        targetTable = args[1];
        processedTimestamp = args[2];

        if (args.length == 4) {
            isIncrementalLoad = Boolean.valueOf(args[3]);
        }

        if (args.length == 5) {
            whereCondition = String.format("where %s", args[4]);
        }

        this.tempView = tempViewName;
        logger.info(">>-->> Arguments Received: Source [" + sourceTable + "], Target [" + targetTable + "], Processing Timestamp [" + processedTimestamp + "], Incremental Insert [" + isIncrementalLoad + "] + Where Clause [" + whereCondition + "]");
    }

    /**
     * Initializes a spark session for the given app name
     */
    protected void initSparkSession(String appName) {
        spark = SparkSessionBuilder.getOrCreateSparkSession(appName);
    }

    /**
     * Perform merge from temp table to the target table
     */
    private void doMerge(Dataset<Row> transformedDataset) {
        //add processed timestamp column before merging
        transformedDataset.withColumn("processing_dttm", lit(processedTimestamp)).createOrReplaceTempView(tempView);

        if (isIncrementalLoad) {
            spark.sql(String.format("insert into table %s select * from %s", targetTable, tempView));
        } else {
            spark.sql(String.format("insert overwrite table %s select * from %s", targetTable, tempView));
        }
    }
}
