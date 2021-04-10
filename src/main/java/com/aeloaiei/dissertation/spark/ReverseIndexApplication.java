package com.aeloaiei.dissertation.spark;

import com.aeloaiei.dissertation.spark.job.PageRankingJob;
import com.aeloaiei.dissertation.spark.job.DocumentProcessingJob;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import static com.aeloaiei.dissertation.spark.config.Configuration.MONGODB_URI;
import static com.aeloaiei.dissertation.spark.config.Configuration.SPARK_MONGODB_INPUT_URI_KEY;
import static com.aeloaiei.dissertation.spark.config.Configuration.SPARK_MONGODB_OUTPUT_URI_KEY;

public class ReverseIndexApplication {
    public static void main(final String[] args) throws InterruptedException {
        /* Create the SparkSession.
         * If config arguments are passed from the command line using --conf,
         * parse args for the values to set.
         */
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("MongoSparkConnectorIntro")
                .config(SPARK_MONGODB_INPUT_URI_KEY, MONGODB_URI)
                .config(SPARK_MONGODB_OUTPUT_URI_KEY, MONGODB_URI)
                .getOrCreate();
        // Create a JavaSparkContext using the SparkSession's SparkContext object
        JavaSparkContext javaSparkContext = new JavaSparkContext(spark.sparkContext());

        new PageRankingJob().run(javaSparkContext);
        new DocumentProcessingJob().run(javaSparkContext);

        javaSparkContext.close();
    }
}
