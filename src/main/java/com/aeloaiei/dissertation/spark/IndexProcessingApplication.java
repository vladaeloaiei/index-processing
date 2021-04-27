package com.aeloaiei.dissertation.spark;

import com.aeloaiei.dissertation.spark.job.DocumentProcessingJob;
import com.aeloaiei.dissertation.spark.job.PageRankingJob;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import static com.aeloaiei.dissertation.spark.config.Configuration.MONGODB_URI;
import static com.aeloaiei.dissertation.spark.config.Configuration.SPARK_MASTER;
import static com.aeloaiei.dissertation.spark.config.Configuration.SPARK_MONGODB_INPUT_URI_KEY;
import static com.aeloaiei.dissertation.spark.config.Configuration.SPARK_MONGODB_OUTPUT_URI_KEY;

public class IndexProcessingApplication {
    public static void main(final String[] args) {
        SparkSession spark = SparkSession.builder()
                .master(SPARK_MASTER)
                .appName("IndexProcessingApplication")
                .config(SPARK_MONGODB_INPUT_URI_KEY, MONGODB_URI)
                .config(SPARK_MONGODB_OUTPUT_URI_KEY, MONGODB_URI)
                .getOrCreate();
        JavaSparkContext javaSparkContext = new JavaSparkContext(spark.sparkContext());

        new PageRankingJob().run(javaSparkContext);
        new DocumentProcessingJob().run(javaSparkContext);

        javaSparkContext.close();
    }
}
