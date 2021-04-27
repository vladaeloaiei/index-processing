package com.aeloaiei.dissertation.spark.config;

public class Configuration {

    /**
     * SPARK
     */
    public static final String SPARK_MASTER = "spark://spark-master:7077";

    /**
     * MONGO
     */
    public static final String MONGODB_HOST = "data-lake";
    public static final String MONGODB_PORT = "27017";
    public static final String MONGODB_DATABASE = "data-lake";
    public static final String MONGODB_USERNAME = "user";
    public static final String MONGODB_PASSWORD = "password";
    public static final String SPARK_MONGODB_INPUT_URI_KEY = "spark.mongodb.input.uri";
    public static final String SPARK_MONGODB_OUTPUT_URI_KEY = "spark.mongodb.output.uri";
    public static final String MONGODB_URI = String.format("mongodb://%s:%s@%s:%s/%s.%s",
            MONGODB_USERNAME, MONGODB_PASSWORD, MONGODB_HOST, MONGODB_PORT, MONGODB_DATABASE, "dummy");
}
