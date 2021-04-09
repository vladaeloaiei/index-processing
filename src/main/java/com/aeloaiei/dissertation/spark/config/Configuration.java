package com.aeloaiei.dissertation.spark.config;

public class Configuration {
    private static final String MONGODB_USER = "user";
    private static final String MONGODB_PASSWORD = "password";
    private static final String MONGODB_ADDRESS = "localhost";
    private static final String MONGODB_PORT = "27017";
    private static final String MONGODB_DATABASE = "data-lake";
    private static final String MONGODB_DUMMY_COLLECTION = "dummy-collection";

    public static final String SPARK_MONGODB_INPUT_URI_KEY = "spark.mongodb.input.uri";
    public static final String SPARK_MONGODB_OUTPUT_URI_KEY = "spark.mongodb.output.uri";
    public static final String MONGODB_URI = String.format("mongodb://%s:%s@%s:%s/%s.%s",
            MONGODB_USER, MONGODB_PASSWORD, MONGODB_ADDRESS, MONGODB_PORT, MONGODB_DATABASE, MONGODB_DUMMY_COLLECTION);
}
