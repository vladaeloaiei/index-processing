package com.aeloaiei.dissertation.spark.repository;

import com.aeloaiei.dissertation.spark.model.WebUrl;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.Document;

import java.io.Serializable;

public class WebUrlRepository implements Serializable {
    private static final String COLLECTION_KEY = "collection";

    private ObjectMapper objectMapper = new ObjectMapper();

    public String getCollectionName() {
        return WebUrl.COLLECTION_NAME;
    }

    public JavaRDD<WebUrl> load(JavaSparkContext javaSparkContext) {
        ReadConfig readConfig = getReadConfig(javaSparkContext);

        return MongoSpark.load(javaSparkContext, readConfig)
                .map(Document::toJson)
                .map(x -> objectMapper.readValue(x, WebUrl.class));
    }

    private ReadConfig getReadConfig(JavaSparkContext javaSparkContext) {
        return ReadConfig.create(javaSparkContext)
                .withOption(COLLECTION_KEY, getCollectionName());
    }
}
