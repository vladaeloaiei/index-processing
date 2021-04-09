package com.aeloaiei.dissertation.spark.repository;

import com.aeloaiei.dissertation.spark.model.WebDocument;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.Document;

import java.io.Serializable;

public class WebDocumentRepository implements Serializable {
    private static final String COLLECTION_KEY = "collection";

    private ObjectMapper objectMapper = new ObjectMapper();

    public String getCollectionName() {
        return WebDocument.COLLECTION_NAME;
    }

    public JavaRDD<WebDocument> load(JavaSparkContext javaSparkContext) {
        ReadConfig readConfig = getReadConfig(javaSparkContext);

        return MongoSpark.load(javaSparkContext, readConfig)
                .map(Document::toJson)
                .map(x -> objectMapper.readValue(x, WebDocument.class));
    }

    private ReadConfig getReadConfig(JavaSparkContext javaSparkContext) {
        return ReadConfig.create(javaSparkContext)
                .withOption(COLLECTION_KEY, getCollectionName());
    }
}
