package com.aeloaiei.dissertation.spark.repository;

import com.aeloaiei.dissertation.spark.model.WebWord;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.WriteConfig;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.Document;

import java.io.Serializable;

public class WebWordRepository implements Serializable {
    private static final String COLLECTION_KEY = "collection";

    private ObjectMapper objectMapper = new ObjectMapper();

    public String getCollectionName() {
        return WebWord.COLLECTION_NAME;
    }

    public void save(JavaSparkContext javaSparkContext, JavaRDD<WebWord> rdd) {
        WriteConfig writeConfig = getWriteConfig(javaSparkContext);

        JavaRDD<Document> documents = rdd.map(x -> objectMapper.writeValueAsString(x))
                .map(x -> objectMapper.readValue(x, Document.class));

        MongoSpark.save(documents, writeConfig);
    }

    private WriteConfig getWriteConfig(JavaSparkContext javaSparkContext) {
        return WriteConfig.create(javaSparkContext)
                .withOption(COLLECTION_KEY, getCollectionName());
    }
}
