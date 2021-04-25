package com.aeloaiei.dissertation.spark.text.utils;

import edu.stanford.nlp.pipeline.StanfordCoreNLP;

import java.util.Properties;

public class OpenNLPUtils {

    public static StanfordCoreNLP getPipeline(String annotators) {
        Properties properties = new Properties();
        properties.put("annotators", annotators);
        return new StanfordCoreNLP(properties);
    }
}
