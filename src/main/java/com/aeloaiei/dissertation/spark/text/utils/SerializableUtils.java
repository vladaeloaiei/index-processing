package com.aeloaiei.dissertation.spark.text.utils;

import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import org.apache.lucene.analysis.en.EnglishAnalyzer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static java.util.Objects.isNull;

public class SerializableUtils {

    private static Map<String, StanfordCoreNLP> pipelines;
    private static EnglishAnalyzer englishAnalyser;

    public static StanfordCoreNLP getPipeline(String annotators) {
        if (isNull(pipelines)) {
            pipelines = new HashMap<>();
        }

        if (!pipelines.containsKey(annotators)) {
            pipelines.put(annotators, buildPipeline(annotators));
        }

        return pipelines.get(annotators);
    }

    public static EnglishAnalyzer getEnglishAnalyser() {
        if (isNull(englishAnalyser)) {
            englishAnalyser = new EnglishAnalyzer();
        }

        return englishAnalyser;
    }

    private static StanfordCoreNLP buildPipeline(String annotators) {
        Properties properties = new Properties();
        properties.put("annotators", annotators);
        return new StanfordCoreNLP(properties);
    }

    private SerializableUtils() {
    }
}
