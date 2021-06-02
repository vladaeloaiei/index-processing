package com.aeloaiei.dissertation.spark.text.utils;

import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.en.EnglishAnalyzer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static java.util.Objects.isNull;

public class SerializableUtils {
    private static final Logger LOGGER = LogManager.getLogger(SerializableUtils.class);

    private static Map<String, StanfordCoreNLP> pipelines;
    private static EnglishAnalyzer englishAnalyser;

    public static StanfordCoreNLP getPipeline(String annotators) {
        LOGGER.info("Trying to get pipeline for: " + annotators);

        if (isNull(pipelines)) {
            LOGGER.info("Pipeline map is null. Creating a new one.");
            pipelines = new HashMap<>();
        }

        if (!pipelines.containsKey(annotators) || isNull(pipelines.get(annotators))) {
            LOGGER.info("Pipeline does not exists for the provided annotators. Creating a new one.");
            pipelines.put(annotators, buildPipeline(annotators));
        }

        LOGGER.info("Returning pipeline for: " + annotators);
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
