package com.aeloaiei.dissertation.spark.utils;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.modelmapper.internal.Pair;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Arrays.asList;

public class TextUtils implements Serializable {
    private static final Logger LOGGER = LogManager.getLogger(TextUtils.class);

    public Set<String> getParagraphs(String text) {
        return new HashSet<>(asList(text.split("\\R")));
    }

    public Pair<Integer, Map<String, Integer>> getWordsWithCount(String text) {
        Analyzer analyzer = new EnglishAnalyzer();
        Map<String, Integer> words = new HashMap<>();
        int totalWordsCount = 0;

        try (TokenStream tokenStream = analyzer.tokenStream(null, text)) {
            CharTermAttribute charTermAttribute = tokenStream.addAttribute(CharTermAttribute.class);
            tokenStream.reset();

            while (tokenStream.incrementToken()) {
                String word = charTermAttribute.toString();

                totalWordsCount += putWord(words, word);
            }
        } catch (IOException e) {
            LOGGER.error("Failed to extract tokens", e);
        }

        return Pair.of(totalWordsCount, words);
    }

    private int putWord(Map<String, Integer> words, String word) {
        if (!word.isEmpty()) {
            int wordCount = 0;

            if (words.containsKey(word)) {
                wordCount = words.get(word);
            }

            words.put(word, wordCount + 1);
            return 1;
        }

        return 0;
    }
}

