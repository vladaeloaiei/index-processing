package com.aeloaiei.dissertation.spark.text.tokenizer.filters;

import java.io.Serializable;
import java.util.Map;

import static org.apache.lucene.analysis.en.EnglishAnalyzer.ENGLISH_STOP_WORDS_SET;

public class LuceneEnglishStopWordFilter implements Serializable {
    public static int putWord(Map<String, Integer> words, String word) {
        if (ENGLISH_STOP_WORDS_SET.contains(word)) {
            return 0;
        }

        if (word.isEmpty()) {
            return 0;
        }

        int wordCount = 0;

        if (words.containsKey(word)) {
            wordCount = words.get(word);
        }

        words.put(word, wordCount + 1);
        return 1;
    }
}
