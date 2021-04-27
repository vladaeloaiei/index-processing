package com.aeloaiei.dissertation.spark.text.tokenizer;

import com.aeloaiei.dissertation.spark.text.tokenizer.filters.LuceneEnglishStopWordFilter;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.modelmapper.internal.Pair;

import java.util.HashMap;
import java.util.Map;

import static com.aeloaiei.dissertation.spark.text.utils.SerializableUtils.getEnglishAnalyser;

public class LuceneTokenizer implements Tokenizer {
    private static final Logger LOGGER = LogManager.getLogger(LuceneTokenizer.class);

    @Override
    public Pair<Integer, Map<String, Integer>> extract(String text) {
        Map<String, Integer> words = new HashMap<>();
        int totalWordsCount = 0;

        try (TokenStream tokenStream = getEnglishAnalyser().tokenStream(null, text)) {
            CharTermAttribute charTermAttribute = tokenStream.addAttribute(CharTermAttribute.class);
            tokenStream.reset();

            while (tokenStream.incrementToken()) {
                String word = charTermAttribute.toString();

                totalWordsCount += LuceneEnglishStopWordFilter.putWord(words, word.toLowerCase());
            }
        } catch (Exception e) {
            LOGGER.error("Failed to extract words", e);
        }

        return Pair.of(totalWordsCount, words);
    }
}
