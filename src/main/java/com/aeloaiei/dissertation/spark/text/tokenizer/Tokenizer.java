package com.aeloaiei.dissertation.spark.text.tokenizer;

import org.modelmapper.internal.Pair;

import java.io.Serializable;
import java.util.Map;

public interface Tokenizer extends Serializable {
    /**
     * Extract tokens from a given text
     *
     * @param text Input text
     * @return a {@link Pair} of number of total words from the given text and a map of tokens and their appearance
     */
    public Pair<Integer, Map<String, Integer>> extract(String text);
}
