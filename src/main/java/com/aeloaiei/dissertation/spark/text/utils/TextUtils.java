package com.aeloaiei.dissertation.spark.text.utils;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import static java.util.Arrays.asList;

public class TextUtils implements Serializable {
    public Set<String> getParagraphs(String text) {
        return new HashSet<>(asList(text.split("\\R")));
    }
}

