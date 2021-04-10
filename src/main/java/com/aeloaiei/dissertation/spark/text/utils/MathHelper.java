package com.aeloaiei.dissertation.spark.text.utils;

import static java.lang.Math.log;

public class MathHelper {

    public static float logNormalizedTermFrequency(int termCount, int documentSize) {
        return (float) log(1 + (float) termCount / documentSize);
    }

    public static float inverseDocumentFrequencySmooth(int total, int appearances) {
        return (float) log((float) total / (1 + appearances)) + 1;
    }
}
