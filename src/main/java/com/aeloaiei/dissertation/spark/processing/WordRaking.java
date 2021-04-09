package com.aeloaiei.dissertation.spark.processing;

import com.aeloaiei.dissertation.spark.model.WebWord;

import java.io.Serializable;

import static java.lang.Math.log;

public class WordRaking implements Serializable {

    public WebWord computeWordRanking(WebWord webWord, int totalWebDocuments) {
        int wordAppearancesCount = webWord.getAppearances().size();

        for (WebWord.Appearance appearance : webWord.getAppearances()) {
            computeAppearanceRanking(appearance, wordAppearancesCount, totalWebDocuments);
        }

        return webWord;
    }

    private void computeAppearanceRanking(WebWord.Appearance appearance, int wordAppearancesCount, int totalWebDocuments) {
        appearance.setLogNormalizedTermFrequency((float) log(1 + (float) appearance.getTermCount() / appearance.getDocumentSizeInTerms()));
        appearance.setInverseDocumentFrequencySmooth((float) log((float) totalWebDocuments / (1 + wordAppearancesCount)) + 1);
    }
}
