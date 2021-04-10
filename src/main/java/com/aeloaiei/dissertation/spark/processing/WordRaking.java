package com.aeloaiei.dissertation.spark.processing;

import com.aeloaiei.dissertation.spark.model.WebWord;

import java.io.Serializable;

import static com.aeloaiei.dissertation.spark.text.utils.MathHelper.inverseDocumentFrequencySmooth;
import static com.aeloaiei.dissertation.spark.text.utils.MathHelper.logNormalizedTermFrequency;

public class WordRaking implements Serializable {

    public WebWord computeWordRanking(WebWord webWord, int totalWebDocuments) {
        int wordAppearancesCount = webWord.getAppearances().size();

        webWord.setInverseDocumentFrequencySmooth(inverseDocumentFrequencySmooth(totalWebDocuments, wordAppearancesCount));

        for (WebWord.Appearance appearance : webWord.getAppearances()) {
            computeAppearanceRanking(appearance);
        }

        return webWord;
    }

    private void computeAppearanceRanking(WebWord.Appearance appearance) {
        appearance.setLogNormalizedTermFrequency(logNormalizedTermFrequency(appearance.getTermCount(), appearance.getDocumentSizeInTerms()));
    }
}
