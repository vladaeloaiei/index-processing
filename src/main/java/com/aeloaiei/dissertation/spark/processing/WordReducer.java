package com.aeloaiei.dissertation.spark.processing;

import com.aeloaiei.dissertation.spark.model.WebWord;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

import static java.util.Objects.isNull;
import static java.util.stream.Collectors.toMap;

public class WordReducer implements Serializable {
    public WebWord seq(WebWord webWord, WebWord.Entry entryWordReverseIndex) {
        if (isEmptyIndex(webWord)) {
            return new WebWord(entryWordReverseIndex);
        } else {
            return comb(webWord, new WebWord(entryWordReverseIndex));
        }
    }

    private boolean isEmptyIndex(WebWord webWord) {
        return isNull(webWord.getWord()) || webWord.getWord().isEmpty();
    }

    public WebWord comb(WebWord webWord1, WebWord webWord2) {
        if (isEmptyIndex(webWord1)) {
            return webWord2;
        } else if (isEmptyIndex(webWord2)) {
            return webWord1;
        } else {
            Map<String, WebWord.Appearance> webWord1Appearances = webWord1.getAppearances()
                    .stream()
                    .collect(toMap(WebWord.Appearance::getLocation, x -> x));
            Map<String, WebWord.Appearance> webWord2Appearances = webWord2.getAppearances()
                    .stream()
                    .collect(toMap(WebWord.Appearance::getLocation, x -> x));

            for (String webWord2AppearanceLocation : webWord2Appearances.keySet()) {
                if (webWord1Appearances.containsKey(webWord2AppearanceLocation)) {
                    WebWord.Appearance webWord1Appearance = webWord1Appearances.get(webWord2AppearanceLocation);
                    WebWord.Appearance webWord2Appearance = webWord2Appearances.get(webWord2AppearanceLocation);
                    WebWord.Appearance mergedAppearance = mergeAppearances(webWord1Appearance, webWord2Appearance);

                    webWord1Appearances.put(webWord2AppearanceLocation, mergedAppearance);
                }
            }

            return webWord1;
        }
    }

    private WebWord.Appearance mergeAppearances(WebWord.Appearance a, WebWord.Appearance b) {
        if (!Objects.equals(a.getLocation(), b.getLocation())) {
            throw new RuntimeException("Can not merge different appearances: " + a + b);
        }

        a.getParagraphsIds().addAll(b.getParagraphsIds());
        a.setTermCount(a.getTermCount() + b.getTermCount());
        a.setDocumentSizeInTerms(a.getDocumentSizeInTerms() + b.getDocumentSizeInTerms());
        return a;
    }
}
