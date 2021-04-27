package com.aeloaiei.dissertation.spark.processing;

import com.aeloaiei.dissertation.spark.model.WebDocument;
import com.aeloaiei.dissertation.spark.model.WebIntro;
import com.aeloaiei.dissertation.spark.text.utils.TextUtils;

import java.io.Serializable;

public class IntroMapper implements Serializable {
    public TextUtils textUtils;

    public IntroMapper() {
        textUtils = new TextUtils();
    }

    public WebIntro map(WebDocument webDocument) {
        return textUtils.getParagraphs(webDocument.getContent())
                .stream()
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .findFirst()
                .map(p -> new WebIntro(webDocument.getLocation(), p))
                .orElseGet(() -> new WebIntro(webDocument.getLocation(), ""));
    }
}
