package com.aeloaiei.dissertation.spark.processing;

import com.aeloaiei.dissertation.spark.model.WebDocument;
import com.aeloaiei.dissertation.spark.model.WebParagraph;
import com.aeloaiei.dissertation.spark.utils.TextUtils;

import java.io.Serializable;
import java.util.Iterator;

public class ParagraphMapper implements Serializable {
    public TextUtils textUtils;

    public ParagraphMapper() {
        textUtils = new TextUtils();
    }

    public Iterator<WebParagraph> map(WebDocument webDocument) {
        return textUtils.getParagraphs(webDocument.getContent())
                .stream()
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(p -> new WebParagraph(webDocument.getLocation(), p))
                .iterator();
    }
}
