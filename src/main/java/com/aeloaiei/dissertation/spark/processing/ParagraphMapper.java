package com.aeloaiei.dissertation.spark.processing;

import com.aeloaiei.dissertation.spark.model.WebDocument;
import com.aeloaiei.dissertation.spark.model.WebParagraph;
import com.aeloaiei.dissertation.spark.text.utils.TextUtils;

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
                .map(p -> new WebParagraph(webDocument.getLocation(), p))
                .iterator();
    }
}
