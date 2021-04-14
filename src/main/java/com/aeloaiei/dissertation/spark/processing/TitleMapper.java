package com.aeloaiei.dissertation.spark.processing;

import com.aeloaiei.dissertation.spark.model.WebDocument;
import com.aeloaiei.dissertation.spark.model.WebTitle;
import com.aeloaiei.dissertation.spark.text.OpenNLPKeywords;

import java.io.Serializable;

public class TitleMapper implements Serializable {
    public WebTitle map(WebDocument webDocument) {
        return new WebTitle(webDocument.getLocation(), webDocument.getTitle());
    }
}
