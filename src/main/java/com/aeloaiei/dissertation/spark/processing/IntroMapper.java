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
        return new WebIntro(webDocument.getLocation(), textUtils.getIntro(webDocument.getContent()));
    }
}
