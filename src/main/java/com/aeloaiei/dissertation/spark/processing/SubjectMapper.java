package com.aeloaiei.dissertation.spark.processing;

import com.aeloaiei.dissertation.spark.model.WebDocument;
import com.aeloaiei.dissertation.spark.model.WebDocumentSubject;
import com.aeloaiei.dissertation.spark.text.OpenNLPKeywords;

import java.io.Serializable;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

public class SubjectMapper implements Serializable {
    public OpenNLPKeywords openNLPKeywords;

    public SubjectMapper() {
        openNLPKeywords = new OpenNLPKeywords();
    }

    public WebDocumentSubject map(WebDocument webDocument) {
        Set<WebDocumentSubject.Subject> subjects = openNLPKeywords.extract(webDocument.getContent())
                .getRight()
                .entrySet()
                .stream()
                .filter(t -> !t.getKey().isEmpty())
                .map(t -> new WebDocumentSubject.Subject(t.getKey(), t.getValue()))
                .collect(toSet());

        return new WebDocumentSubject(webDocument.getLocation(), subjects);
    }
}
