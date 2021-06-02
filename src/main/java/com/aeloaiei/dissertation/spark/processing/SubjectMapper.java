package com.aeloaiei.dissertation.spark.processing;

import com.aeloaiei.dissertation.spark.model.WebDocumentSubject;
import com.aeloaiei.dissertation.spark.model.WebIntro;
import com.aeloaiei.dissertation.spark.text.OpenNLPKeywords;

import java.io.Serializable;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

public class SubjectMapper implements Serializable {
    public OpenNLPKeywords openNLPKeywords;

    public SubjectMapper() {
        openNLPKeywords = new OpenNLPKeywords();
    }

    public WebDocumentSubject map(WebIntro webIntro) {
        Set<WebDocumentSubject.Subject> subjects = openNLPKeywords.extract(webIntro.getContent())
                .getRight()
                .entrySet()
                .stream()
                .filter(t -> !t.getKey().isEmpty())
                .map(t -> new WebDocumentSubject.Subject(t.getKey(), t.getValue()))
                .collect(toSet());

        return new WebDocumentSubject(webIntro.getLocation(), subjects);
    }
}
