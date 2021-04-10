package com.aeloaiei.dissertation.spark.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Set;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class WebDocumentSubject implements Serializable {
    public static final String COLLECTION_NAME = "web-document-subjects";

    @JsonProperty("_id")
    private String location;
    private Set<Subject> subjects;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Subject implements Serializable {
        private String subject;
        private Integer count;
    }
}
