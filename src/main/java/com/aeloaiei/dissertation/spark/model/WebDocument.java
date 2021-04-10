package com.aeloaiei.dissertation.spark.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class WebDocument implements Serializable {
    public static final String COLLECTION_NAME = "web-documents";

    @JsonProperty("_id")
    private String location;
    private String title;
    private String content;
}
