package com.aeloaiei.dissertation.spark.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

import static java.lang.Math.abs;

@Data
@NoArgsConstructor
public class WebParagraph implements Serializable {
    public static final String COLLECTION_NAME = "web-paragraphs";

    @JsonProperty("_id")
    private String id;
    private String location;
    private String content;

    public WebParagraph(String location, String content) {
        this.id = abs(location.hashCode()) + "_" + abs(content.hashCode());
        this.location = location;
        this.content = content;
    }
}
