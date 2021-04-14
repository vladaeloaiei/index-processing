package com.aeloaiei.dissertation.spark.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class WebTitle implements Serializable {
    public static final String COLLECTION_NAME = "web-titles";

    @JsonProperty("_id")
    private String location;
    private String title;
}
