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
public class WebUrlRank implements Serializable {
    public static final String COLLECTION_NAME = "web-url-ranks";

    @JsonProperty("_id")
    private String location;
    private float rank;
}
