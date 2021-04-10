package com.aeloaiei.dissertation.spark.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class WebWord implements Serializable {
    public static final String COLLECTION_NAME = "web-words";

    @JsonProperty("_id")
    private String word;
    private Float inverseDocumentFrequencySmooth;
    private Set<Appearance> appearances;

    public WebWord(Entry entry) {
        this.word = entry.word;
        this.inverseDocumentFrequencySmooth = entry.inverseDocumentFrequencySmooth;
        this.appearances = new HashSet<Appearance>() {{
            add(new Appearance(entry.getAppearance()));
        }};
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Appearance implements Serializable {
        private String location;
        private Set<String> paragraphsIds;
        private Integer termCount;
        private Integer documentSizeInTerms;
        private Float logNormalizedTermFrequency;

        public Appearance(Entry entry) {
            this.location = entry.location;
            this.paragraphsIds = new HashSet<String>() {{
                add(entry.paragraphId);
            }};
            this.termCount = entry.termCount;
            this.documentSizeInTerms = entry.paragraphSizeInTerms;
            logNormalizedTermFrequency = 0F;
        }

        @Data
        @NoArgsConstructor
        @AllArgsConstructor
        public static class Entry implements Serializable {
            private String location;
            private String paragraphId;
            private Integer termCount;
            private Integer paragraphSizeInTerms;
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Entry implements Serializable {
        private String word;
        private Float inverseDocumentFrequencySmooth;
        private Appearance.Entry appearance;

        public Entry(String word, Appearance.Entry appearance) {
            this.word = word;
            this.inverseDocumentFrequencySmooth = 0F;
            this.appearance = appearance;
        }
    }
}
