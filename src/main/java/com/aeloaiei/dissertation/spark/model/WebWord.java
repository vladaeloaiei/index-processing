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
    private Set<Appearance> appearances;

    public WebWord(Entry entry) {
        this.word = entry.word;
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
        private Float inverseDocumentFrequencySmooth;

        public Appearance(Entry entry) {
            this.location = entry.location;
            this.paragraphsIds = new HashSet<String>() {{
                add(entry.paragraphId);
            }};
            this.termCount = entry.termCount;
            this.documentSizeInTerms = entry.paragraphSizeInTerms;
            logNormalizedTermFrequency = 0F;
            inverseDocumentFrequencySmooth = 0F;
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
        private Appearance.Entry appearance;
    }
}
