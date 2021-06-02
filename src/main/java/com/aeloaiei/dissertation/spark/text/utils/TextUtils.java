package com.aeloaiei.dissertation.spark.text.utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class TextUtils implements Serializable {
    public List<String> getParagraphs(String text) {
        List<String> paragraphs = new ArrayList<>();
        StringBuilder builder = new StringBuilder();

        for (int i = 0; i < text.length(); ++i) {
            if (text.charAt(i) == '\n' || text.charAt(i) == '\r') {
                String paragraph = builder.toString().trim();

                if (!paragraph.isEmpty()) {
                    paragraphs.add(paragraph);
                }

                builder.setLength(0);
            } else {
                builder.append(text.charAt(i));
            }
        }
        return paragraphs;
    }

    public String getIntro(String text) {
        StringBuilder builder = new StringBuilder();

        for (int i = 0; i < text.length(); ++i) {
            if (text.charAt(i) == '\n' || text.charAt(i) == '\r') {
                String paragraph = builder.toString().trim();

                if (!paragraph.isEmpty()) {
                    return paragraph;
                }
            } else {
                builder.append(text.charAt(i));
            }
        }

        return "";
    }
}

