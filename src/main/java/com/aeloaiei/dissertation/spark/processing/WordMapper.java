package com.aeloaiei.dissertation.spark.processing;

import com.aeloaiei.dissertation.spark.model.WebParagraph;
import com.aeloaiei.dissertation.spark.model.WebWord;
import com.aeloaiei.dissertation.spark.text.tokenizer.LuceneTokenizer;
import org.modelmapper.internal.Pair;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;

public class WordMapper implements Serializable {
    public LuceneTokenizer luceneTokenizer;

    public WordMapper() {
        luceneTokenizer = new LuceneTokenizer();
    }

    public Iterator<WebWord.Entry> map(WebParagraph webParagraph) {
        Pair<Integer, Map<String, Integer>> paragraph = luceneTokenizer.extract(webParagraph.getContent());
        int paragraphWordsCount = paragraph.getLeft();
        Map<String, Integer> words = paragraph.getRight();

        return words.entrySet()
                .stream()
                .map(word -> getEntry(webParagraph, word, paragraphWordsCount))
                .iterator();
    }

    private WebWord.Entry getEntry(WebParagraph webParagraph, Map.Entry<String, Integer> word, int wordsCount) {
        return new WebWord.Entry(
                word.getKey(),
                new WebWord.Appearance.Entry(webParagraph.getLocation(), webParagraph.getId(), word.getValue(), wordsCount));
    }
}
