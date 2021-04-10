package com.aeloaiei.dissertation.spark.text.tokenizer;

import com.aeloaiei.dissertation.spark.text.tokenizer.filters.LuceneEnglishStopWordFilter;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;
import org.modelmapper.internal.Pair;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class OpenNLPTokenizer implements Tokenizer {
    private final Properties properties;

    public OpenNLPTokenizer() {
        // Define annotators: tokenize, split, POS tagging, lemmatization
        properties = new Properties();
        properties.put("annotators", "tokenize, ssplit, pos, lemma");
    }

    @Override
    public Pair<Integer, Map<String, Integer>> extract(String text) {
        // StanfordCoreNLP is not serializable, so in spark we need to create a new instance each time
        StanfordCoreNLP pipeline = new StanfordCoreNLP(properties);
        Annotation document = new Annotation(text);
        Map<String, Integer> words = new HashMap<>();
        int totalWordsCount = 0;

        pipeline.annotate(document);

        for (CoreMap sentence : document.get(CoreAnnotations.SentencesAnnotation.class)) {
            for (CoreLabel token : sentence.get(CoreAnnotations.TokensAnnotation.class)) {
                totalWordsCount += LuceneEnglishStopWordFilter.putWord(words, token.lemma().toLowerCase());
            }
        }

        return Pair.of(totalWordsCount, words);
    }
}




