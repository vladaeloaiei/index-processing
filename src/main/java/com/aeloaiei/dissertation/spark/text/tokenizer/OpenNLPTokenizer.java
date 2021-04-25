package com.aeloaiei.dissertation.spark.text.tokenizer;

import com.aeloaiei.dissertation.spark.text.tokenizer.filters.LuceneEnglishStopWordFilter;
import com.aeloaiei.dissertation.spark.text.utils.OpenNLPUtils;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;
import org.modelmapper.internal.Pair;

import java.util.HashMap;
import java.util.Map;

public class OpenNLPTokenizer implements Tokenizer {

    transient private static final StanfordCoreNLP pipeline = OpenNLPUtils.getPipeline("tokenize, ssplit, pos, lemma");

    @Override
    public Pair<Integer, Map<String, Integer>> extract(String text) {

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




