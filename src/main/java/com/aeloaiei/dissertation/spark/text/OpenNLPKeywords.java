package com.aeloaiei.dissertation.spark.text;

import com.aeloaiei.dissertation.spark.text.tokenizer.filters.LuceneEnglishStopWordFilter;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.semgraph.SemanticGraph;
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations;
import edu.stanford.nlp.semgraph.SemanticGraphEdge;
import edu.stanford.nlp.util.CoreMap;
import org.modelmapper.internal.Pair;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class OpenNLPKeywords implements Serializable {
    private static final Set<String> TARGET_RELATIONS = new HashSet<String>() {{
        add("nsubj");
        add("obj");
        add("compound");
    }};

    private final Properties properties;

    public OpenNLPKeywords() {
        this.properties = new Properties();
        // Define annotators: tokenize, split, POS tagging, lemmatization, parsing
        this.properties.put("annotators", "tokenize, ssplit, pos, lemma, depparse");
    }

    /**
     * Extracts subjects, objects and compounds from a given text
     *
     * @param text Input text
     * @return subjects and objects
     */
    public Pair<Integer, Map<String, Integer>> extract(String text) {
        // StanfordCoreNLP is not serializable, so in spark we need to create a new instance each time
        StanfordCoreNLP pipeline = new StanfordCoreNLP(properties);
        Annotation document = new Annotation(text);
        Map<String, Integer> tokens = new HashMap<>();
        int totalTokenCount = 0;

        pipeline.annotate(document);

        for (CoreMap sentence : document.get(CoreAnnotations.SentencesAnnotation.class)) {
            SemanticGraph tree = sentence.get(SemanticGraphCoreAnnotations.EnhancedPlusPlusDependenciesAnnotation.class);

            for (SemanticGraphEdge relation : tree.edgeIterable()) {
                if (TARGET_RELATIONS.contains(relation.getRelation().getShortName())) {
                    totalTokenCount += LuceneEnglishStopWordFilter.putWord(tokens, relation.getDependent().lemma().toLowerCase());
                }
            }
        }

        return Pair.of(totalTokenCount, tokens);
    }
}
