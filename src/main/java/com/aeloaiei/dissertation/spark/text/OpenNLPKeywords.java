package com.aeloaiei.dissertation.spark.text;

import com.aeloaiei.dissertation.spark.text.tokenizer.filters.LuceneEnglishStopWordFilter;
import com.aeloaiei.dissertation.spark.text.utils.SerializableUtils;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.semgraph.SemanticGraph;
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations;
import edu.stanford.nlp.semgraph.SemanticGraphEdge;
import edu.stanford.nlp.util.CoreMap;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.modelmapper.internal.Pair;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.aeloaiei.dissertation.spark.text.utils.SerializableUtils.getPipeline;

public class OpenNLPKeywords implements Serializable {
    private static final Logger LOGGER = LogManager.getLogger(OpenNLPKeywords.class);

    private static final Set<String> TARGET_RELATIONS = new HashSet<String>() {{
        add("nsubj");
        add("obj");
        add("compound");
    }};

    /**
     * Extracts subjects, objects and compounds from a given text
     *
     * @param text Input text
     * @return subjects and objects
     */
    public Pair<Integer, Map<String, Integer>> extract(String text) {
        Annotation document = new Annotation(text);
        Map<String, Integer> tokens = new HashMap<>();
        int totalTokenCount = 0;

        getPipeline("tokenize, ssplit, pos, lemma, depparse").annotate(document);

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
