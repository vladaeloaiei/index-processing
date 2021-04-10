package com.aeloaiei.dissertation.spark.job;

import com.aeloaiei.dissertation.spark.model.WebDocument;
import com.aeloaiei.dissertation.spark.model.WebDocumentSubject;
import com.aeloaiei.dissertation.spark.model.WebParagraph;
import com.aeloaiei.dissertation.spark.model.WebWord;
import com.aeloaiei.dissertation.spark.processing.ParagraphMapper;
import com.aeloaiei.dissertation.spark.processing.SubjectMapper;
import com.aeloaiei.dissertation.spark.processing.WordMapper;
import com.aeloaiei.dissertation.spark.processing.WordRaking;
import com.aeloaiei.dissertation.spark.processing.WordReducer;
import com.aeloaiei.dissertation.spark.repository.WebDocumentRepository;
import com.aeloaiei.dissertation.spark.repository.WebDocumentSubjectRepository;
import com.aeloaiei.dissertation.spark.repository.WebParagraphRepository;
import com.aeloaiei.dissertation.spark.repository.WebWordRepository;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import static org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK;

public class DocumentProcessingJob implements SparkJob {
    private static final Logger LOGGER = LogManager.getLogger(DocumentProcessingJob.class);

    @Override
    public void run(JavaSparkContext javaSparkContext) {
        WebDocumentRepository webDocumentRepository = new WebDocumentRepository();
        WebParagraphRepository webParagraphRepository = new WebParagraphRepository();
        WebDocumentSubjectRepository webDocumentSubjectRepository = new WebDocumentSubjectRepository();
        WebWordRepository webWordRepository = new WebWordRepository();

        JavaRDD<WebDocument> webDocuments;
        JavaRDD<WebParagraph> webParagraphs;
        JavaRDD<WebDocumentSubject> webDocumentSubjects;
        JavaRDD<WebWord> webWords;
        int webDocumentsCount;

        LOGGER.info("Starting the word indexing job");

        webDocuments = webDocumentRepository.load(javaSparkContext);
        webDocuments.persist(MEMORY_AND_DISK());
        webDocumentsCount = (int) webDocuments.count();

        webParagraphs = extractParagraphs(webDocuments);
        webDocumentSubjects = extractSubjects(webDocuments);
        webWords = extractWords(webParagraphs);
        webWords = rankWords(webWords, webDocumentsCount);

        LOGGER.info("Saving paragraphs..");
        webParagraphRepository.save(javaSparkContext, webParagraphs);
        LOGGER.info("Saving subjects..");
        webDocumentSubjectRepository.save(javaSparkContext, webDocumentSubjects);
        LOGGER.info("Saving words..");
        webWordRepository.save(javaSparkContext, webWords);
        LOGGER.info("Word indexing job finished");

        webDocuments.unpersist();
        webParagraphs.unpersist();
    }

    private JavaRDD<WebParagraph> extractParagraphs(JavaRDD<WebDocument> webDocuments) {
        ParagraphMapper paragraphMapper = new ParagraphMapper();
        JavaRDD<WebParagraph> webParagraphs = webDocuments.flatMap(paragraphMapper::map);

        webParagraphs.persist(MEMORY_AND_DISK());
        webParagraphs.count();

        return webParagraphs;
    }

    private JavaRDD<WebDocumentSubject> extractSubjects(JavaRDD<WebDocument> webDocuments) {
        SubjectMapper subjectMapper = new SubjectMapper();

        return webDocuments.map(subjectMapper::map);
    }

    private JavaRDD<WebWord> extractWords(JavaRDD<WebParagraph> webParagraphs) {
        WordMapper wordMapper = new WordMapper();
        WordReducer wordReducer = new WordReducer();

        return webParagraphs.flatMap(wordMapper::map)
                .mapToPair(x -> new Tuple2<>(x.getWord(), x))
                .aggregateByKey(new WebWord(), wordReducer::seq, wordReducer::comb)
                .map(x -> x._2);
    }

    private JavaRDD<WebWord> rankWords(JavaRDD<WebWord> webWords, int webDocumentsCount) {
        WordRaking wordRaking = new WordRaking();

        return webWords.map(x -> wordRaking.computeWordRanking(x, webDocumentsCount));
    }
}
