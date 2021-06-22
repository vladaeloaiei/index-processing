package com.aeloaiei.dissertation.spark.job;

import com.aeloaiei.dissertation.spark.model.WebDocument;
import com.aeloaiei.dissertation.spark.model.WebDocumentSubject;
import com.aeloaiei.dissertation.spark.model.WebIntro;
import com.aeloaiei.dissertation.spark.model.WebParagraph;
import com.aeloaiei.dissertation.spark.model.WebTitle;
import com.aeloaiei.dissertation.spark.model.WebWord;
import com.aeloaiei.dissertation.spark.processing.IntroMapper;
import com.aeloaiei.dissertation.spark.processing.ParagraphMapper;
import com.aeloaiei.dissertation.spark.processing.SubjectMapper;
import com.aeloaiei.dissertation.spark.processing.TitleMapper;
import com.aeloaiei.dissertation.spark.processing.WordMapper;
import com.aeloaiei.dissertation.spark.processing.WordRaking;
import com.aeloaiei.dissertation.spark.processing.WordReducer;
import com.aeloaiei.dissertation.spark.repository.WebDocumentRepository;
import com.aeloaiei.dissertation.spark.repository.WebDocumentSubjectRepository;
import com.aeloaiei.dissertation.spark.repository.WebIntroRepository;
import com.aeloaiei.dissertation.spark.repository.WebParagraphRepository;
import com.aeloaiei.dissertation.spark.repository.WebTitleRepository;
import com.aeloaiei.dissertation.spark.repository.WebWordRepository;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import static com.aeloaiei.dissertation.spark.config.Configuration.NUMBER_OF_SHARDS;
import static org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK;

public class DocumentProcessingJob implements SparkJob {
    private static final Logger LOGGER = LogManager.getLogger(DocumentProcessingJob.class);

    @Override
    public void run(JavaSparkContext javaSparkContext) {
        WebDocumentRepository webDocumentRepository = new WebDocumentRepository();
        WebTitleRepository webTitleRepository = new WebTitleRepository();
        WebParagraphRepository webParagraphRepository = new WebParagraphRepository();
        WebIntroRepository webIntroRepository = new WebIntroRepository();
        WebDocumentSubjectRepository webDocumentSubjectRepository = new WebDocumentSubjectRepository();
        WebWordRepository webWordRepository = new WebWordRepository();

        JavaRDD<WebDocument> webDocuments;
        JavaRDD<WebTitle> webTitles;
        JavaRDD<WebParagraph> webParagraphs;
        JavaRDD<WebIntro> webIntros;
        JavaRDD<WebDocumentSubject> webDocumentSubjects;
        JavaRDD<WebWord> webWords;
        int webDocumentsCount;

        LOGGER.info("Starting the word indexing job");

        webDocuments = webDocumentRepository.load(javaSparkContext);
        webDocuments = webDocuments.repartition(NUMBER_OF_SHARDS);
        webDocuments.persist(MEMORY_AND_DISK());
        webDocumentsCount = (int) webDocuments.count();

        webTitles = extractTitles(webDocuments);
        webParagraphs = extractParagraphs(webDocuments);
        webParagraphs.persist(MEMORY_AND_DISK());
        webParagraphs.count();
        webIntros = extractIntros(webDocuments);
        webIntros.persist(MEMORY_AND_DISK());
        webIntros.count();

        webDocumentSubjects = extractSubjects(webIntros);
        webWords = extractWords(webParagraphs);
        webWords = rankWords(webWords, webDocumentsCount);

        LOGGER.info("Saving titles..");
        webTitleRepository.save(javaSparkContext, webTitles);

        LOGGER.info("Saving paragraphs..");
        webParagraphRepository.save(javaSparkContext, webParagraphs);

        LOGGER.info("Saving intros..");
        webIntroRepository.save(javaSparkContext, webIntros);

        LOGGER.info("Saving subjects..");
        webDocumentSubjectRepository.save(javaSparkContext, webDocumentSubjects);

        LOGGER.info("Saving words..");
        webWordRepository.save(javaSparkContext, webWords);

        LOGGER.info("Word indexing job finished");

        webDocuments.unpersist();
        webParagraphs.unpersist();
        webIntros.unpersist();
    }

    private JavaRDD<WebTitle> extractTitles(JavaRDD<WebDocument> webDocuments) {
        TitleMapper titleMapper = new TitleMapper();

        return webDocuments.map(titleMapper::map);
    }

    private JavaRDD<WebParagraph> extractParagraphs(JavaRDD<WebDocument> webDocuments) {
        ParagraphMapper paragraphMapper = new ParagraphMapper();

        return webDocuments.flatMap(paragraphMapper::map);
    }

    private JavaRDD<WebIntro> extractIntros(JavaRDD<WebDocument> webDocuments) {
        IntroMapper introMapper = new IntroMapper();

        return webDocuments.map(introMapper::map);
    }

    private JavaRDD<WebDocumentSubject> extractSubjects(JavaRDD<WebIntro> webIntros) {
        SubjectMapper subjectMapper = new SubjectMapper();

        return webIntros.map(subjectMapper::map);
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
