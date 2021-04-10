package com.aeloaiei.dissertation.spark.job;

import com.aeloaiei.dissertation.spark.model.WebUrl;
import com.aeloaiei.dissertation.spark.model.WebUrlRank;
import com.aeloaiei.dissertation.spark.repository.WebUrlRankRepository;
import com.aeloaiei.dissertation.spark.repository.WebUrlRepository;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Iterator;
import java.util.Optional;
import java.util.Set;

import static org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK;

public class PageRankingJob implements SparkJob {
    private static final Logger LOGGER = LogManager.getLogger(DocumentProcessingJob.class);

    private static final int ITERATIONS = 1_000;
    private static final float EPSILON = 1F / 1_000; // 1 per-mil delta between iterations
    private static final float DAMPING_FACTOR = 0.85F;

    @Override
    public void run(JavaSparkContext javaSparkContext) {
        WebUrlRepository webUrlRepository = new WebUrlRepository();
        WebUrlRankRepository webUrlRankRepository = new WebUrlRankRepository();

        LOGGER.info("Starting page ranking job");
        computePageRanks(javaSparkContext, webUrlRepository, webUrlRankRepository);
        LOGGER.info("Page ranking job finished");
    }

    private void computePageRanks(JavaSparkContext javaSparkContext, WebUrlRepository webUrlRepository, WebUrlRankRepository webUrlRankRepository) {
        JavaPairRDD<String, Set<String>> links;
        JavaPairRDD<String, Float> ranks;
        Optional<JavaPairRDD<String, Float>> rootRanks; //The root links will not be present in new ranks RDD after an iteration
        JavaPairRDD<String, Float> ranksFractions;
        JavaPairRDD<String, Float> newRanks;
        int totalLinksCount;

        links = loadLinks(javaSparkContext, webUrlRepository);
        links.persist(MEMORY_AND_DISK());
        totalLinksCount = (int) links.count();
        ranks = computeInitialRanks(links, totalLinksCount);
        ranks.persist(MEMORY_AND_DISK());
        rootRanks = Optional.empty();

        for (int i = 0; i < ITERATIONS; ++i) {
            LOGGER.info("Iteration: " + i + " out of " + ITERATIONS + " iterations");

            ranksFractions = computeRanksFractions(links, ranks);
            newRanks = computeNewRanks(ranksFractions, totalLinksCount);
            newRanks.persist(MEMORY_AND_DISK());

            // Add the root links with their initial rank
            if (!rootRanks.isPresent()) {
                rootRanks = Optional.of(extractRootRanks(links, newRanks, totalLinksCount).persist(MEMORY_AND_DISK()));
            }

            // If root links exists, add append their initial rank to the "new rank"
            if (!rootRanks.get().isEmpty()) {
                newRanks = newRanks.union(rootRanks.get());
            }

            // Finish if the difference between old rank and new rank is less than epsilon
            if (!isDeltaMoreThanEpsilon(ranks, newRanks, totalLinksCount)) {
                break;
            }

            // Remove old ranks from memory
            ranks.unpersist();
            ranks = newRanks;
        }

        LOGGER.info("Processing finished");
        LOGGER.info("Saving page ranks..");
        webUrlRankRepository.save(javaSparkContext, ranks.map(rank -> new WebUrlRank(rank._1, rank._2)));
        links.unpersist();
        ranks.unpersist();
        rootRanks.ifPresent(JavaPairRDD::unpersist);
    }

    private JavaPairRDD<String, Set<String>> loadLinks(JavaSparkContext javaSparkContext, WebUrlRepository webUrlRepository) {
        return webUrlRepository.load(javaSparkContext).mapToPair(this::webUrlToTuple2);
    }

    private JavaPairRDD<String, Float> computeInitialRanks(JavaPairRDD<String, Set<String>> links, int totalLinksCount) {
        return links.mapToPair(link -> computeInitialRanks(link._1, totalLinksCount));
    }

    private Tuple2<String, Set<String>> webUrlToTuple2(WebUrl webUrl) {
        return new Tuple2<>(webUrl.getLocation(), webUrl.getLinksReferred());
    }

    private Tuple2<String, Float> computeInitialRanks(String link, int numberOfLinks) {
        return new Tuple2<>(link, 1F / numberOfLinks);
    }

    private JavaPairRDD<String, Float> computeRanksFractions(JavaPairRDD<String, Set<String>> links, JavaPairRDD<String, Float> ranks) {
        return links.join(ranks)
                .values()
                .flatMapToPair(this::shareRankPortionWithChildren);
    }

    private Iterator<Tuple2<String, Float>> shareRankPortionWithChildren(Tuple2<Set<String>, Float> currentLink) {
        //share an equal proportion of your probability with your children
        //more exactly: probability/numberOfChildren
        float rank = currentLink._2;
        int numberOfChildren = currentLink._1.size();
        return currentLink._1.stream()
                .map(child -> shareWithChild(rank, child, numberOfChildren))
                .iterator();
    }

    private Tuple2<String, Float> shareWithChild(float parentRank, String child, int numberOfChildren) {
        float childRank = parentRank / numberOfChildren;

        return new Tuple2<>(child, childRank);
    }

    private JavaPairRDD<String, Float> computeNewRanks(JavaPairRDD<String, Float> ranksFractions, int totalLinksCount) {
        return ranksFractions.reduceByKey(Float::sum)
                .mapValues(totalIntermediate -> applyDampingFactor(totalIntermediate, totalLinksCount));
    }

    private float applyDampingFactor(float probability, int numberOfLinks) {
        return (1.0F - DAMPING_FACTOR) / numberOfLinks + DAMPING_FACTOR * probability;
    }

    private JavaPairRDD<String, Float> extractRootRanks(JavaPairRDD<String, Set<String>> links, JavaPairRDD<String, Float> ranks, int totalLinksCount) {
        return links.keys()
                .subtract(ranks.keys())
                .mapToPair(link -> computeInitialRanks(link, totalLinksCount));
    }

    private boolean isDeltaMoreThanEpsilon(JavaPairRDD<String, Float> oldRank, JavaPairRDD<String, Float> newRank, int totalLinksCount) {
        return !oldRank.join(newRank)
                .values()
                .map(rank -> rank._1 - rank._2)
                .filter(delta -> delta > (EPSILON / totalLinksCount))
                .isEmpty();
    }
}
