package com.aeloaiei.dissertation.spark.job;

import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;

public interface SparkJob extends Serializable {
    void run(JavaSparkContext javaSparkContext);
}
