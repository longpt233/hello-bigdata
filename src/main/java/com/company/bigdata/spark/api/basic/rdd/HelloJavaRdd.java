package com.company.bigdata.spark.api.basic.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class HelloJavaRdd {

    private static final String localMaster = "local[4]";

    public HelloJavaRdd(){

    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster(localMaster)
                .setAppName("HelloJavaRdd");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("file:/home/long/IdeaProjects/hello-bigdata/data/wc.txt").cache();

        System.out.println(lines.getNumPartitions());
//        JavaPairRDD<String, String> words = lines.flatMapToPair((PairFlatMapFunction<String, String, String>) line -> {
//            System.out.println(line);
//            String[] wordsInLine = line.split(",");
//            String[] names = wordsInLine[2].split(" ");
//            List<Tuple2<String, String>> results = new ArrayList<>();
//            for (String word : wordsInLine) {
//                results.add(new Tuple2<>(names[0], word));
//            }
//            return results.iterator();
//        });
//
//        words.groupByKey().foreach(t -> {
//            System.out.println(t._1() + ":");
//            t._2().forEach(w -> {
//                System.out.print("." + w + ",");
//            });
//            System.out.println();
//        });
    }
}
