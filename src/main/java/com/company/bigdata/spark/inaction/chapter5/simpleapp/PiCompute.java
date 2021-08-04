package com.company.bigdata.spark.inaction.chapter5.simpleapp;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class PiCompute implements Serializable {

    private static long counter = 0;

    private void start(int slices) {

        int numberOfThrows = 100000 * slices;
        System.out.println("About to throw " + numberOfThrows
                + " darts, ready? Stay away from the target!");
        long t0 = System.currentTimeMillis();
        SparkSession spark = SparkSession
                .builder()
                .appName("Spark Pi")
                .master("local[2]")
                .getOrCreate();
        Logger.getLogger("org").setLevel(Level.ERROR);
        long t1 = System.currentTimeMillis();
        System.out.println("Session initialized in " + (t1 - t0) + " ms");

        List<Integer> listOfThrows = new ArrayList<>(numberOfThrows);
        for (int i = 0; i < numberOfThrows; i++) {
            listOfThrows.add(i);
        }
        Dataset<Row> incrementalDf = spark.createDataset(listOfThrows, Encoders.INT()).toDF();
        long t2 = System.currentTimeMillis();
        System.out.println("Initial dataframe built in " + (t2 - t1) + " ms");
        incrementalDf.show(5,false);

        Dataset<Integer> dartsDs = incrementalDf.map(new DartMapper(), Encoders.INT());
        dartsDs.show(5,false);

        long t3 = System.currentTimeMillis();
        System.out.println("Throwing darts done in " + (t3 - t2) + " ms");

        int dartsInCircle = dartsDs.reduce(new DartReducer());
        long t4 = System.currentTimeMillis();
        System.out.println("Analyzing result in " + (t4 - t3) + " ms");

        System.out.println("Pi is roughly " + 4.0 * dartsInCircle / numberOfThrows);

        spark.stop();

    }

    public static void main(String[] args) {
        PiCompute app = new PiCompute();
        app.start(10);
    }
    private final class DartMapper implements MapFunction<Row, Integer> {
        private static final long serialVersionUID = 38446L;
        @Override
        public Integer call(Row r) throws Exception {
            double x = Math.random() * 2 - 1;
            double y = Math.random() * 2 - 1;
            counter++;
            if (counter % 100000 == 0) {
                System.out.println("" + counter + " darts thrown so far");
            }
            return (x * x + y * y <= 1) ? 1 : 0;
        }
    }
    private final class DartReducer implements ReduceFunction<Integer> {
        private static final long serialVersionUID = 12859L;
        @Override
        public Integer call(Integer x, Integer y) {
            return x + y;
        }
    }
}


