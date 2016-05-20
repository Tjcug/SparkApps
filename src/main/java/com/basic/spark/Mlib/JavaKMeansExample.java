package com.basic.spark.Mlib;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

/**
 * Created by dell-pc on 2016/5/20.
 */
public class JavaKMeansExample {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("JavaKMeansExample");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        // $example on$
        // Load and parse data
        String path = "data/mllib/kmeans_data.txt";
        JavaRDD<String> data = jsc.textFile(path);
        JavaRDD<Vector> parsedData = data.map(
                new Function<String, Vector>() {
                    public Vector call(String s) {
                        String[] sarray = s.split(" ");
                        double[] values = new double[sarray.length];
                        for (int i = 0; i < sarray.length; i++)
                            values[i] = Double.parseDouble(sarray[i]);
                        return Vectors.dense(values);
                    }
                }
        );
        parsedData.cache();

        // Cluster the data into two classes using KMeans
        int numClusters = 2;
        int numIterations = 20;
        KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations);

        // Evaluate clustering by computing Within Set Sum of Squared Errors
        double WSSSE = clusters.computeCost(parsedData.rdd());
        System.out.println("Within Set Sum of Squared Errors = " + WSSSE);

        // Save and load model
        clusters.save(jsc.sc(), "target/org/apache/spark/JavaKMeansExample/KMeansModel");
        KMeansModel sameModel = KMeansModel.load(jsc.sc(),
                "target/org/apache/spark/JavaKMeansExample/KMeansModel");
        // $example off$

        jsc.stop();
    }
}
