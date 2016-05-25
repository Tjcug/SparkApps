package com.basic.spark.Mlib.OilSeaField;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.rdd.RDD;

/**
 * Created by dell-pc on 2016/5/20.
 */
public class JavaKMeansExample {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("JavaKMeansExample").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        // $example on$
        // Load and parse data
        String path = "data/mllib/kmeans_data.txt";
        JavaRDD<String> data = jsc.textFile(path);
        final JavaRDD<Vector> parsedData = data.map(
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

        final RDD pareseDataRDD=parsedData.rdd();
        // Cluster the data into two classes using KMeans
        int numClusters = 4;
        int numIterations = 20;
        final KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations);

        double cost = clusters.computeCost(parsedData.rdd());
        System.out.println("Cost: " + cost);

        // Evaluate clustering by computing Within Set Sum of Squared Errors
        double WSSSE = clusters.computeCost(parsedData.rdd());
        System.out.println("Within Set Sum of Squared Errors = " + WSSSE);

        System.out.println("Cluster centers:");
        for (Vector center: clusters.clusterCenters()) {
            System.out.println(" " + center);
        }

        JavaRDD<String> ss=parsedData.map(new Function<Vector, String>() {
            @Override
            public String call(Vector vector) throws Exception {
                return vector.toString()+"belong to cluster " +clusters.predict(vector);
            }
        });

        ss.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        System.out.println("(2.1,2.5,3.7) belong to cluster" +clusters.predict(Vectors.dense(new double[]{2.1,2.5,3.7})));

        for(int i=1;i<9;i++){
            KMeansModel meansModel=KMeans.train(pareseDataRDD,i,20,1);
            double coast=meansModel.computeCost(pareseDataRDD);
            System.out.println("sum of squared distances of points to their nearest center when k=" + i + " -> "+ coast);
        }
//
        // Save and load model
        //clusters.save(jsc.sc(), "target/org/apache/spark/JavaKMeansExample/KMeansModel");
        //KMeansModel sameModel = KMeansModel.load(jsc.sc(),
                //"target/org/apache/spark/JavaKMeansExample/KMeansModel");
        // $example off$

        jsc.stop();
    }
}
