package com.dt.spark.cores;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

/**
 * Created by dell-pc on 2016/3/15.
 */
public class LengthCount {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("Length Count").setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);

        JavaRDD<String> lines=sc.textFile("E://TDDOWNLOAD//BigDataSpark//spark-1.6.0-bin-hadoop2.6//README.md",1);
        JavaRDD<Integer> lengths=lines.map(new Function<String, Integer>() {
            @Override
            public Integer call(String line) throws Exception {
                return line.length();
            }
        });

        int lengthsTotal=lengths.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        });

        lines.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        System.out.println("LengthCount.main "+ lengthsTotal);

        sc.close();
    }
}
