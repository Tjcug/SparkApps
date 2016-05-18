package com.dt.spark.cores;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by dell-pc on 2016/3/13.
 * 使用java的方式开发进行本地Spark的WordCount程序
 */
public class WordCount {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("Spark WordCount written by Java").setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf); //其实底层就是Scala的SparkContext
        JavaRDD<String> lines= sc.textFile("E://TDDOWNLOAD//BigDataSpark//spark-1.6.0-bin-hadoop2.6//README.md",1);

        /*
         * 对初始的JavaRDD进行Transformation级别的处理，列入map、filter等高阶函数等的编程，来进行具体的数据计算
         */

        JavaRDD<String> words= lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" "));
            }
        });

        JavaPairRDD<String,Integer>
                pairs=words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s,1);
            }
        });

        JavaPairRDD<String,Integer> WordCount=pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        }).sortByKey();

        WordCount.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> pairs) throws Exception {
                System.out.println("单词 " + pairs._1 + "  计数：" +pairs._2);
            }
        });

        sc.stop();
    }
    }
