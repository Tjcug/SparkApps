package com.basic.spark.cores;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;
import java.util.ArrayList;
import java.util.List;

/**
 * locate com.basic.spark.cores
 * Created by 79875 on 2017/10/20.
 * spark-submit --class com.basic.spark.cores.HollyWoodGraph --master  spark://root2:7077 /root/TJ/SparkApps-1.0-SNAPSHOT.jar /SparkBench/hollywood-2009.graph-txt /SparkBench/Hollwood
 */
public class HollyWoodGraph {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("HollyWoodGraph");
        JavaSparkContext sc=new JavaSparkContext(conf); //其实底层就是Scala的SparkContext
        JavaRDD<String> lines= sc.textFile(args[0]);
        JavaRDD<Tuple2<String, String>> tuple2JavaRDD = lines.flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {
            @Override
            public Iterable<Tuple2<String, String>> call(String s) throws Exception {
                List<Tuple2<String, String>> tuple2List = new ArrayList<>();
                String[] split = s.split(" ");
                for (int i=0;i<split.length;i++)
                    for (int j=0;j<i;j++) {
                            tuple2List.add(new Tuple2<String, String>(split[i], split[j]));
                    }
                return tuple2List;
            }
        });
        tuple2JavaRDD.distinct();
        tuple2JavaRDD.saveAsTextFile(args[1]);
    }
}
