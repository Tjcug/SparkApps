package com.basic.spark.cores;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by xuzhanya on 16/5/19.
 */
public class SparkPi{

    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("Spark01").setMaster("spark://Master:7077")
                .setJars(new String[]{"D:\\IntelliJ IDEA 15.0.2\\BigDataProject\\SparkApps\\out\\artifacts\\SparkApps_jar\\SparkApps.jar"});
        //.setJars(new String[]{"/Users/xuzhanya/Desktop/油气大数据/BigDataProject/SparkApps/out/artifacts/SparkApps_jar/SparkApps.jar"});
        JavaSparkContext sc=new JavaSparkContext(conf);
        int silices=2;
        int n=100*silices;
        List list=new ArrayList<Integer>();
        for(int i=0;i<n;i++){
            list.add(i);
        }
        System.out.println(list.toString());

        JavaRDD<Integer> javardd=sc.parallelize(list,silices);
        JavaPairRDD<Integer,String> javarddPar=javardd.mapToPair(new PairFunction<Integer, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Integer integer) throws Exception {
                return new Tuple2<Integer, String>(integer,"hello");
            }
        });
        javarddPar.foreach(new VoidFunction<Tuple2<Integer, String>>() {
            @Override
            public void call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                System.out.println(integerStringTuple2._1+" "+integerStringTuple2._2);
            }
        });
        javardd.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });

//        val slices = if (args.length > 0) args(0).toInt else 2
//        val n = 100000 * slices
//        val count = spark.parallelize(1 to n, slices).map { i =>
//            val x = random * 2 - 1
//            val y = random * 2 - 1
//            if (x * x + y * y < 1) 1 else 0
//        }.reduce(_ + _)
//        println("Pi is roughly " + 4.0 * count / n)
//        spark.stop()
//

        System.out.println(n);
    }
}
