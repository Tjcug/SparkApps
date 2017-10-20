package com.basic.spark.util;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by xuzhanya on 16/5/23.
 */
public class MlibUtils {

    private static final Logger log = LoggerFactory.getLogger(MlibUtils.class);

    private static KMeansModel kMeansModel;
    private static int numIterations = 20;         //设置算法迭代次数

    public static void KMeansModelOilField(int numClusters, JavaRDD<Vector> parsedData){
        /**
         * 训练KMeansModel模型
         */
        kMeansModel = KMeans.train(parsedData.rdd(), numClusters, numIterations);
        JavaRDD<String> ss=parsedData.map(new Function<Vector, String>() {
            @Override
            public String call(Vector vector) throws Exception {
                int val=kMeansModel.predict(vector);
                return vector.toString()+"belong to cluster " +val;
            }
        });
        ss.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                log.info(s);
            }
        });
    }

    public static KMeansModel getkMeansModel() {
        return kMeansModel;
    }

    public static void setkMeansModel(KMeansModel kMeansModel) {
        MlibUtils.kMeansModel = kMeansModel;
    }

    public static int getNumIterations() {
        return numIterations;
    }

    public static void setNumIterations(int numIterations) {
        MlibUtils.numIterations = numIterations;
    }
}
