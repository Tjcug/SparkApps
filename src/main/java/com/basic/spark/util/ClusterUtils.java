package com.basic.spark.util;

import org.apache.spark.mllib.linalg.Vector;

import java.util.Arrays;

/**
 * Created by xuzhanya on 16/5/27.
 */
public class ClusterUtils {
    /**
     *  通过算法算出集该聚类属于哪个聚类类型
     * @param clusterType   默认是从小到大排列
     * @param centers
     * @return
     */
    public static String getClustrType(String[] clusterType, Vector[] centers, Vector clusters){
        double[] numcenters=new double[4];
        int result;
        for(int i=0;i<centers.length;i++){
            Vector center=centers[i];
            numcenters[i]=center.apply(0);
        }
        Arrays.sort(numcenters);
        double cluster=clusters.apply(0);
        if(cluster<numcenters[0])
            result=0;
        else if(cluster<numcenters[1])
            result=1;
        else if(cluster<numcenters[2])
            result=2;
        else result=3;
        return clusterType[result];
    }
}
