package com.basic.spark.Mlib.OilSeaField;

import com.basic.spark.DataInput;
import com.basic.spark.util.MlibUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;

/**
 * 对油气田综合含水率进行聚类分析
 */

/**
 * Created by xuzhanya on 16/5/25.
 */
public class OilSeaFieldZHHSLCluster {
    private static final Logger log = LoggerFactory.getLogger(OilSeaFieldZHHSLCluster.class);

    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf().setAppName("OilSeaFieldCZHHSLCluster").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        //数据导入
        JavaPairRDD<ImmutableBytesWritable, Result> myRDD = DataInput.getOilSeaFieldData(jsc,args[0]);

        //如果参数是面积就按照面积进行聚类
        final JavaRDD<Vector> parsedData=myRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, Vector>() {
            @Override
            public Vector call(Tuple2<ImmutableBytesWritable, Result> resultTuple2) throws Exception {
                double[] values = new double[1];
                String CZHHSLvalue="";
                Result result=resultTuple2._2;
                Cell CZHHSLCell=result.getColumnLatestCell("info".getBytes(),"CZHHSL".getBytes());
                CZHHSLvalue=new String(CellUtil.cloneValue(CZHHSLCell));

                //打印所有列族信息
                for(Cell cell : result.rawCells()){
                    log.info("列簇为：" + new String(CellUtil.cloneFamily(cell)));
                    log.info("列修饰符为：" + new String(CellUtil.cloneQualifier(cell)));
                    log.info("值为：" + new String(CellUtil.cloneValue(cell)));
                }

                values[0]=Double.parseDouble(CZHHSLvalue);
                return Vectors.dense(values);
            }
        });

        parsedData.cache();             //将数据缓存到内存中
        int numClusters = 4;            //设置距离集群的个数
        MlibUtils.KMeansModelOilField(numClusters,parsedData);
    }
}
