package com.basic.spark.Mlib.OilSeaField;

import com.basic.spark.DataInput;
import com.basic.spark.util.MlibUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
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
 * Created by xuzhanya on 16/5/24.
 */
public class OilSeaFieldGeologicCluster {
    private static final Logger log = LoggerFactory.getLogger(OilSeaFieldGeologicCluster.class);
    private static String[] oilFiedlGeoHYQCX;
    private static String[] getOilFiedlGeoYX;
    private static Configuration configuration;

    public static void main(String[] args) throws IOException {

        /**
         *  读取配置文件oilSeaField.properties
         */
        try {
            PropertiesConfiguration propertiesConfiguration=new PropertiesConfiguration("oilSeaField.properties");
            propertiesConfiguration.setEncoding("gbk");
            propertiesConfiguration.load();
            oilFiedlGeoHYQCX = propertiesConfiguration.getString("oilFiedlGeoHYQCX").split(" ");
            getOilFiedlGeoYX = propertiesConfiguration.getString("oilFieldGeoYX").split(" ");
            log.info("config info oilFiedlGeoHYQCX: " + propertiesConfiguration.getString("oilFiedlGeoHYQCX"));
            log.info("config info oilFieldGeoYX: " + propertiesConfiguration.getString("oilFieldGeoYX"));
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }
        /**
         *  读取配置文件oilSeaField.properties
         */

        SparkConf conf = new SparkConf().setAppName("OilSeaFieldGeologicCluster").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        //数据导入 参数为年份进行过滤
        JavaPairRDD<ImmutableBytesWritable, Result> myRDD = DataInput.getOilSeaFieldData(jsc,args[0]);

        //如果参数是面积就按照面积进行聚类
        final JavaRDD<Vector> parsedData=myRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, Vector>() {
            @Override
            public Vector call(Tuple2<ImmutableBytesWritable, Result> resultTuple2) throws Exception {
                double[] values = new double[2];
                String HYQCXvalue="";
                String YXvalue="";
                Result result=resultTuple2._2;
                Cell HYQCXCell=result.getColumnLatestCell("info".getBytes(),"HYQCX".getBytes());
                Cell YXCell=result.getColumnLatestCell("info".getBytes(),"YX".getBytes());
                HYQCXvalue=new String(CellUtil.cloneValue(HYQCXCell));
                YXvalue=new String(CellUtil.cloneValue(YXCell));

                //打印所有列族信息
                for(Cell cell : result.rawCells()){
                    log.info("列簇为：" + new String(CellUtil.cloneFamily(cell)));
                    log.info("列修饰符为：" + new String(CellUtil.cloneQualifier(cell)));
                    log.info("值为：" + new String(CellUtil.cloneValue(cell)));
                }

                for(int i=0;i<oilFiedlGeoHYQCX.length;i++){
                    if(HYQCXvalue.equals(oilFiedlGeoHYQCX[i])){
                        values[0]=(i+1)*100.0;
                        break;
                    }
                }

                for(int i=0;i<getOilFiedlGeoYX.length;i++){
                    if(YXvalue.equals(getOilFiedlGeoYX[i])){
                        values[1]=(i+1)*100.0;
                        break;
                    }
                }
                return Vectors.dense(values);
            }
        });

        parsedData.cache();             //将数据缓存到内存中
        int numClusters = 4;            //设置距离集群的个数
        MlibUtils.KMeansModelOilField(numClusters,parsedData);
    }
}
