package com.basic.spark.Mlib.OilSeaField;

import com.basic.spark.DataInput;
import com.basic.spark.dao.OilfieldGeologicClusterDAO;
import com.basic.spark.entity.OilfieldGeologicCluster;
import com.basic.spark.util.HBaseUtils;
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
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.hibernate.Session;
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
    private static String[] clusterType=new String[]{"地质类型1油田","地质类型2油田","地质类型3油田","地质类型4油田"};
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
        final String NF=args[0];
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
        int numIterations = 20;         //设置算法迭代次数
        final KMeansModel KmeansModel= KMeans.train(parsedData.rdd(), numClusters, numIterations);
        final double coast=KmeansModel.computeCost(parsedData.rdd());

        double[] centers=new double[4];
        System.out.println("Cluster centers:");
        for(int i=0;i<KmeansModel.clusterCenters().length;i++){
            Vector center=KmeansModel.clusterCenters()[i];
            centers[i]= center.apply(0);
            System.out.println(" " + center);
        }

        //存储数据结果集到上层关系型数据库中
        myRDD.foreach(new VoidFunction<Tuple2<ImmutableBytesWritable, Result>>() {
            @Override
            public void call(Tuple2<ImmutableBytesWritable, Result> resultTuple2) throws Exception {
                Result r=resultTuple2._2;
                String YQTBM= HBaseUtils.getColumnValueByResult(r,"info","YQTBM");
                String YQTMC=HBaseUtils.getColumnValueByResult(r,"info","YQTMC");
                String PDMC=HBaseUtils.getColumnValueByResult(r,"info","PDMC");
                String YQCLX=HBaseUtils.getColumnValueByResult(r,"info","YQCLX");
                String HYQCXvalue=HBaseUtils.getColumnValueByResult(r,"info","HYQCX");
                String YXvalue=HBaseUtils.getColumnValueByResult(r,"info","YX");

                double[] values = new double[2];
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
                Vector vector=Vectors.dense(values);

                int cluster_id=KmeansModel.predict(vector);
                String cluster_type=clusterType[cluster_id];
                log.info("YQTBM "+"油气田名称:"+YQTMC+" belong to "+cluster_id+" "+cluster_type);

                //将数据存储到Mysql数据库中
                OilfieldGeologicClusterDAO oilfieldGeologicClusterDAO=new OilfieldGeologicClusterDAO();
                Session session=oilfieldGeologicClusterDAO.getSession();
                session.beginTransaction();
                OilfieldGeologicCluster geologicCluster=new OilfieldGeologicCluster();
                geologicCluster.setClusterCost(coast);
                geologicCluster.setClusterId(cluster_id);
                geologicCluster.setPdmc(PDMC);
                geologicCluster.setHyqcx(HYQCXvalue);
                geologicCluster.setYx(YXvalue);
                geologicCluster.setNf(NF);
                geologicCluster.setYqclx(YQCLX);
                geologicCluster.setYqtbm(YQTBM);
                geologicCluster.setYqtmc(YQTMC);
                geologicCluster.setClusterType(cluster_type);
                session.save(geologicCluster);
                session.getTransaction().commit();
                session.close();
            }
        });
        jsc.stop();
    }
}

