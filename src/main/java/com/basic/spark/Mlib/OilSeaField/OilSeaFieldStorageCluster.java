package com.basic.spark.Mlib.OilSeaField;

import com.basic.spark.DataInput;
import com.basic.spark.dao.OilfieldStorageClusterDAO;
import com.basic.spark.entity.OilfieldStorageCluster;
import com.basic.spark.util.HBaseUtils;
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

//对石油储量进行聚类
/**
 * Created by xuzhanya on 16/5/24.
 */
public class OilSeaFieldStorageCluster {
    private static final Logger log = LoggerFactory.getLogger(OilSeaFieldStorageCluster.class);
    private static String[] clusterType=new String[]{"石油储量油田1","石油储量油田2","石油储量油田3","石油储量油田4"};
    public static void main(String[] args) throws IOException {

        final String NF=args[0];
        SparkConf conf = new SparkConf().setAppName("OilSeaFieldStorageCluster").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        //数据导入
        JavaPairRDD<ImmutableBytesWritable, Result> myRDD = DataInput.getOilSeaFieldData(jsc,args[0]);

        //如果参数是面积就按照面积进行聚类
        final JavaRDD<Vector> parsedData=myRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, Vector>() {
            @Override
            public Vector call(Tuple2<ImmutableBytesWritable, Result> resultTuple2) throws Exception {
                double[] values = new double[3];
                String CCBvalue="";
                Result result=resultTuple2._2;
                Cell CCBCell=result.getColumnLatestCell("info".getBytes(),"CCB".getBytes());
                CCBvalue=new String(CellUtil.cloneValue(CCBCell));

                String LJTMDZCLZLvalue="";
                Cell LJTMDZCLZLCell=result.getColumnLatestCell("info".getBytes(),"LJTMDZCLZL".getBytes());
                LJTMDZCLZLvalue=new String(CellUtil.cloneValue(LJTMDZCLZLCell));

                String LJTMDZCLTJvalue="";
                Cell LJTMDZCLTJCell=result.getColumnLatestCell("info".getBytes(),"LJTMDZCLTJ".getBytes());
                LJTMDZCLTJvalue=new String(CellUtil.cloneValue(LJTMDZCLTJCell));

                //打印所有列族信息
                for(Cell cell : result.rawCells()){
                    log.info("列簇为：" + new String(CellUtil.cloneFamily(cell)));
                    log.info("列修饰符为：" + new String(CellUtil.cloneQualifier(cell)));
                    log.info("值为：" + new String(CellUtil.cloneValue(cell)));
                }

                values[0]=Double.parseDouble(CCBvalue);
                values[1]=Double.parseDouble(LJTMDZCLZLvalue);
                values[2]=Double.parseDouble(LJTMDZCLTJvalue);
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
                Double CCB= Double.valueOf(HBaseUtils.getColumnValueByResult(r,"info","CCB"));
                Double LJTMDZCLZL= Double.valueOf(HBaseUtils.getColumnValueByResult(r,"info","LJTMDZCLZL"));
                Double LJTMDZCLTJ= Double.valueOf(HBaseUtils.getColumnValueByResult(r,"info","LJTMDZCLTJ"));

                double[] values = new double[3];
                values[0]=CCB;
                values[1]=LJTMDZCLZL;
                values[2]=LJTMDZCLTJ;
                Vector vector=Vectors.dense(values);

                int cluster_id=KmeansModel.predict(vector);
                String cluster_type=clusterType[cluster_id];
                log.info("YQTBM "+"油气田名称:"+YQTMC+" belong to "+cluster_id+" "+cluster_type);

                //将数据存储到Mysql数据库中
                OilfieldStorageClusterDAO storageClusterDAO=new OilfieldStorageClusterDAO();
                Session session=storageClusterDAO.getSession();
                session.beginTransaction();
                OilfieldStorageCluster storageCluster=new OilfieldStorageCluster();
                storageCluster.setClusterCost(coast);
                storageCluster.setClusterId(cluster_id);
                storageCluster.setPdmc(PDMC);
                storageCluster.setCcb(CCB);
                storageCluster.setLjtmdzcltj(LJTMDZCLTJ);
                storageCluster.setLjtmdzclzl(LJTMDZCLZL);
                storageCluster.setNf(NF);
                storageCluster.setYqclx(YQCLX);
                storageCluster.setYqtbm(YQTBM);
                storageCluster.setYqtmc(YQTMC);
                storageCluster.setClusterType(cluster_type);
                session.save(storageCluster);
                session.getTransaction().commit();
                session.close();
            }
        });
        jsc.stop();
    }
}
