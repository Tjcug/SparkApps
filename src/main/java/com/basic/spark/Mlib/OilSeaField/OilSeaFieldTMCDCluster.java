package com.basic.spark.Mlib.OilSeaField;

import com.basic.spark.DataInput;
import com.basic.spark.dao.OilfieldTmcdClusterDAO;
import com.basic.spark.entity.OilfieldTmcdCluster;
import com.basic.spark.util.ClusterUtils;
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

//第1个参数是需要聚类的年份
/**
 *  聚类石油田 按照油气田探明程度进行聚类
 */

/**
 * Created by xuzhanya on 16/5/23.
 */
public class OilSeaFieldTMCDCluster {
    private static final Logger log = LoggerFactory.getLogger(OilSeaFieldTMCDCluster.class);
    private static String[] clusterType=new String[]{"小型探明程度油田","中等探明程度油田","偏大探明程度油田","大型探明程度油田"};
    public static void main(String[] args) throws IOException {
        final String NF=args[0];
        SparkConf conf = new SparkConf().setAppName("OilSeaFieldTMCDCluster").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        //数据导入
        JavaPairRDD<ImmutableBytesWritable, Result> myRDD = DataInput.getOilSeaFieldData(jsc,args[0]);

        final JavaRDD<Vector> parsedData=myRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, Vector>() {
            @Override
            public Vector call(Tuple2<ImmutableBytesWritable, Result> resultTuple2) throws Exception {
                double[] values = new double[1];
                String Areavalue="";
                Result result=resultTuple2._2;
                Cell AreaCell=result.getColumnLatestCell("info".getBytes(),"TMCD".getBytes());
                Areavalue=new String(CellUtil.cloneValue(AreaCell));

                //打印所有列族信息
                for(Cell cell : result.rawCells()){
                    log.info("列簇为：" + new String(CellUtil.cloneFamily(cell)));
                    log.info("列修饰符为：" + new String(CellUtil.cloneQualifier(cell)));
                    log.info("值为：" + new String(CellUtil.cloneValue(cell)));
                }

                values[0]=Double.parseDouble(Areavalue);
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
                double TMCD= Double.valueOf(HBaseUtils.getColumnValueByResult(r,"info","TMCD"));
                Vector vector=Vectors.dense(new double[]{TMCD});

                int cluster_id=KmeansModel.predict(vector);
                String cluster_type= ClusterUtils.getClustrType(clusterType,KmeansModel.clusterCenters(),vector);
                log.info("YQTBM "+"油气田名称:"+YQTMC+" 油气田探明程度: "+TMCD+" belong to "+cluster_id+" "+cluster_type);

                //将数据存储到Mysql数据库中
                OilfieldTmcdClusterDAO tmcdClusterDAO =new OilfieldTmcdClusterDAO();
                Session session=tmcdClusterDAO.getSession();
                session.beginTransaction();
                OilfieldTmcdCluster tmcdCluster=new OilfieldTmcdCluster();
                tmcdCluster.setClusterCost(coast);
                tmcdCluster.setClusterId(cluster_id);
                tmcdCluster.setPdmc(PDMC);
                tmcdCluster.setTmcd(TMCD);
                tmcdCluster.setNf(NF);
                tmcdCluster.setYqclx(YQCLX);
                tmcdCluster.setYqtbm(YQTBM);
                tmcdCluster.setYqtmc(YQTMC);
                tmcdCluster.setClusterType(cluster_type);
                session.save(tmcdCluster);
                session.getTransaction().commit();
                session.close();
            }
        });
        jsc.stop();
    }
}
