package com.basic.spark;

import com.basic.spark.util.HBaseUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;

/**
 * Created by xuzhanya on 16/5/23.
 */
public class DataInput {
    private static Configuration conf;

    public static JavaPairRDD<ImmutableBytesWritable, Result> getOilSeaFieldData(JavaSparkContext jsc, String NF) throws IOException {
        conf = HBaseUtils.getConf();
        String tableName = "OILFIELDINFO";
        conf.set(TableInputFormat.INPUT_TABLE, tableName);

        //数据从Hbase中导入
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("info"));
        scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("YQTMC"));      //油气田名称
        scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("YQTBM"));      //油气田编码
        scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("PDMC"));      //油气田盆地名称
        scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("SSPDBM"));      //所属盆地编码
        scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("MJ"));      //油气田面积
        scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("YQCLX"));      //油气藏类型
        scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("SQMC"));      //省区名称
        scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("TMCD"));      //探明程度
        scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("CJYHD"));      //沉积岩厚度
        SingleColumnValueFilter filter = new SingleColumnValueFilter(
                "info".getBytes(),
                "NF".getBytes(),
                CompareFilter.CompareOp.EQUAL,
                new BinaryComparator(NF.getBytes())
        );
        scan.setFilter(filter);

        ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
        String ScanToString = Base64.encodeBytes(proto.toByteArray());
        conf.set(TableInputFormat.SCAN, ScanToString);
        JavaPairRDD<ImmutableBytesWritable, Result> myRDD = jsc.newAPIHadoopRDD(conf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
        System.out.println(myRDD.count());
        return myRDD;
    }
}
