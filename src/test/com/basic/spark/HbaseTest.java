package com.basic.spark;

import com.basic.spark.util.HBaseUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.TreeSet;

/**
 * Created by xuzhanya on 16/5/21.
 */
public class HbaseTest {

    @Test
    public void Test() throws IOException {
        List<Result> resutList= HBaseUtils.getAllRecordByQualifier("DK00_NATIONEXP","info",new String[]{"NF"});
        TreeSet<String> set= new TreeSet<>();
        for(Result result :resutList){
            for(Cell cell:result.rawCells())
                set.add(new String(CellUtil.cloneValue(cell)));
        }
        for(Object str :set.toArray())
            System.out.println((String)str);
        System.out.println("--------------------------------------");
    }
}
