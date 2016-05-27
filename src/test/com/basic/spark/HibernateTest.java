package com.basic.spark;

import com.basic.spark.dao.YqOilFieldDAO;
import com.basic.spark.entity.YqOilField;
import org.junit.Test;

import java.util.List;

/**
 * Created by xuzhanya on 16/5/27.
 */
public class HibernateTest {
    @Test
    public void Test(){
        YqOilFieldDAO yqOilFieldDAO=new YqOilFieldDAO();
        List<YqOilField> oilFieldList=yqOilFieldDAO.findList("from YqOilField");
        for(YqOilField yqOilField:oilFieldList){
            System.out.println(yqOilField.getYqCompany().getName());
        }
    }
}
