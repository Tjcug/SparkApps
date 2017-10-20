package com.basic.spark;

import com.basic.spark.dao.YqCompanyDAO;
import com.basic.spark.dao.YqOilFieldDAO;
import com.basic.spark.entity.YqCompany;
import com.basic.spark.entity.YqOilField;
import org.hibernate.Session;
import org.junit.Test;

import java.util.List;

/**
 * Created by xuzhanya on 16/5/27.
 */
public class HibernateTest {

    @Test
    public void addYqCompany(){
        YqCompany yqCompany=new YqCompany();
        yqCompany.setName("tanjie");
        yqCompany.setStatus(1);
        YqCompanyDAO companyDAO=new YqCompanyDAO();
        Session session=companyDAO.getSession();
        session.beginTransaction();
        session.save(yqCompany);
        session.getTransaction().commit();
        session.close();
    }

    @Test
    public void Test(){
        YqOilFieldDAO yqOilFieldDAO=new YqOilFieldDAO();
        List<YqOilField> oilFieldList=yqOilFieldDAO.findList("from YqOilField");
        for(YqOilField yqOilField:oilFieldList){
            System.out.println(yqOilField.getYqCompany().getName());
        }
    }
}
