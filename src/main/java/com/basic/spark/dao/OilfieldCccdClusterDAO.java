package com.basic.spark.dao;

import com.china.oil.application.entity.OilfieldCccdCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

/**
 	* A data access object (DAO) providing persistence and search support for OilfieldCccdCluster entities.
 			* Transaction control of the save(), update() and delete() operations 
		can directly support Spring container-managed transactions or they can be augmented	to handle user-managed Spring transactions. 
		Each of these methods provides additional information for how to configure it for the desired type of transaction control.
  * @author MyEclipse Persistence Tools 
 */
@Repository
public class OilfieldCccdClusterDAO extends BaseHibernateDAOImpl<OilfieldCccdCluster>  {
	     private static final Logger log = LoggerFactory.getLogger(OilfieldCccdClusterDAO.class);

}
