package com.basic.spark.dao;

import com.basic.spark.entity.OilfieldGeologicCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

/**
 	* A data access object (DAO) providing persistence and search support for OilfieldGeologicCluster entities.
 			* Transaction control of the save(), update() and delete() operations 
		can directly support Spring container-managed transactions or they can be augmented	to handle user-managed Spring transactions. 
		Each of these methods provides additional information for how to configure it for the desired type of transaction control.
 */
@Repository
public class OilfieldGeologicClusterDAO extends BaseHibernateDAOImpl<OilfieldGeologicCluster> {
	     private static final Logger log = LoggerFactory.getLogger(OilfieldGeologicClusterDAO.class);

}
