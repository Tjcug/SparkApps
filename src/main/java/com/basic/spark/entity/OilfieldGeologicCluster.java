package com.basic.spark.entity;

/**
 * OilfieldGeologicCluster entity. @author MyEclipse Persistence Tools
 */

public class OilfieldGeologicCluster implements java.io.Serializable {

	// Fields

	private long id;
	private String nf;
	private Integer clusterId;
	private String clusterType;
	private double clusterCost;
	private String yqtbm;
	private String yqtmc;
	private String pdmc;
	private String yqclx;
	private String hyqcx;
	private String yx;

	// Constructors

	/** default constructor */
	public OilfieldGeologicCluster() {
	}

	/** full constructor */
	public OilfieldGeologicCluster(String nf, Integer clusterId,
			String clusterType, double clusterCost, String yqtbm, String yqtmc,
			String pdmc, String yqclx, String hyqcx, String yx) {
		this.nf = nf;
		this.clusterId = clusterId;
		this.clusterType = clusterType;
		this.clusterCost = clusterCost;
		this.yqtbm = yqtbm;
		this.yqtmc = yqtmc;
		this.pdmc = pdmc;
		this.yqclx = yqclx;
		this.hyqcx = hyqcx;
		this.yx = yx;
	}

	// Property accessors

	public long getId() {
		return this.id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String getNf() {
		return this.nf;
	}

	public void setNf(String nf) {
		this.nf = nf;
	}

	public Integer getClusterId() {
		return this.clusterId;
	}

	public void setClusterId(Integer clusterId) {
		this.clusterId = clusterId;
	}

	public String getClusterType() {
		return this.clusterType;
	}

	public void setClusterType(String clusterType) {
		this.clusterType = clusterType;
	}

	public double getClusterCost() {
		return this.clusterCost;
	}

	public void setClusterCost(double clusterCost) {
		this.clusterCost = clusterCost;
	}

	public String getYqtbm() {
		return this.yqtbm;
	}

	public void setYqtbm(String yqtbm) {
		this.yqtbm = yqtbm;
	}

	public String getYqtmc() {
		return this.yqtmc;
	}

	public void setYqtmc(String yqtmc) {
		this.yqtmc = yqtmc;
	}

	public String getPdmc() {
		return this.pdmc;
	}

	public void setPdmc(String pdmc) {
		this.pdmc = pdmc;
	}

	public String getYqclx() {
		return this.yqclx;
	}

	public void setYqclx(String yqclx) {
		this.yqclx = yqclx;
	}

	public String getHyqcx() {
		return this.hyqcx;
	}

	public void setHyqcx(String hyqcx) {
		this.hyqcx = hyqcx;
	}

	public String getYx() {
		return this.yx;
	}

	public void setYx(String yx) {
		this.yx = yx;
	}

}
