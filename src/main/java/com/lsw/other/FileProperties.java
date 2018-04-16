package com.lsw.other;

import java.util.Properties;

public class FileProperties {

	private String masterIp;
	private String zookeeperIp;
	private String masterPort;
	private String zookeeperPort;
	public String getMasterPort() {
		return masterPort;
	}
	public void setMasterPort(String masterPort) {
		this.masterPort = masterPort;
	}
	public String getZookeeperPort() {
		return zookeeperPort;
	}
	public void setZookeeperPort(String zookeeperPort) {
		this.zookeeperPort = zookeeperPort;
	}
	public String getMasterIp() {
		return masterIp;
	}
	public FileProperties() {
		super();
		// TODO Auto-generated constructor stub
	}
	public FileProperties(String masterIp, String zookeeperIp) {
		super();
		this.masterIp = masterIp;
		this.zookeeperIp = zookeeperIp;
		
	}
	public void setMasterIp(String masterIp) {
		this.masterIp = masterIp;
	}
	public String getZookeeperIp() {
		return zookeeperIp;
	}
	public void setZookeeperIp(String zookeeperIp) {
		this.zookeeperIp = zookeeperIp;
	}
	
	
}
