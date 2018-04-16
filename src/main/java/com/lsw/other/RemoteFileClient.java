package com.lsw.other;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class RemoteFileClient {

	private FileProperties fp;
	//private String 
	protected String hostip;
	protected int hostPort;
	protected BufferedReader socketReader;
	protected PrintWriter socketWriter;

	public RemoteFileClient(String hostip, int hostPort) {
		super();
		this.hostip = hostip;
		this.hostPort = hostPort;
	}

	public void setUpConnection() {
		try {
			Socket client = new Socket(hostip, hostPort);
			socketReader = new BufferedReader(new InputStreamReader(
					client.getInputStream()));
			socketWriter = new PrintWriter(client.getOutputStream());

		} catch (Exception e) {

		}
	}

	public FileProperties getFileProperties(String fileNameToGet) {
		fp=new FileProperties();
		//StringBuffer fileLines = new StringBuffer();
		try {
			socketWriter.println(fileNameToGet);
			socketWriter.flush();
			String line = null;
			while ((line = socketReader.readLine()) != null){
			//	fp.setHbaseIp(line.split("=")[1]);
				String[] splitString=line.split("=");
				 if("masterIp".equalsIgnoreCase(splitString[0])){
					fp.setMasterIp(splitString[1]);
				}else if("masterPort".equalsIgnoreCase(splitString[0])){
					fp.setMasterPort(splitString[1]);
				}else if("zkIp".equalsIgnoreCase(splitString[0])){
					fp.setZookeeperIp(splitString[1]);
				}else if("zkPort".equalsIgnoreCase(splitString[0])){
					fp.setZookeeperPort(splitString[1]);
				}
				/*switch(line.split("=")){
				
				}*/
			}
				//fileLines.append(line + " ");
		} catch (IOException e) {
			System.out.println("Error reading from file: " + fileNameToGet);
		}
		return fp;
	}

	public void tearDownConnection() {
		try {
			socketWriter.close();
			socketReader.close();
		} catch (IOException e) {
			System.out.println("Error tearing down socket connection: " + e);
		}
	}
	public static FileProperties getProperties(){
		RemoteFileClient remoteFileClient = new RemoteFileClient("192.168.202.11", 3000);
		remoteFileClient.setUpConnection();
		FileProperties fileContents =remoteFileClient.getFileProperties("/home/hadoop/2015/li/conf/HBaseConnect");
		return fileContents;
	}
	public static void main(String[] args) {
		FileProperties fp=getProperties();
		System.out.println(fp.getZookeeperIp());
		//remoteFileClient.tearDownConnection();
	//	System.out.println(fileContents);
	}


	
}
