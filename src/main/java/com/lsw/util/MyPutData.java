package com.lsw.util;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class MyPutData extends Thread {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		System.out.println();
		long start=System.currentTimeMillis();
		Configuration config=HBaseConfiguration.create();
				
				String master_ip="172.21.3.4";
				//String master_ip=args[0];
				String zk_ip="172.21.3.1";
				//String table_name="zrk1";
				//TableName tablename = TableName.valueOf(table_name);
				config.set("hbase.zookeeper.property.clientPort", "2181"); 
				config.set("hbase.zookeeper.quorum", zk_ip); 
				config.set("hbase.master", master_ip+":16030");
				String filePath="/home/hadoop/data/data-0826.txt";
				try {
					//share_aggregate_2015-07-15
					HTable table1=new HTable(config,"share_aggregate_2015-07-15");
					long d=1455550998606l;
					/*for(int i=0;i<=20;i++){
						System.out.println(222);
						byte[]	rowkey=String.valueOf(i).getBytes();*/
				 	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
				 	
					 FileInputStream fis = new FileInputStream(filePath);
                     InputStreamReader is = new InputStreamReader(fis);
                     BufferedReader br = new BufferedReader(is);
                   
                     String str;
                     try {
                      while((str=br.readLine())!=null){
                       System.out.println(str);
                
                       String strArr[]=str.split("\001");
                       long startTime=Long.parseLong(strArr[10]);
                       long endTime=Long.parseLong(strArr[11]);
                     
                       long startTimeNew=Long.parseLong(strArr[10])+49L*24L*3600L*1000L;
                       long endTimeNew=Long.parseLong(strArr[11])+49L*24L*3600L*1000L;
                       String  strNew=str.replaceAll(startTime+"", startTimeNew+"");
                       strNew=strNew.replaceAll(endTime+"", endTimeNew+"");
                  String date=sdf.format(new Date(endTimeNew));
                       Put p1=new Put(Bytes.toBytes(date+"\001"+strArr[13]+"\001"+strArr[14]));
						p1.add("cf".getBytes(),"up".getBytes(),strArr[18].getBytes());
						p1.add("cf".getBytes(),"dn".getBytes(),strArr[19].getBytes());
						//p1.add("cf".getBytes(),"up".getBytes(),String.valueOf(i).getBytes());
						
						table1.put(p1);
                      }
                     } catch (IOException e) {
                      e.printStackTrace();
                     }
                    
                     if(br!=null){
                     	br.close();
                     }
                     
						
						
				//	}
					table1.close();
					long end=System.currentTimeMillis();
					System.out.println("111111111111111111111111111");
					System.out.println((end-start)/1000/60);
					
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			
		/*Configuration config=HBaseConfiguration.create();
		
		String master_ip="192.168.164.132";
		//String master_ip=args[0];
		String zk_ip="192.168.164.132";
		//String table_name="zrk1";
		//TableName tablename = TableName.valueOf(table_name);
		config.set("hbase.zookeeper.property.clientPort", "2181"); 
		config.set("hbase.zookeeper.quorum", zk_ip); 
		config.set("hbase.master", master_ip+":10060");
		try {
			HTable table1=new HTable(config,"zrk1");
			long d=1455550998606l;
			for(int i=0;i<=99999;i++){
				System.out.println(222);
				byte[]	rowkey=String.valueOf(i).getBytes();
				Put p1=new Put(rowkey,(d+(long)i));
				p1.add("cf".getBytes(),"upsum".getBytes(),String.valueOf(0).getBytes());
				p1.add("cf".getBytes(),"down".getBytes(),String.valueOf(0).getBytes());
				p1.add("cf".getBytes(),"up".getBytes(),String.valueOf(i).getBytes());
				
				table1.put(p1);
				
			}
			table1.close();
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
	
		
	}


