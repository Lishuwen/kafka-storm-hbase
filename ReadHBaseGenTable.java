package com.lsw.day0916;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TimeZone;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.lsw.other.DayList;
import com.lsw.other.FileProperties;
import com.lsw.other.RemoteFileClient;


public class ReadHBaseGenTable {

	/**
	 * @param args
	 */
	private Configuration config;
	private HTable table, tableDay;
	private HBaseAdmin admin;

	public ReadHBaseGenTable() {
		FileProperties fp=RemoteFileClient.getProperties();
		config = HBaseConfiguration.create();
		config.set("hbase.master",fp.getMasterIp()+ ":" + fp.getMasterPort());
		config.set("hbase.zookeeper.property.clientPort", fp.getZookeeperPort());
		config.set("hbase.zookeeper.quorum",  fp.getZookeeperIp());

		try {
			table = new HTable(config, Bytes.toBytes("genetraffic_5minute"));
			tableDay = new HTable(config, Bytes.toBytes("genetraffic_5minute"));
			admin = new HBaseAdmin(config);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public JSONArray genFlowTotalInfoHbase() throws IOException {
		JSONArray jsonArray = new JSONArray();
		JSONArray injsonArray = new JSONArray();
		Scan scan = new Scan();
		scan.setMaxVersions();
		scan.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("AppTraffic_UP"));
		scan.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("AppTraffic_DN"));
		scan.setTimeRange(0L, Long.MAX_VALUE);
		ResultScanner reScan = table.getScanner(scan);
		double upSum = 0.0, downSum = 0.0, upSumTB = 0.0, downSumTB = 0.0;
		for (Result rs : reScan) {
			upSum += Double.parseDouble(Bytes.toString(rs.getValue(
					Bytes.toBytes("cf"), Bytes.toBytes("AppTraffic_UP"))));
			downSum += Double.parseDouble(Bytes.toString(rs.getValue(
					Bytes.toBytes("cf"), Bytes.toBytes("AppTraffic_DN"))));

		}
		upSumTB = Double.parseDouble(String.format("%.4f", upSum
				/ (1024 * 1024)));
		downSumTB = Double.parseDouble(String.format("%.4f", downSum
				/ (1024 * 1024)));
		// upSumTB = Double.parseDouble(String.format("%.4f", upSum));
		// downSumTB = Double.parseDouble(String.format("%.4f", downSum));
		injsonArray.add(upSumTB);
		injsonArray.add(downSumTB);
		jsonArray.add(injsonArray);
		return jsonArray;

	}

	public JSONArray genFlowL2Hbase(String date, String userGroupId,
			Map<String, List<String>> appTypeMap, String appTraffic_Field,
			List<String> devNameList) throws IOException {

		Map<String, Map<Integer, Double>> aptimeMap = new HashMap<String, Map<Integer, Double>>();
		JSONArray jsonArray = new JSONArray();
		JSONArray sm_jsonArray = new JSONArray();
		// JSONObject jsonObject = new JSONObject();
		for (Map.Entry<String, List<String>> typeEntry : appTypeMap.entrySet()) {
			JSONArray sm_jsonArray2 = new JSONArray();
			for (String appName : typeEntry.getValue()) {
				Map<Integer, Double> versionMap = new HashMap<Integer, Double>();
				for (int i =0; i < 288; i++) {
					versionMap.put(i, 0.0);
				}
				double sum = 0.0;
				String sm_appName = null;
				for (String devName : devNameList) {
					// concat the rowkey with the input field
					String rowkey =  date + "\001"
							+userGroupId + "\001" + typeEntry.getKey() + "\001" + appName + "\001"
							+ devName;
					Get get = new Get(Bytes.toBytes(rowkey));
					get.setMaxVersions();
					get.addColumn(Bytes.toBytes("cf"),
							Bytes.toBytes(appTraffic_Field));
					get.setTimeRange(0L, Long.MAX_VALUE);
					Result rst = null;
					rst = table.get(get);
					SimpleDateFormat sdf = new SimpleDateFormat(
							"yyyy-MM-dd HH:mm:ss");
					sdf.setTimeZone(TimeZone.getTimeZone("GMT+8"));

					for (KeyValue kv : rst.raw()) {
						String[] arr = Bytes.toString(kv.getRow())
								.split("\001");
						sm_appName = arr[3];
						long time = kv.getTimestamp();
						String date2 = sdf.format(new Date(time));
						String arr2[] = date2.substring(11, 18).split(":");
						int hour = Integer.parseInt(arr2[0]);
						int min = Integer.parseInt(arr2[1]);
						int sec = Integer.parseInt(arr2[2]);
						int timeStampNum = (int) ((hour * 3600 + min
								* 60 + sec)
								/ (60 * 5));
						sum += Double
								.parseDouble((Bytes.toString(kv.getValue())));
						double tmpSum = versionMap.get(timeStampNum);
						versionMap.put(
								timeStampNum,
								tmpSum
										+ Double.parseDouble((Bytes.toString(kv
												.getValue()))));
					
//						System.out.print(Bytes.toString(kv.getKey()) + "  ");
//						System.out.print(timeStampNum + "  ");
					
					}
				}

				// for (Map.Entry<Integer, Double> entry :
				// versionMap.entrySet()) {
				// sm_jsonArray.clear();
				// sm_jsonArray.add(entry.getValue());
				// }
				// jsonObject.put(appType, sm_jsonArray);
				if (sm_appName != null) {
					sm_jsonArray2.add(sm_appName);
					for (int i = 0; i <= versionMap.size()-1; i++) {
						sm_jsonArray.add(versionMap.get(i));
					}
					sm_jsonArray2.add(sm_jsonArray);
					jsonArray.add(sm_jsonArray2);
					sm_jsonArray2.clear();
					sm_jsonArray.clear();
				}
			}

		}
		// jsonObject.put("appName", appType);

		return jsonArray;

	}

	public JSONArray genFlowL2_SumHbase(String date, String userGroup,
			Map<String, List<String>> appNamesMap, String appTraffic_Field,
			List<String> devNameList) throws IOException {
		JSONArray jsonArray = new JSONArray();
		JSONArray sm_jsonArray = null;
		// JSONObject jsonObject = new JSONObject();
		Result rst = null;
		JSONArray jsonArraysm = new JSONArray();
		for (Map.Entry<String, List<String>> appEntry : appNamesMap.entrySet()) {

			sm_jsonArray = new JSONArray();
			Map<Integer, Double> versionMap = new HashMap<Integer, Double>();
			for (int i = 0; i < 288; i++) {
				versionMap.put(i, 0.0);
			}
			String sm_appType = null;
			for (String appName : appEntry.getValue()) {
				for (String devName : devNameList) {
					// concat the rowkey with the input field
					String rowkey =date + "\001"
							+ userGroup + "\001" +  appEntry.getKey() + "\001" + appName + "\001"
							+ devName;
					Get get = new Get(Bytes.toBytes(rowkey));
					get.setMaxVersions();
					get.addColumn(Bytes.toBytes("cf"),
							Bytes.toBytes(appTraffic_Field));
					get.setTimeRange(0L, Long.MAX_VALUE);
					rst = table.get(get);
					SimpleDateFormat sdf = new SimpleDateFormat(
							"yyyy-MM-dd HH:mm:ss");
					sdf.setTimeZone(TimeZone.getTimeZone("GMT+8"));

					for (KeyValue kv : rst.raw()) {
						String arr2[] = Bytes.toString(kv.getRow()).split(
								"\001");
						sm_appType = arr2[2];
						long time = kv.getTimestamp();
						String date2 = sdf.format(new Date(time));
						String arr[] = date2.substring(11, 18).split(":");
						int hour = Integer.parseInt(arr[0]);
						int min = Integer.parseInt(arr[1]);
						int sec = Integer.parseInt(arr[2]);
						int timeStampNum = (int)((hour * 3600 + min
								* 60 + sec)
								/ (60 * 5));
						double tmpSum = versionMap.get(timeStampNum);
						versionMap.put(
								timeStampNum,
								tmpSum
										+ Double.parseDouble((Bytes.toString(kv
												.getValue()))));
//					
//						System.out.print(Bytes.toString(kv.getKey()) + "  ");
//						System.out.print(timeStampNum + "  ");
//						System.out
//								.println(Bytes.toString(kv.getValue()) + "  ");
//						
					}
				}

			}
			if (sm_appType != null) {
				jsonArray.add(sm_appType);
				for (int i = 0; i <= versionMap.size()-1; i++) {
					sm_jsonArray.add(versionMap.get(i));
				}

				jsonArray.add(sm_jsonArray);
				jsonArraysm.add(jsonArray);
				jsonArray.clear();
				sm_jsonArray.clear();
			}
		}

		// jsonObject.put("appName", appType);
		return jsonArraysm;
	}

	public JSONArray genFlowPieHbase(String date, String userGroupId,
			Map<String, List<String>> appTypeMap, String appTraffic_Field,
			List<String> devNameList) throws IOException {
		// public JSONArray genFlowPieHbase() throws IOException {
		JSONArray jsonArrayPie = new JSONArray();

		// System.out.println();
		for (Map.Entry<String, List<String>> appEntry : appTypeMap.entrySet()) {
			System.out.println(appEntry.getKey());
			for (String appName : appEntry.getValue()) {
				JSONArray sm_jsonArrayPie = new JSONArray();
				System.out.println(appName);
				double sum = 0.0;
				String sm_appName = null;
				for (String devName : devNameList) {
					String rowkey = date + "\001"
							+userGroupId + "\001" +  appEntry.getKey() + "\001" + appName + "\001"
							+ devName;
					System.out.println("rowkey" + rowkey);
					Get get = new Get(Bytes.toBytes(rowkey));
					get.setMaxVersions();
					get.addColumn(Bytes.toBytes("cf"),
							Bytes.toBytes(appTraffic_Field));
					get.setTimeRange(0L, Long.MAX_VALUE);
					Result rs = null;
					rs = table.get(get);
					if (rs != null) {
						System.out.println("PieHbase  :" + rs.toString());
						for (KeyValue kv : rs.raw()) {
							String[] arr2 = Bytes.toString(kv.getRow()).split(
									"\001");
							sm_appName = arr2[3];
							sum += Double.parseDouble(Bytes.toString(kv
									.getValue()));
						}
					}

					System.out.print(appName + ":" + sum + "    ");

				}
				if (sm_appName != null) {
					sm_jsonArrayPie.add(sm_appName);
					sm_jsonArrayPie.add(sum);
					jsonArrayPie.add(sm_jsonArrayPie);
				}

			}

		}
		return jsonArrayPie;
	}

	public JSONArray genFlowPie_SumHbase(String date, String userGroupId,
			Map<String, List<String>> appTypeMap, String appTraffic_Field,
			List<String> devNameList) throws IOException {
		JSONArray jsonArrayPie = new JSONArray();

		for (Map.Entry<String, List<String>> appEntry : appTypeMap.entrySet()) {
			double sum = 0.0;
			String appType = null;
			JSONArray sm_jsonArrayPie = new JSONArray();
			for (String appName : appEntry.getValue()) {
				for (String devName : devNameList) {
					String rowkey = date + "\001"
							+ userGroupId + "\001" + appEntry.getKey() + "\001" + appName + "\001"
							+ devName;
					System.out.println("rowkey:" + rowkey);
					Get get = new Get(Bytes.toBytes(rowkey));
					 get.setMaxVersions();
					get.addColumn(Bytes.toBytes("cf"),
							Bytes.toBytes(appTraffic_Field));
					// get.setTimeRange(0L, Long.MAX_VALUE);
					Result rs = null;
					rs = table.get(get);
					if (rs != null) {
						System.out.println("PieHbase  :" + rs.toString());
						for (KeyValue kv : rs.raw()) {
							String[] arr2 = Bytes.toString(kv.getRow()).split(
									"\001");
							appType = arr2[2];
							sum += Double.parseDouble(Bytes.toString(kv
									.getValue()));
						}
					}

					System.out.print(appName + ":" + sum + "    ");
				}

			}
			if (appType != null) {
				sm_jsonArrayPie.add(appType);
				sm_jsonArrayPie.add(sum);
				jsonArrayPie.add(sm_jsonArrayPie);

			}

		}
		return jsonArrayPie;
	}

	public JSONArray genFlowTop10Hbase(String date, String userGroupId,
			Map<String, List<String>> appMap, String appTraffic_Field,
			List<String> devNameList) throws IOException {
		Map<Double, ArrayList<JSONArray>> appName_numMap = new HashMap<Double, ArrayList<JSONArray>>();
		// ArrayList<JSONArray> appList = new ArrayList<JSONArray>();
		JSONArray top_jsonArray = new JSONArray();

		for (Map.Entry<String, List<String>> appEntry : appMap.entrySet()) {
			System.out.println(appEntry.getKey());
			for (String appName : appEntry.getValue()) {
				JSONArray one_jsonArray = new JSONArray();
				System.out.println(appName);
				System.out.println("&&&" + one_jsonArray);
				Map<JSONArray, Double> tmpMap = new HashMap<JSONArray, Double>();
				for (String devName : devNameList) {
					double sum = 0.0;
					// one_jsonArray = new JSONArray();
					String rowkey = date + "\001"
							+userGroupId + "\001" +  appEntry.getKey() + "\001" + appName + "\001"
							+ devName;
					System.out.println("rowkey:" + rowkey);
					Get get = new Get(Bytes.toBytes(rowkey));
					 get.setMaxVersions();
					get.addColumn(Bytes.toBytes("cf"),
							Bytes.toBytes(appTraffic_Field));
					// get.setTimeRange(0L, Long.MAX_VALUE);
					Result rst = table.get(get);
					if (rst != null) {
						one_jsonArray.clear();
						one_jsonArray.add(appEntry.getKey());
						one_jsonArray.add(appName);
						if (!tmpMap.containsKey(one_jsonArray)) {

							for (KeyValue kv : rst.raw()) {
								tmpMap.put(one_jsonArray, Double
										.parseDouble(Bytes.toString(kv
												.getValue())));
							}
						} else {
							for (KeyValue kv : rst.raw()) {
								tmpMap.put(
										one_jsonArray,
										Double.parseDouble(Bytes.toString(kv
												.getValue()))
												+ tmpMap.get(one_jsonArray));
							}
						}

					}

					System.out.print(appName + ":" + sum + "    ");
					// appList.add(one_jsonArray);
				}
				for (Map.Entry<JSONArray, Double> e : tmpMap.entrySet()) {
					ArrayList<JSONArray> tmpList = new ArrayList<JSONArray>();
					if (appName_numMap.get(e.getValue()) != null) {
						ArrayList<JSONArray> tmpList2 = appName_numMap.get(e
								.getValue());
						tmpList2.add(e.getKey());
						appName_numMap.put(e.getValue(), tmpList2);
					} else {
						tmpList.add(e.getKey());
						appName_numMap.put(e.getValue(), tmpList);
					}
				}
			}
		}
		// sort the sum of appName ,and get the top 10
		Double[] sm_value = appName_numMap.keySet().toArray(
				new Double[appName_numMap.size()]);
		Arrays.sort(sm_value);
		JSONArray tmpjArray = new JSONArray();
		System.out.println(",,,,," + sm_value.length);
		// System.out.println(",,,,," + appName_numMap.get(0.0).get(0));

		int count = 1;
		for (int i = sm_value.length - 1; i >= 0; i--) {
			if (count <= 10) {
				System.out.println("the key of the map is: " + i);
				for (JSONArray oneAppName : appName_numMap.get(sm_value[i])) {
					oneAppName.add(sm_value[i]);
					top_jsonArray.add(oneAppName);
				}
			}
			count++;
		}

		return top_jsonArray;
	}

	public static byte[] POSTFIX = new byte[] { 1 };


	/**
	 * The part of the day
	 * 
	 * 
	 * 
	 * 
	 * 
	 * */
	public JSONArray genFlowL2Hbase_day(String date1, String date2,
			String userGroupId, Map<String, List<String>> appTypeMap,
			String appTraffic_Field, List<String> devNameList)
			throws IOException {
		Map<String, Map<Integer, Double>> aptimeMap = new HashMap<String, Map<Integer, Double>>();
		JSONArray jsonArray = new JSONArray();
		JSONArray sm_jsonArray = new JSONArray();
		// JSONObject jsonObject = new JSONObject();
		DayList dy = new DayList();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		List<Date> lDate = dy.findDates(date1, date2);
		for (Map.Entry<String, List<String>> typeEntry : appTypeMap.entrySet()) {
			JSONArray sm_jsonArray2 = new JSONArray();
			for (String appName : typeEntry.getValue()) {
				Result[] rst = null;
				Map<String, Double> versionMap = new HashMap<String, Double>();
				for (int i = 0; i <= lDate.size() - 1; i++) {
					System.out.println(sdf.format(lDate.get(i)));
					versionMap.put(sdf.format(lDate.get(i)).replace('-', '/'),
							0.0);
				}
				String sm_appName = null;
				for (String devName : devNameList) {
					List<Get> getList = new ArrayList<Get>();
					for (Date day : lDate) {
						// concat the rowkey with the input field
						String rowkey = sdf.format(day).replace('-', '/')
								+ "\001" + userGroupId + "\001"
								+ typeEntry.getKey() + "\001" + appName
								+ "\001" + devName;
						
						Get get = new Get(Bytes.toBytes(rowkey));
						get.addColumn(Bytes.toBytes("cf"),
								Bytes.toBytes(appTraffic_Field+"_Sum"));
						getList.add(get);

					}
					rst = tableDay.get(getList);

					for (int i = 0; i < rst.length - 1; i++) {
						for (KeyValue kv : rst[i].raw()) {
							String[] arr = Bytes.toString(kv.getRow()).split(
									"\001");
							sm_appName = arr[3];
							versionMap.put(
									arr[0],
									versionMap.get(arr[0])
											+ Double.parseDouble((Bytes
													.toString(kv.getValue()))));

						}
					}

				}

				// for (Map.Entry<Integer, Double> entry :
				// versionMap.entrySet()) {
				// sm_jsonArray.clear();
				// sm_jsonArray.add(entry.getValue());
				// }
				// jsonObject.put(appType, sm_jsonArray);
				if (sm_appName != null) {
					sm_jsonArray2.add(sm_appName);
					for (int i = 0; i <= lDate.size() - 1; i++) {
						sm_jsonArray.add(versionMap.get(sdf
								.format(lDate.get(i)).replace('-', '/')));
					}
					sm_jsonArray2.add(sm_jsonArray);
					jsonArray.add(sm_jsonArray2);
					sm_jsonArray.clear();
				}
			}

		}
		// jsonObject.put("appName", appType);

		return jsonArray;

	}

	public JSONArray genFlowL2_SumHbase_day(String date1, String date2,
			String userGroup, Map<String, List<String>> appNamesMap,
			String appTraffic_Field, List<String> devNameList)
			throws IOException {
		
		JSONArray sm_jsonArray = null;
		// JSONObject jsonObject = new JSONObject();
		Result[] rst = null;
		JSONArray jsonArraysm = new JSONArray();
		DayList dy = new DayList();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		List<Date> lDate = dy.findDates(date1, date2);
		for (Map.Entry<String, List<String>> appEntry : appNamesMap.entrySet()) {
			sm_jsonArray = new JSONArray();
			JSONArray jsonArray = new JSONArray();
			Map<String, Double> versionMap = new HashMap<String, Double>();
			for (int i = 0; i <= lDate.size() - 1; i++) {
				versionMap.put(sdf.format(lDate.get(i)).replace('-', '/'), 0.0);
			}

			String sm_appType = null;
			for (String appName : appEntry.getValue()) {
				for (String devName : devNameList) {
					List<Get> getList = new ArrayList<Get>();
					for (Date day : lDate) {
						// concat the rowkey with the input field
						String rowkey = sdf.format(day).replace('-', '/')
								+ "\001" + userGroup + "\001"
								+ appEntry.getKey() + "\001" + appName + "\001"
								+ devName;
						Get get = new Get(Bytes.toBytes(rowkey));
						get.addColumn(Bytes.toBytes("cf"),
								Bytes.toBytes(appTraffic_Field+"_Sum"));
						getList.add(get);
					}
					rst = tableDay.get(getList);

					for (int i = 0; i < rst.length - 1; i++) {
						for (KeyValue kv : rst[i].raw()) {
							String arr2[] = Bytes.toString(kv.getRow()).split(
									"\001");
							sm_appType = arr2[2];
							versionMap.put(
									arr2[0],
									versionMap.get(arr2[0])
											+ Double.parseDouble((Bytes
													.toString(kv.getValue()))));
//							System.out.println("###this is the test output###");
//							System.out
//									.print(Bytes.toString(kv.getKey()) + "  ");
//							System.out.print(arr2[0] + "  ");
//							System.out.println(Bytes.toString(kv.getValue())
//									+ "  ");
//							System.out
//									.println("######this is the test output########");
						}
					}
				}
			}
			if (sm_appType != null) {
				jsonArray.add(sm_appType);
				for (int i = 0; i <= lDate.size() - 1; i++) {
					sm_jsonArray.add(versionMap.get(sdf.format(lDate.get(i))
							.replace('-', '/')));
				}

				jsonArray.add(sm_jsonArray);
				jsonArraysm.add(jsonArray);
				sm_jsonArray.clear();
			}
		}

		// jsonObject.put("appName", appType);
		return jsonArraysm;
	}

	public JSONArray genFlowPieHbase_day(String date1, String date2,
			String userGroupId, Map<String, List<String>> appTypeMap,
			String appTraffic_Field, List<String> devNameList)
			throws IOException {
		// public JSONArray genFlowPieHbase() throws IOException {
		System.out.println(appTraffic_Field+"_Sum");
		JSONArray jsonArrayPie = new JSONArray();
		DayList dy = new DayList();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		List<Date> lDate = dy.findDates(date1, date2);
		// System.out.println();
		for (Map.Entry<String, List<String>> appEntry : appTypeMap.entrySet()) {
			System.out.println(appEntry.getKey());
			for (String appName : appEntry.getValue()) {
				JSONArray sm_jsonArrayPie = new JSONArray();
				System.out.println(appName);
				double sum = 0.0;
				String sm_appName = null;
				for (String devName : devNameList) {
					Result[] rs = null;
					List<Get> getList = new ArrayList<Get>();
					for (Date day : lDate) {
						String rowkey = sdf.format(day).replace('-', '/')
								+ "\001" + userGroupId + "\001"
								+ appEntry.getKey() + "\001" + appName + "\001"
								+ devName;
						System.out.println("rowkey" + rowkey);
						Get get = new Get(Bytes.toBytes(rowkey));

						get.addColumn(Bytes.toBytes("cf"),
								Bytes.toBytes(appTraffic_Field+"_Sum"));
						getList.add(get);
					}
					rs = tableDay.get(getList);
					if (rs != null) {
						System.out.println("PieHbase  :" + rs.toString());
						for (int i = 0; i < rs.length - 1; i++) {
							for (KeyValue kv : rs[i].raw()) {
								String[] arr2 = Bytes.toString(kv.getRow())
										.split("\001");
								sm_appName = arr2[3];
								sum += Double.parseDouble(Bytes.toString(kv
										.getValue()));
							}
						}
					}

					System.out.print(appName + ":" + sum + "    ");

				}
				if (sm_appName != null) {
					sm_jsonArrayPie.add(sm_appName);
					sm_jsonArrayPie.add(sum);
					jsonArrayPie.add(sm_jsonArrayPie);
				}

			}

		}
		return jsonArrayPie;
	}

	public JSONArray genFlowPie_SumHbase_day(String date1, String date2,
			String userGroupId, Map<String, List<String>> appTypeMap,
			String appTraffic_Field, List<String> devNameList)
			throws IOException {
		JSONArray jsonArrayPie = new JSONArray();
		DayList dy = new DayList();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		List<Date> lDate = dy.findDates(date1, date2);
		for (Map.Entry<String, List<String>> appEntry : appTypeMap.entrySet()) {
			double sum = 0.0;
			String appType = null;
			JSONArray sm_jsonArrayPie = new JSONArray();
			for (String appName : appEntry.getValue()) {
				for (String devName : devNameList) {
					Result[] rs = null;
					List<Get> getList = new ArrayList<Get>();
					for (Date day : lDate) {
						String rowkey = sdf.format(day).replace('-', '/')
								+ "\001" + userGroupId + "\001"
								+ appEntry.getKey() + "\001" + appName + "\001"
								+ devName;
						System.out.println("rowkey:" + rowkey);
						Get get = new Get(Bytes.toBytes(rowkey));
						// get.getMaxVersions();
						get.addColumn(Bytes.toBytes("cf"),
								Bytes.toBytes(appTraffic_Field+"_Sum"));
						// get.setTimeRange(0L, Long.MAX_VALUE);
						getList.add(get);
					}
					rs = tableDay.get(getList);
					if (rs != null) {
						System.out.println("PieHbase  :" + rs.toString());
						for (int i = 0; i < rs.length - 1; i++) {
							for (KeyValue kv : rs[i].raw()) {
								String[] arr2 = Bytes.toString(kv.getRow())
										.split("\001");
								appType = arr2[2];
								sum += Double.parseDouble(Bytes.toString(kv
										.getValue()));
							}
						}
					}

					System.out.print(appName + ":" + sum + "    ");

				}
			}
			if (appType != null) {
				sm_jsonArrayPie.add(appType);
				sm_jsonArrayPie.add(sum);
				jsonArrayPie.add(sm_jsonArrayPie);

			}

		}
		return jsonArrayPie;
	}

	public JSONArray genFlowTop10Hbase_day(String date1, String date2,
			String userGroupId, Map<String, List<String>> appMap,
			String appTraffic_Field, List<String> devNameList)
			throws IOException {
		Map<Double, ArrayList<JSONArray>> appName_numMap = new HashMap<Double, ArrayList<JSONArray>>();
		// ArrayList<JSONArray> appList = new ArrayList<JSONArray>();
		JSONArray top_jsonArray = new JSONArray();
		DayList dy = new DayList();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		List<Date> lDate = dy.findDates(date1, date2);
		for (Map.Entry<String, List<String>> appEntry : appMap.entrySet()) {
			System.out.println(appEntry.getKey());
			for (String appName : appEntry.getValue()) {
				JSONArray one_jsonArray = new JSONArray();
				System.out.println(appName);
				System.out.println("&&&" + one_jsonArray);
				Map<JSONArray, Double> tmpMap = new HashMap<JSONArray, Double>();
				for (String devName : devNameList) {

					Result rst[] = null;
					List<Get> getList = new ArrayList<Get>();
					for (Date day : lDate) {
						double sum = 0.0;
						// one_jsonArray = new JSONArray();
						String rowkey = sdf.format(day).replace('-', '/')
								+ "\001" + userGroupId + "\001"
								+ appEntry.getKey() + "\001" + appName + "\001"
								+ devName;
						System.out.println("rowkey:" + rowkey);
						Get get = new Get(Bytes.toBytes(rowkey));
						// get.getMaxVersions();
						get.addColumn(Bytes.toBytes("cf"),
								Bytes.toBytes(appTraffic_Field+"_Sum"));
						// get.setTimeRange(0L, Long.MAX_VALUE);
						getList.add(get);
					}
					rst = tableDay.get(getList);
					if (rst != null) {
						one_jsonArray.clear();
						one_jsonArray.add(appEntry.getKey());
						one_jsonArray.add(appName);
						if (!tmpMap.containsKey(one_jsonArray)) {
							for (int i = 0; i < rst.length - 1; i++) {
								for (KeyValue kv : rst[i].raw()) {
									tmpMap.put(one_jsonArray, Double
											.parseDouble(Bytes.toString(kv
													.getValue())));
								}
							}
						} else {
							for (int i = 0; i < rst.length - 1; i++) {
								for (KeyValue kv : rst[i].raw()) {
									tmpMap.put(
											one_jsonArray,
											Double.parseDouble(Bytes
													.toString(kv.getValue()))
													+ tmpMap.get(one_jsonArray));
								}
							}
						}

					}
					// appList.add(one_jsonArray);
				}
				for (Map.Entry<JSONArray, Double> e : tmpMap.entrySet()) {
					ArrayList<JSONArray> tmpList = new ArrayList<JSONArray>();
					if (appName_numMap.get(e.getValue()) != null) {
						ArrayList<JSONArray> tmpList2 = appName_numMap.get(e
								.getValue());
						tmpList2.add(e.getKey());
						appName_numMap.put(e.getValue(), tmpList2);
					} else {
						tmpList.add(e.getKey());
						appName_numMap.put(e.getValue(), tmpList);
					}
				}
			}
		}
		// sort the sum of appName ,and get the top 10
		Double[] sm_value = appName_numMap.keySet().toArray(
				new Double[appName_numMap.size()]);
		Arrays.sort(sm_value);
		JSONArray tmpjArray = new JSONArray();
	//	System.out.println(",,,,," + sm_value.length);
		// System.out.println(",,,,," + appName_numMap.get(0.0).get(0));

		int count = 1;
		for (int i = sm_value.length - 1; i >= 0; i--) {
			if (count <= 10) {
			//	System.out.println("the key of the map is: " + i);
				for (JSONArray oneAppName : appName_numMap.get(sm_value[i])) {
					oneAppName.add(sm_value[i]);
					top_jsonArray.add(oneAppName);
				}
			}
			count++;
		}

		return top_jsonArray;
	}

	public JSONObject genFlowDetailsHbase_day(String date1, String date2,
			Map<String, String> userGroupId,
			Map<String, List<String>> appTypeMap, List<String> devNameList,
			int iDisplayStart, int iDisplayLength) throws IOException {
		DayList dy = new DayList();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		List<Date> lDate = dy.findDates(date1, date2);
		Filter filter = new PageFilter(iDisplayLength);
		int localRows = iDisplayStart;
		int total = 0;
		
		byte[] lastRow = Bytes.toBytes(iDisplayStart);
		System.out.println(lastRow.length);
		String[] column = { "R_StartTime", "R_EndTime", "AppUserNum",
				"AppTraffic_UP", "AppTraffic_DN", "AppPacketsNum",
				"AppSessionNum", "AppNewSessionNum" };
		Result[] result = new Result[column.length];
		JSONObject allObject = new JSONObject();
		JSONArray outArray = new JSONArray();
		int realLength=iDisplayLength+iDisplayStart-1;
		JSONArray propertyArray = new JSONArray();
		for (Map.Entry<String, List<String>> appEntry : appTypeMap.entrySet()) {
			for (String appName : appEntry.getValue()) {
				for (Map.Entry<String, String> userEntry : userGroupId
						.entrySet()) {
					for (String devName : devNameList) {
						for (Date day : lDate) {
						
						//	System.out.println("....." + devName);
							String rowkey = sdf.format(day).replace('-', '/')
									+ "\001" + userEntry.getKey() + "\001"
									+ appEntry.getKey() + "\001" + appName
									+ "\001" + devName;
							System.out.println("rowkey" + rowkey);
							for (int no = 0; no <= column.length - 1; no++) {
								System.out.println("column " + column[no]);
								Get get = new Get(Bytes.toBytes(rowkey));
								get.setMaxVersions();
								get.setTimeRange(0L, Long.MAX_VALUE);
								get.addColumn(Bytes.toBytes("cf"),
										Bytes.toBytes(column[no]));
								result[no] = table.get(get);
//								System.out.println("resultlength"
//										+ result.length);
							}

						
					}
						if (result[1].getRow() != null) {
							
							
							if(realLength>=result[1].size()){
								realLength=result[1].size();
							}
							for (int kno = iDisplayStart-1; kno <=realLength-1; kno++) {
								total=result[1].size();
								Map<Integer, String> resultMap = new HashMap<Integer, String>();
								propertyArray.add(devName);
								localRows++;
								for (int i = 0; i <= result.length - 1; i++) {
									String[] rowkeyarr = Bytes.toString(
											result[i].getRow()).split(
											"\001");
									if (i < 2) {//0,1
										
										resultMap.put(
												i,
												Bytes.toString(result[i]
														.list().get(kno)
														.getValue()));
										System.out.println("<<:"+Bytes.toString(result[i]
												.list().get(kno)
												.getValue()));
									}
									if (i == 2) {
										
										resultMap.put(2, userGroupId
												.get(rowkeyarr[1]));
										resultMap.put(3, rowkeyarr[2]);
										resultMap.put(4, rowkeyarr[3]);
										resultMap.put(
												5,
												Bytes.toString(result[i]
														.list().get(kno)
														.getValue()));
										System.out.println("==:"+Bytes.toString(result[i]
												.list().get(kno)
												.getValue()));
									}
									if (i > 2) {//3,4,5,6,7
										resultMap.put(
												i + 3,//6,7,8,9,10
												Bytes.toString(result[i]
														.list().get(kno)
														.getValue()));
										System.out.println(">>:"+Bytes.toString(result[i]
												.list().get(kno)
												.getValue()));
									}
//									System.out.println("column Value---"+column[i]+"##resultMap:" + i
//											+ "-->" + resultMap.get(i));
//								System.out.println();
								}
							

								for (int mno = 0; mno <= resultMap.size() - 1; mno++) {
//									System.out.println("*****resultMap:"
//											+ mno + "-->"
//											+ resultMap.get(mno));
									propertyArray.add(resultMap.get(mno));
									
								}
								
								outArray.add(propertyArray);
								propertyArray.clear();

							}
						}
					}
				}
			}
		}

		allObject.put("aaData", outArray);
		allObject.put("iTotalDisplayRecords", localRows-1);
		allObject.put("iTotalRecords",total);
		return allObject;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ReadHBaseGenTable rht = new ReadHBaseGenTable();
		List<String> appList = new ArrayList<String>();
		List<String> devList = new ArrayList<String>();
	//	devList.add("gn-1");
		// devList.add("pp");
		// devList.add("test");
	//	devList.add("angry");
		devList.add("绿网");
		Map<String, List<String>> appMap = new HashMap<String, List<String>>();
		appList.add("uusee中http流");
		appList.add("hotmail邮箱");
		appList.add("BitTorrent");
		appList.add("我乐网");
//		appList.add("阿里通");
		appMap.put("VoIP", appList);
		appMap.put("P2PStream", appList);
		appMap.put("VideoStream", appList);
		appMap.put("P2PDownload", appList);
		Map<String, String> appMap2 = new HashMap<String, String>();
		appMap2.put("0", "海2区");
		JSONArray jsonArraynew = new JSONArray();
		JSONArray jsonArraynew2 = new JSONArray();
		JSONArray jsonArray = new JSONArray();
		JSONArray jsonArray1 = new JSONArray();
		JSONArray jsonArray2 = new JSONArray();
		JSONArray jsonArray3 = new JSONArray();
		JSONObject jsonObject = new JSONObject();
		try {
			 jsonArraynew = rht.genFlowL2Hbase("2015/04/21", 
			 "0", appMap, "AppTraffic_UP", devList);
			 jsonArraynew2 = rht.genFlowL2_SumHbase_day("2015/04/21",
			 "2015/04/21", "0", appMap, "AppTraffic_UP", devList);
			 jsonArray = rht.genFlowPieHbase_day("2015/04/21", "2015/04/21",
			 "0",
			 appMap, "AppTraffic_UP", devList);
//			 jsonArray1 = rht.genFlowPie_SumHbase_day("2015/03/30",
//			 "2015/03/30",
//			 "0", appMap, "AppTraffic_UP", devList);
//			 jsonArray2 = rht.genFlowTop10Hbase_day("2015/03/30", "2015/03/30",
//			 "0", appMap, "AppTraffic_UP", devList);
//			 jsonArray3 = rht.genFlowTotalInfoHbase();
			jsonObject = rht.genFlowDetailsHbase_day("2015/04/21",  "2015/04/21",appMap2, appMap,
					devList, 1, 5);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 System.out.println("$$$$******this is the jsonarraynew*******");
		 System.out.println(jsonArraynew.toString());
		 System.out.println("$$$$******this is the jsonarraynew2*******");
		 System.out.println(jsonArraynew2.toString());
		 System.out.println("$$$$******this is the jsonarray*******");
		 System.out.println(jsonArray.toString());
		 System.out.println("$$$$******this is the jsonarray1*******");
		 System.out.println(jsonArray1.toString());
		 System.out.println("$$$******this is the jsonarray2*******");
		 System.out.println(jsonArray2.toString());
		 System.out.println("$$$$******this is the jsonarray3*******");
		 System.out.println(jsonArray3.toString());
		System.out.println("$$$$******this is the jsonobject*******");
		System.out.println(jsonObject.toString());
		// System.out.println("#########this is the Sum_jsonarray##########");
		// System.out.println(jaSum.toString());

	}

	public void read() throws IOException {
		// long st=System.currentTimeMillis();

		Get get = new Get(Bytes.toBytes(("row1")));
		// Get get=new Get(); 0002df4afd7476a5945f98da4572bf17
		// get.addFamily(Bytes.toBytes("records"));
		get.getMaxVersions();
		get.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("id"));
		get.setTimeRange(0L, Long.MAX_VALUE);
		Result rst = table.get(get);
		// table.close();
		// long en=System.currentTimeMillis();
		// System.out.println(en-st);
		System.out.println(get.setMaxVersions());
		final NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = rst
				.getMap();
		final NavigableMap<byte[], NavigableMap<Long, byte[]>> familyMap = map
				.get(Bytes.toBytes("cf1"));
		final NavigableMap<Long, byte[]> versionMap = familyMap.get(Bytes
				.toBytes("id"));
		for (final Map.Entry<Long, byte[]> entry : versionMap.entrySet()) {
			System.err.println(Bytes.toBytes("id") + " -> "
					+ Bytes.toString(entry.getValue()));
		}

		// final NavigableMap<Long, byte[]> versionMap2 =
		// familyMap.get(qualifier2);
		// for (final Map.Entry<Long, byte[]> entry : versionMap2.entrySet())
		// {
		// System.err.println(Bytes.toString(qualifier2) + " -> " +
		// Bytes.toString(entry.getValue()));
		// }

		// System.out.println("size"+rst.size()+",value="+
		// Bytes.toInt(rst.list().get(0).getValue()));
		for (KeyValue rs : rst.raw()) {
			System.out.print(Bytes.toString(rs.getRow()) + "  ");
			System.out.println(Bytes.toString(rs.getValue()));
		}

	}

}
