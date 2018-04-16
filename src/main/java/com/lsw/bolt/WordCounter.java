/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.lsw.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import static backtype.storm.utils.Utils.tuple;

public class WordCounter implements IBasicBolt {

	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context) {
	}

	/*
	 * Just output the word value with a count of 1. The HBaseBolt will handle
	 * incrementing the counter.
	 */
	public void execute(Tuple input, BasicOutputCollector collector) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		
		String str = ((String) (input.getValues().get(0)));
		int down = 0;
		int up = 0;
		long ts = 1452700800000L;
		String dateHour = "";
		String appType = "", appName = "", appid = "";
		System.out.println("str length-->"+str.split("\001").length);
		if (24 == str.split("\001").length) {
			appid = str.split("\001")[14];
			appType = str.split("\001")[13];
			appName = str.split("\001")[16];
			up = Integer.parseInt(str.split("\001")[18]);
			down = Integer.parseInt(str.split("\001")[19]);
			ts = Long.parseLong(str.split("\001")[11]);
			dateHour = sdf.format(new Date(ts));

		} else {

		}
		String rowkey1 = "#\001" + dateHour + "\001" + appType;
		String rowkey2 = "@\001" + dateHour + "\001" + appid + "\001" + appName;
		String rowkey3 = "$\001" + dateHour + "\001" + appType + "\001" + appid
				+ "\001" + appName;
		collector.emit(tuple(rowkey1, up, down, ts));
		collector.emit(tuple(rowkey2, up, down, ts));
		collector.emit(tuple(rowkey3, up, down, ts));

		
	}

	public void cleanup() {

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// String upSum = "upSum";
		// String downSum = "downSum";
		declarer.declare(new Fields("rowkey", "up", "down", "timestamp"));
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}