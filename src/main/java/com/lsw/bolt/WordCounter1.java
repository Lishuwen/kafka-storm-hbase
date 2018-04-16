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
import java.util.HashMap;
import java.util.Map;

import static backtype.storm.utils.Utils.tuple;

public class WordCounter1 implements IBasicBolt {
	Map<String, Integer> countsMap = new HashMap<String, Integer>();
	private String part;

	public String getPart() {
		return part;
	}

	public void setPart(String part) {
		this.part = part;
	}

	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context) {

	}

	/*
	 * Just output the word value with a count of 1. The HBaseBolt will handle
	 * incrementing the counter.
	 */
	public void execute(Tuple input, BasicOutputCollector collector) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String word = ((String) (input.getValues().get(0)));
		Integer up = ((Integer) (input.getValues().get(1)));
		Integer down = ((Integer) (input.getValues().get(2)));
		Long ts = (Long) (input.getValues().get(3));
		Integer upcount = 0, downcount = 0;
		String dateHour = "";
		
		//行键+时间戳为键，将相同周期的应用的流量加和，保存在bolt全局的map中
		if (countsMap.containsKey("u" + word + ts)) {
			upcount = countsMap.get("u" + word + ts);
		}
		if (countsMap.containsKey("d" + word + ts)) {
			downcount = countsMap.get("d" + word + ts);
		}
		upcount += up;
		downcount += down;
		countsMap.put("u" + word + ts, upcount);
		countsMap.put("d" + word + ts, downcount);

		dateHour = sdf.format(new Date(ts));
		collector.emit(tuple(word, upcount, downcount, ts,sdf2.format(new Date(ts)), dateHour, upcount,
				downcount));

	}

	public void cleanup() {

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		String dateHour = "dateHour";
		String upSum = "upS";
		String downSum = "downS";
		declarer.declare(new Fields("rowkey", "up", "down", "timestamp","realtime",
				dateHour, upSum, downSum));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}