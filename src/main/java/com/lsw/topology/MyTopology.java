package com.lsw.topology;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.hbase.bolt.HBaseBolt;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.lsw.bolt.WordCounter;
import com.lsw.bolt.WordCounter1;
import com.lsw.mapper.MyHbaseMapper;

public class MyTopology {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		//zookeeper的ip及端口
		BrokerHosts brokerHosts = new ZkHosts(
				"master:2181,slave1:2181,slave2:2181");
		String topic = "mytopic";
		String zkRoot = "/storm";
		//两个不同的任务不能使用同一个spoutid,这个id在zookeeper中保留的是一个目录，进zk客户端可以查看详细信息
		String spoutId = "myKafka";
		
		String spoutId2 = "myKafka2";
		int spoutNum = 3;
		SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, topic, zkRoot,
				spoutId);
	//	spoutConfig.startOffsetTime=kafka.api.OffsetRequest.LatestTime();;
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		new StringScheme();
		SpoutConfig spoutConfig2 = new SpoutConfig(brokerHosts, "test", zkRoot,
				spoutId2);
	
		//设置kafkaspout输出的schema，包括streamid
		spoutConfig2.scheme = new SchemeAsMultiScheme(new StringScheme());
		Config config = new Config();
		Map<String, Object> hbConf = new HashMap<String, Object>();
		// if (args.length > 0) {
		// hbConf.put("hbase.rootdir", args[0]);
		hbConf.put("hbase.rootdir", "hdfs://master:9000/hbase");
		// }
		config.put("hbase.conf", hbConf);
		//Tuple tu=new TupleImpl();
		long time=System.currentTimeMillis();
		SimpleDateFormat sdf = new SimpleDateFormat(
				"yyyyMMddHHmm");
		
		WordCounter bolt = new WordCounter();
		WordCounter1 bolt1 = new WordCounter1();
		
		//定义入库到hbase的映射形式（哪些字段作为行键，哪些字段作为普通列，哪些字段作为自增列），可实现HBaseMapper,定义自己的mapper
		MyHbaseMapper mapper = new MyHbaseMapper()
		.withRowKeyField("rowkey").withColumnFields(new Fields("up","down","timestamp","realtime"))
		.withCounterFields(new Fields("upS","downS")).withColumnFamily("cf");
		
		//设置入库的表名为lsw2,映射，以及HBase的根目录信息
		HBaseBolt hbase = new HBaseBolt("lsw2", mapper)
				.withConfigKey("hbase.conf");
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout2", new KafkaSpout(spoutConfig2));
		builder.setSpout("spout", new KafkaSpout(spoutConfig), spoutNum);
		 builder.setBolt("count-bolt", bolt, 1).shuffleGrouping("spout").shuffleGrouping("spout2");
		builder.setBolt("count-bolt1", bolt1, 1).fieldsGrouping("count-bolt",new Fields("rowkey","timestamp"));
		builder.setBolt("hbasebolt", hbase).shuffleGrouping("count-bolt1");
		
		//本地模式提交storm任务
		LocalCluster cl=new LocalCluster();
		cl.submitTopology("test", config,
				builder.createTopology());
		/*try {
		 //集群模式提交storm任务
			StormSubmitter.submitTopology(args[1], config,
					builder.createTopology());
		} catch (AlreadyAliveException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidTopologyException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/

	}

}
