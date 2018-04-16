package com.lsw.mapper;

import static org.apache.storm.hbase.common.Utils.toBytes;
import static org.apache.storm.hbase.common.Utils.toLong;

import org.apache.storm.hbase.bolt.mapper.HBaseMapper;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;
import org.apache.storm.hbase.common.ColumnList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class MyHbaseMapper implements HBaseMapper {
	private static final Logger LOG = LoggerFactory
			.getLogger(SimpleHBaseMapper.class);

	private String rowKeyField;
	// private String timestampField;
	private byte[] columnFamily;
	private Fields columnFields;
	private Fields counterFields;

	public MyHbaseMapper() {
	}

	public MyHbaseMapper withRowKeyField(String rowKeyField) {
		this.rowKeyField = rowKeyField;
		return this;
	}

	public MyHbaseMapper withColumnFields(Fields columnFields) {
		this.columnFields = columnFields;
		return this;
	}

	public MyHbaseMapper withCounterFields(Fields counterFields) {
		this.counterFields = counterFields;
		return this;
	}

	public MyHbaseMapper withColumnFamily(String columnFamily) {
		this.columnFamily = columnFamily.getBytes();
		return this;
	}

	// public SimpleTridentHBaseMapper withTimestampField(String
	// timestampField){
	// this.timestampField = timestampField;
	// return this;
	// }

	@Override
	public byte[] rowKey(Tuple tuple) {
		Object objVal = tuple.getValueByField(this.rowKeyField);
		return toBytes(objVal);
	}

	@Override
	public ColumnList columns(Tuple tuple) {
		ColumnList cols = new ColumnList();
		if (this.columnFields != null) {
			// TODO timestamps
			for (String field : this.columnFields) {
				cols.addColumn(this.columnFamily, field.getBytes(),
						(Long) tuple.getValueByField("timestamp"),
						toBytes(tuple.getValueByField(field)));
				/*
				 * cols.addColumn(this.columnFamily, field.getBytes(), 0L,
				 * toBytes(tuple .getValueByField(field)));
				 */
			}
		}
		if (this.counterFields != null) {
			for (String field : this.counterFields) {
				// cols.addCou

				cols.addCounter(
						this.columnFamily,
						toBytes(field + "_" + tuple.getValueByField("dateHour")),
						toLong(tuple.getValueByField(field)));

			}
		}
		return cols;
	}
}
