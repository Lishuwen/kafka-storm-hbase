package com.lsw.spout;

import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;



public class TestMessageScheme implements Scheme{
	 private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(TestMessageScheme.class);  
	
	
	 @Override
	public List<Object> deserialize(byte[] ser) {
		// TODO Auto-generated method stub
		 try {  
	            String msg = new String(ser, "UTF-8");  
	            return new Values(msg);  
	        } catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				LOGGER.error("Cannot parse the provided message!");  
			}  
	          
	        //TODO: what happend if returns null?  
		return null;
	}

	@Override
	public Fields getOutputFields() {
		// TODO Auto-generated method stub
		return new Fields("msg"); 
	}

}
