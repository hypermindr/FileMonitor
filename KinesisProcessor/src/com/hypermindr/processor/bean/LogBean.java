package com.hypermindr.processor.bean;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

/**
 * Java bean created to save data into DynamoDB. This bean is used when the optional DynamoDB integration is enabled. 
 * @author ricardo
 *
 */
@DynamoDBTable(tableName = "LogRaas")
public class LogBean {
	
	private String tracerid;
	
	private Double timestamp;
	
	private String module_name;
	
	private String class_name;
	
	private String method_name;
	
	private String message;

	@DynamoDBHashKey
	public String getTracerid() {
		return tracerid;
	}

	public void setTracerid(String tracerid) {
		this.tracerid = tracerid;
	}

	 @DynamoDBRangeKey
	public Double getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Double timestamp) {
		this.timestamp = timestamp;
	}

	@DynamoDBAttribute(attributeName = "moduleName")
	public String getModule_name() {
		return module_name;
	}

	public void setModule_name(String module_name) {
		this.module_name = module_name;
	}

	@DynamoDBAttribute(attributeName = "className")
	public String getClass_name() {
		return class_name;
	}

	
	public void setClass_name(String class_name) {
		this.class_name = class_name;
	}

	@DynamoDBAttribute(attributeName = "methodName")
	public String getMethod_name() {
		return method_name;
	}

	public void setMethod_name(String method_name) {
		this.method_name = method_name;
	}

	@DynamoDBAttribute(attributeName = "message")
	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public static LogBean createFromJson(String kinesisData) {
		DBObject json = (DBObject) JSON.parse(kinesisData);
		LogBean bean = new LogBean();
		String tracerid = (String) json.get("tracerid");
		if (tracerid == null) {
			return null;
		}
		bean.setTracerid(tracerid);
		
		bean.setClass_name((String) json.get("class_name"));
		bean.setModule_name((String) json.get("module_name"));
		bean.setMethod_name((String) json.get("method_name"));
		
		try{
			bean.setTimestamp((Double)json.get("timestamp"));
		}catch(Exception ex) {
			bean.setTimestamp(new Double( ((Long)json.get("timestamp")).toString()));
		}
		bean.setMessage((String)json.get("message"));
		return bean;
	}
	
	

}
