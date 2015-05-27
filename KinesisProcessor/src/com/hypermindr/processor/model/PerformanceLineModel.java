package com.hypermindr.processor.model;

public class PerformanceLineModel {

	private String server;
	
	private String tracerid;
	
	private String endpoint;
	
	private double timestamp;
	
	private String module_name;
	
	private String class_name;
	
	private String method_name;
	
	private String stage;
	
	private String message;

	public String getTracerid() {
		return tracerid;
	}

	public void setTracerid(String tracerId) {
		this.tracerid = tracerId;
	}

	public String getServer() {
		return server;
	}

	public void setServer(String server) {
		this.server = server;
	}

	public String getEndpoint() {
		return endpoint;
	}

	public void setEndpoint(String endpoint) {
		this.endpoint = endpoint;
	}

	public double getTimestamp() {
		return new Double(timestamp);
	}

	public void setTimestamp(double timestamp) {
		this.timestamp = timestamp;
	}

	public String getModule_name() {
		return module_name;
	}

	public void setModule_name(String module_name) {
		this.module_name = module_name;
	}

	public String getClass_name() {
		return class_name;
	}

	public void setClass_name(String class_name) {
		this.class_name = class_name;
	}

	public String getMethod_name() {
		return method_name;
	}

	public void setMethod_name(String method_name) {
		this.method_name = method_name;
	}

	public String getStage() {
		return stage;
	}

	public void setStage(String stage) {
		this.stage = stage;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	@Override
	public String toString() {
		return "PerformanceLineModel [tracerid=" + tracerid + ", server="
				+ server + ", endpoint=" + endpoint + ", timestamp="
				+ timestamp + ", module_name=" + module_name + ", class_name="
				+ class_name + ", method_name=" + method_name + ", stage="
				+ stage + ", message=" + message + "]";
	}
	
	
	public String getPairedID() {
		
		return tracerid+endpoint+module_name+class_name+method_name+server;
	}
	
	
	
}
