package com.hypermindr.processor.model;

import java.util.Arrays;

public class PerformanceLineRecord extends PerformanceLineModel {
	
	public PerformanceLineRecord(PerformanceLineModel mirror, double endTime, double beginTime,String[] messages) {
		this.timeElapsed = endTime-beginTime;
		this.setEndpoint(mirror.getEndpoint());
		this.setTracerid(mirror.getTracerid());
		this.setMethod_name(mirror.getMethod_name());
		this.setClass_name(mirror.getClass_name());
		this.setModule_name(mirror.getModule_name());
		this.setServer(mirror.getServer());
		this.setTimestamp(mirror.getTimestamp());
		this.setMessage(Arrays.toString(messages));
	}
	
	private double timeElapsed;

	public double getTimeElapsed() {
		return timeElapsed;
	}

	public void setTimeElapsed(double timeElapsed) {
		this.timeElapsed = timeElapsed;
	}
	
	public String getPerformanceDatumKey() {
		return this.getEndpoint() + ":"
				+ this.getModule_name() + ":"
				+ this.getClass_name() + ":"
				+ this.getModule_name();
	}
	

}
