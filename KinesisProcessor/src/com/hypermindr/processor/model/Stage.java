package com.hypermindr.processor.model;

public enum Stage {
	
	BEGIN("B"),END("E"),RECORD("R");
	
	private String type;
	
	Stage(String t) {
		this.type = t;
	}
	
	public String getType() {
		return type;
	}

}
