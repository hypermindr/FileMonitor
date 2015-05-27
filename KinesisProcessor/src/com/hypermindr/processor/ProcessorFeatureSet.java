package com.hypermindr.processor;

import java.util.ArrayList;

/**
 * Simple way to printout the feature set of KinsesisProcessor instance
 * @author ricardo
 *
 */
public class ProcessorFeatureSet {
	
	public static ArrayList<String> features = new ArrayList<String>();
	
	static{
		features.add("Single line performance records");
		features.add("Raw file aggregation");
		features.add("SyslogD integration");
		features.add("CloudWatch integration");
	}

	
	public static String getFeatures() {
		StringBuilder b = new StringBuilder();
		for (String feature : features) {
			b.append(" - "+feature+"\n");
		}
		return b.toString();
	}
}
