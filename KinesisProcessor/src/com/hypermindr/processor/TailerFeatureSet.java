package com.hypermindr.processor;

import java.util.ArrayList;

/**
 * Simple way to printout the feature set of KinsesisTailer instance
 * @author ricardo
 *
 */
public class TailerFeatureSet {
	
	public static ArrayList<String> features = new ArrayList<String>();
	
	static{
		features.add("TracerId as Kinesis PartitionKey");
		features.add("Perf log line sent as raw to raw stream");
		features.add("Multi Shards");
		features.add("Async Kinesis Writing");
		features.add("Multi-tenant support");
	}

	
	public static String getFeatures() {
		StringBuilder b = new StringBuilder();
		for (String feature : features) {
			b.append(" - "+feature+"\n");
		}
		return b.toString();
	}
}
