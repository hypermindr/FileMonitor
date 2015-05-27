package com.hypermindr.processor.bean;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

/**
 * Beacon used to store the pin-point of Kinesis into DynamoDB. This is
 * pin-point allows KinesisProcessor to restart from last entry processed,  in case of
 * failure or crash.
 * 
 * @author ricardo
 * 
 */

@DynamoDBTable(tableName = "KinesisBeacon")
public class BeaconBean {

	private String beaconKey;

	private String sequence;

	private Double timestamp;

	public BeaconBean(String id, String sequenceNumber, long l) {
		this.beaconKey = id;
		this.sequence = sequenceNumber;
		this.timestamp = new Double(l);
	}

	public BeaconBean(String sequenceNumber, long l) {
		sequence = sequenceNumber;
		timestamp = new Double(l);
	}

	@DynamoDBHashKey
	public String getBeaconKey() {
		return beaconKey;
	}

	public void setBeaconKey(String beaconKey) {
		this.beaconKey = beaconKey;
	}

	public String getSequence() {
		return sequence;
	}

	public void setSequence(String kinesiskey) {
		this.sequence = kinesiskey;
	}

	public Double getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Double timestamp) {
		this.timestamp = timestamp;
	}

}
