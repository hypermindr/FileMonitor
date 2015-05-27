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
@DynamoDBTable(tableName = "KinesisBeaconRaw")
public class BeaconRawBean {
	
	private String rawBeaconKey;
	
	private String sequenceRaw;
	
	private Double timestamp;

	public BeaconRawBean(String id,String sequenceNumber, long l) {
		rawBeaconKey = id;
		sequenceRaw = sequenceNumber;
		timestamp = new Double(l);
	}
	
	public BeaconRawBean(String sequenceNumber, long l) {
		sequenceRaw = sequenceNumber;
		timestamp = new Double(l);
	}
	
	@DynamoDBHashKey
	public String getRawBeaconKey() {
		return rawBeaconKey;
	}

	public void setRawBeaconKey(String beaconKey) {
		this.rawBeaconKey = beaconKey;
	}

	
	public String getSequenceRaw() {
		return sequenceRaw;
	}

	public void setSequenceRaw(String kinesiskey) {
		this.sequenceRaw = kinesiskey;
	}

	public Double getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Double timestamp) {
		this.timestamp = timestamp;
	}
	
}
