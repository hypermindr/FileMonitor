package com.hypermindr.processor.nosql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.hypermindr.processor.bean.BeaconBean;
import com.hypermindr.processor.bean.BeaconRawBean;
import com.hypermindr.processor.main.KinesisLogTailer;
import com.hypermindr.processor.util.KinesisProcessorProperties;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.util.Tables;

public class DynamoDBConnector {

	
	private static String accessKey = KinesisProcessorProperties.getInstance()
			.getAwsAkey();
	private static String secretKey = KinesisProcessorProperties.getInstance()
			.getAwsSkey();
	static AWSCredentials credentials = new BasicAWSCredentials(accessKey,
			secretKey);

	private static AmazonDynamoDBClient dynamoClient = new AmazonDynamoDBClient(
			credentials);
	
	private static DynamoDBMapper mapper = new DynamoDBMapper(dynamoClient);
	
	private static DynamoDB dynamoDB = new DynamoDB(dynamoClient);
	
	private static DynamoDBConnector instance;
	
	private static Table beaconTable;
	
	private static Table rawBeaconTable;

	private static String beaconTablePrefix = "Kinesis";
	private static String beaconRawTablePrefix = "KinesisRaw";
	
	private static String hashKeyName = "beaconKey";
	private static String hashKeyNameRaw = "rawBeaconKey";
	
	private static String beaconTableName = beaconTablePrefix+KinesisProcessorProperties.getInstance().getPartner();
	private static String beaconRawTableName = beaconRawTablePrefix+KinesisProcessorProperties.getInstance().getPartner();
	
	
	
	public static String getBeaconTableName() {
		return beaconTableName;
	}

	public static void setBeaconTableName(String beaconTableName) {
		DynamoDBConnector.beaconTableName = beaconTableName;
	}

	public static String getBeaconRawTableName() {
		return beaconRawTableName;
	}

	public static void setBeaconRawTableName(String beaconRawTableName) {
		DynamoDBConnector.beaconRawTableName = beaconRawTableName;
	}

	private boolean debug = KinesisProcessorProperties.getInstance()
			.getDebugFlag();
	
	private static Logger log = Logger.getLogger(DynamoDBConnector.class.getName());
	
	private DynamoDBConnector() {
		
	}
	
	public static DynamoDBConnector getInstance() {
		if (instance == null) {
			instance = new DynamoDBConnector();
			if (!Tables.doesTableExist(dynamoClient,beaconTableName)) {
				createNewTable(beaconTableName,hashKeyName);
			}

			beaconTable = dynamoDB.getTable(beaconTableName);
			if (!Tables.doesTableExist(dynamoClient,beaconRawTableName)) {
				createNewTable(beaconRawTableName,hashKeyNameRaw);
			}
			rawBeaconTable= dynamoDB.getTable(beaconRawTableName);
		}
		
		return instance;
	}
	
	

	public void updateKinesisBeacon(BeaconBean currentValuedBean) {
	
		String beacon = getCurrentBeacon(currentValuedBean.getBeaconKey());
		if (beacon == null) {
			mapper.save(currentValuedBean,new DynamoDBMapperConfig(new DynamoDBMapperConfig.TableNameOverride(beaconTableName)));
		}else{
			Map<String, String> expressionAttributeNames = new HashMap<String, String>();
            expressionAttributeNames.put("#sequence", "sequence");
            expressionAttributeNames.put("#timestamp", "timestamp");
            
            Map<String, Object> expressionAttributeValues = new HashMap<String, Object>();
            expressionAttributeValues.put(":key", currentValuedBean.getSequence());
            expressionAttributeValues.put(":timestamp", currentValuedBean.getTimestamp());
			 UpdateItemSpec updateItemSpec = new UpdateItemSpec()
	            .withPrimaryKey(hashKeyName, currentValuedBean.getBeaconKey())
	            .withUpdateExpression("set #sequence = :key, #timestamp = :timestamp")
	            .withNameMap(expressionAttributeNames)
	            .withValueMap(expressionAttributeValues)
	            .withReturnValues(ReturnValue.ALL_NEW);

	            UpdateItemOutcome outcome =  beaconTable.updateItem(updateItemSpec);
	            if (debug) {
	            	log.debug(outcome.getUpdateItemResult().toString());
	            }
		}
	}
	
	public void updateKinesisRawBeacon(BeaconRawBean currentValuedBean) {
		
		String beacon = getCurrentBeacon(currentValuedBean.getRawBeaconKey());
		if (beacon == null) {
			mapper.save(currentValuedBean,new DynamoDBMapperConfig(new DynamoDBMapperConfig.TableNameOverride(beaconRawTableName)));
		}else{
			Map<String, String> expressionAttributeNames = new HashMap<String, String>();
            expressionAttributeNames.put("#sequenceRaw", "sequenceRaw");
            expressionAttributeNames.put("#timestamp", "timestamp");
            
            Map<String, Object> expressionAttributeValues = new HashMap<String, Object>();
            expressionAttributeValues.put(":key", currentValuedBean.getSequenceRaw());
            expressionAttributeValues.put(":timestamp", currentValuedBean.getTimestamp());
			 UpdateItemSpec updateItemSpec = new UpdateItemSpec()
	            .withPrimaryKey(hashKeyNameRaw, currentValuedBean.getRawBeaconKey())
	            .withUpdateExpression("set #sequenceRaw = :key, #timestamp = :timestamp")
	            .withNameMap(expressionAttributeNames)
	            .withValueMap(expressionAttributeValues)
	            .withReturnValues(ReturnValue.ALL_NEW);

	            UpdateItemOutcome outcome =  rawBeaconTable.updateItem(updateItemSpec);
	            if (debug) {
	            	log.debug(outcome.getUpdateItemResult().toString());
	            }
		}
	}
	
	
	public String getCurrentBeacon(String shardId) {
		if (!Tables.doesTableExist(dynamoClient, beaconTableName)) return null;
		if (beaconTable == null) {
			beaconTable = dynamoDB.getTable(beaconTableName);
		}
		Item beacon = null;
		try{
			beacon = beaconTable.getItem("beaconKey",shardId);
		}catch(Exception e) {
			return null;
		}
		if (beacon != null) {
			if (beacon.getString("sequence") != null && beacon.getString("sequence").equals("0")){
				return null;
			}
			return beacon.getString("sequence");
		}
		return null;
	}

	public String getCurrentRawBeacon(String shardId) {
		if (!Tables.doesTableExist(dynamoClient, beaconRawTableName)) return null;
		if (rawBeaconTable == null) {
			rawBeaconTable = dynamoDB.getTable(beaconRawTableName);
		}
		Item beacon = null;
		try{
			beacon = rawBeaconTable.getItem("rawBeaconKey",shardId);
		}catch(Exception e) {
			return null;
		}
		if (beacon != null) {
			if (beacon.getString("sequenceRaw") != null && beacon.getString("sequenceRaw").equals("0")){
				return null;
			}
			return beacon.getString("sequenceRaw");
		}
		return null;
	}
	
	private static boolean createNewTable(String tableName, String hashKeyName) {
		
		try {

	            ArrayList<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
	            keySchema.add(new KeySchemaElement()
	                .withAttributeName(hashKeyName)
	                .withKeyType(KeyType.HASH));
	            
	            ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
	            attributeDefinitions.add(new AttributeDefinition()
	                .withAttributeName(hashKeyName)
	                .withAttributeType("S"));

	           
	            CreateTableRequest request = new CreateTableRequest()
	                    .withTableName(tableName)
	                    .withKeySchema(keySchema)
	                    .withProvisionedThroughput( new ProvisionedThroughput()
	                        .withReadCapacityUnits(new Long(120))
	                        .withWriteCapacityUnits(new Long(120)));

	          

	            request.setAttributeDefinitions(attributeDefinitions);

	            log.debug("Issuing CreateTable request for " + tableName);
	            Table table = dynamoDB.createTable(request);
	            log.debug("Waiting for " + tableName
	                + " to be created...this may take a while...");
	            table.waitForActive();
	            return true;
	        } catch (Exception e) {
	        	log.error("CreateTable request failed for " + tableName);
	        	log.error(e.getMessage());
	            return false;
	        }
		
	}
}
