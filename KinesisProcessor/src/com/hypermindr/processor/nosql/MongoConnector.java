package com.hypermindr.processor.nosql;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.hypermindr.processor.util.KinesisProcessorProperties;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoOptions;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import com.mongodb.util.JSON;

public class MongoConnector {

	private static Mongo mongo;

	private static MongoConnector instance;
	
	private static final String PERFORMANCE_SUFIX = "performance";

	private static Logger log = Logger
			.getLogger(MongoConnector.class.getName());
	
	private boolean debug = KinesisProcessorProperties.getInstance()
			.getDebugFlag();

	private MongoConnector() {
	}

	public static MongoConnector getInstance() {
		if (instance == null) {
			try {
				MongoOptions mo = new MongoOptions();
				mo.connectionsPerHost = 200;
				mo.maxWaitTime = 6000;
				mo.socketKeepAlive = true;
				mo.threadsAllowedToBlockForConnectionMultiplier = 1500;
				ServerAddress mongoAddress = new ServerAddress(
						KinesisProcessorProperties.getInstance()
								.getMongoDbUrl(),
						Integer.parseInt(KinesisProcessorProperties
								.getInstance().getMongoDbPort()));
				mo.alwaysUseMBeans = true;
				mongo = new Mongo(mongoAddress, mo);
				instance = new MongoConnector();
			} catch (UnknownHostException e) {
				e.printStackTrace();
				log.debug(e);
				throw new RuntimeException("Server not responding", e);
			}
		}
		return instance;
	}
	
	public Mongo getMongoConnection() throws NumberFormatException, UnknownHostException {
		MongoOptions mo = new MongoOptions();
		mo.connectionsPerHost = 100;
		mo.maxWaitTime = 6000;
		mo.socketKeepAlive = true;
		mo.threadsAllowedToBlockForConnectionMultiplier = 100;
		ServerAddress mongoAddress = new ServerAddress(
				KinesisProcessorProperties.getInstance()
						.getMongoDbUrl(),
				Integer.parseInt(KinesisProcessorProperties
						.getInstance().getMongoDbPort()));
		mo.alwaysUseMBeans = true;
		return new  Mongo(mongoAddress, mo);
	}
	
	

	public String saveLog(List<String> listJson, String collectionName) {
		if (listJson == null || listJson.size() == 0) {
			return "fail";
		}
		java.math.BigDecimal timestamp = null;
		try {
			List<DBObject> mObjects = new ArrayList<DBObject>();
			for (String json : listJson) {

				try {
					DBObject obj = (DBObject) JSON.parse(json);

					try {
						timestamp = new java.math.BigDecimal(
								(Double) obj.get("timestamp") * 1000);
					} catch (ClassCastException cce) {
						timestamp = new java.math.BigDecimal(
								(Long) obj.get("timestamp"));
					}
					obj.put("timestamp",
							Long.parseLong(timestamp.toPlainString()));
					mObjects.add(obj);
				} catch (com.mongodb.util.JSONParseException jpe) {
					jpe.printStackTrace();
				}
			}
			Mongo m = getMongoConnection();
			DB db = m.getDB(KinesisProcessorProperties.getInstance()
					.getMongoDbName());
			DBCollection collection = db.getCollection(collectionName);
			WriteResult result = collection.insert(mObjects,WriteConcern.NONE);
			if (result.getLastError().ok()) {
				if (debug)
					log.debug("Paired performance records saved");
				m.close();
				return "ok";
			}
			m.close();
			return "fail";
		} catch (Exception e) {
			e.printStackTrace();
			log.debug(e);
		}
		return null;
	}
	
	public String savePerformanceLog(List<String> listJson, String collectionName) {
		
		collectionName = collectionName+PERFORMANCE_SUFIX;
		
		if (listJson == null || listJson.size() == 0) {
			return "empty";
		}
		java.math.BigDecimal timestamp = null;
		try {
			List<DBObject> mObjects = new ArrayList<DBObject>();
			for (String json : listJson) {

				try {
					DBObject obj = (DBObject) JSON.parse(json);

					try {
						timestamp = new java.math.BigDecimal(
								(Double) obj.get("timestamp") * 1000);
					} catch (NumberFormatException nfe) {
						timestamp = new java.math.BigDecimal(
								(Double) obj.get("timestamp"));
					}catch(Exception e) {
						e.printStackTrace();
					}
					obj.put("timestamp",
							Long.parseLong(timestamp.toPlainString()));
					mObjects.add(obj);
				} catch (com.mongodb.util.JSONParseException jpe) {
					jpe.printStackTrace();
				}
			}
			Mongo m = getMongoConnection();
			DB db = m.getDB(KinesisProcessorProperties.getInstance()
					.getMongoDbName());
			DBCollection collection = db.getCollection(collectionName);

			WriteResult result = collection.insert(mObjects,WriteConcern.NORMAL);
			if (result.getLastError().ok()) {
				if (debug)
					log.debug("Single performance records saved");
				m.close();
				return "ok";
			}
			m.close();
			return "fail";
		} catch (Exception e) {
			e.printStackTrace();
			log.debug(e);
		}
		return "fail";
	}

}
