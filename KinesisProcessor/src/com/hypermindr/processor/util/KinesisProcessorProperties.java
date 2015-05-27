package com.hypermindr.processor.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;

import com.hypermindr.processor.main.KinesisLogTailer;
import com.hypermindr.processor.tailer.TailerContext;

/**
 * @author ricardo Abstraction of config file. The config file must have this
 *         structure:
 * 
 *         hypermindr.kinesisprocessor.partner =
 *         hypermindr.kinesisprocessor.aws.secretkey =
 *         hypermindr.kinesisprocessor.aws.accesskey =
 *         hypermindr.kinesisprocessor.aws.kinesisStream =
 *         hypermindr.kinesisprocessor.aws.kinesisRawStream =
 *         hypermindr.kinesisprocessor.aws.kinesisShard =
 *         hypermindr.kinesisprocessor.aws.kinesisEndpoint =
 *         hypermindr.kinesisprocessor.aws.kinesisCname =
 *         hypermindr.kinesisprocessor.aws.kinesisLocation =
 *         hypermindr.kinesisprocessor.aws.kinesisPartitionKey =
 *         hypermindr.kinesisprocessor.aws.dinamoTable =
 *         hypermindr.kinesienvironment *sprocessor.mongo.url =
 *         hypermindr.kinesisprocessor.mongo.port =
 *         hypermindr.kinesisprocessor.mongo.db =
 *         hypermindr.kinesisprocessor.mongo.collection =
 *         hypermindr.kinesisprocessor.mongo.user =
 *         hypermindr.kinesisprocessor.mongo.passwd =
 *         hypermindr.kinesisprocessor.jsonOnly = true;
 *         hypermindr.kinesisprocessor.logfile.1 =
 *         hypermindr.kinesisprocessor.logfile.2 =
 *         hypermindr.kinesisprocessor.logfile.3 =
 *         hypermindr.kinesisprocessor.logfile.n =
 * 
 * 
 */

public class KinesisProcessorProperties {

	private static KinesisProcessorProperties instance;

	private static Logger log = Logger
			.getLogger(KinesisProcessorProperties.class.getName());

	// DEFINE CONFIGFILE FIELDS

	private static final int TAILER_BUFFER_PRESIZE = 490;
	private static final String TAILER_BUFFER_SIZE = "hypermindr.kinesisprocessor.tailerBufferSize";
	private static final String PARTNER = "hypermindr.kinesisprocessor.partner";
	private static final String DEBUG_FLAG = "hypermindr.kinesisprocessor.debug";
	private static final String RAW_ENABLED = "hypermindr.kinesisprocessor.aws.rawEnabled";
	private static final String RAW_FILE = "hypermindr.kinesisprocessor.rawFile";
	private static final String AWS_SKEY = "hypermindr.kinesisprocessor.aws.secretkey";
	private static final String AWS_AKEY = "hypermindr.kinesisprocessor.aws.accesskey";
	private static final String AWS_KINESIS_STREAM = "hypermindr.kinesisprocessor.aws.kinesisStream";
	private static final String AWS_KINESIS_RAW_STREAM = "hypermindr.kinesisprocessor.aws.kinesisRawStream";
	private static final String AWS_KINESIS_SERVICE_ENDPOINT = "hypermindr.kinesisprocessor.aws.kinesisEndpoint";
	private static final String AWS_KINESIS_CNAME = "hypermindr.kinesisprocessor.aws.kinesisCname";
	private static final String AWS_KINESIS_LOCATION = "hypermindr.kinesisprocessor.aws.kinesisLocation";
	private static final String AWS_KINESIS_LOG_PREFIX = "hypermindr.kinesisprocessor.logfile.";
	private static final String AWS_KINESIS_PARTITION_KEY = "hypermindr.kinesisprocessor.aws.kinesisPartitionKey";

	private static final String AWS_DYNAMO_TABLE = "hypermindr.kinesisprocessor.aws.dinamoTable";
	private static final String MONGO_DB_URL = "hypermindr.kinesisprocessor.mongo.url";
	private static final String MONGO_DB_PORT = "hypermindr.kinesisprocessor.mongo.port";
	private static final String MONGO_DB_NAME = "hypermindr.kinesisprocessor.mongo.db";
	private static final String MONGO_DB_COLLECTION = "hypermindr.kinesisprocessor.mongo.collection";
	private static final String MONGO_DB_USER = "hypermindr.kinesisprocessor.mongo.user";
	private static final String MONGO_DB_PASSWD = "hypermindr.kinesisprocessor.mongo.passwd";

	private static final String DEFAULT_LOG_FILE = "aggregatedLogFile.log";
	
	public static KinesisProcessorProperties getInstance() {

		if (instance == null) {
			instance = new KinesisProcessorProperties();
		}
		return instance;

	}

	public void prepare(File configFile) {

		Properties prop = new Properties();
		FileReader reader;
		try {
			reader = new FileReader(configFile);
			if (reader != null) {
				prop.load(reader);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		try{
			tailer_buffer_size = Integer.parseInt(prop.getProperty(TAILER_BUFFER_SIZE).trim());
		}catch(Exception nfe){
			nfe.printStackTrace();
			tailer_buffer_size = TAILER_BUFFER_PRESIZE;
			log.warn("Tailer buffer size set to default: "+TAILER_BUFFER_PRESIZE);
		}
		
		partner = prop.getProperty(PARTNER);
		raw_file = prop.getProperty(RAW_FILE);
		aws_skey_propertie = prop.getProperty(AWS_SKEY);
		aws_akey_propertie = prop.getProperty(AWS_AKEY);
		aws_kinesis_stream_propertie = prop.getProperty(AWS_KINESIS_STREAM);
		aws_kinesis_raw_stream_propertie = prop
				.getProperty(AWS_KINESIS_RAW_STREAM);
		aws_dynamo_table_propertie = prop.getProperty(AWS_DYNAMO_TABLE);
		mongo_db_url_propertie = prop.getProperty(MONGO_DB_URL);
		mongo_db_port_propertie = prop.getProperty(MONGO_DB_PORT);
		mongo_db_name_propertie = prop.getProperty(MONGO_DB_NAME);
		mongo_db_collection_propertie = prop.getProperty(MONGO_DB_COLLECTION);
		mongo_db_user_propertie = prop.getProperty(MONGO_DB_USER);
		mongo_db_passwd_propertie = prop.getProperty(MONGO_DB_PASSWD);
		aws_kinesis_endpoint_propertie = prop
				.getProperty(AWS_KINESIS_SERVICE_ENDPOINT);
		aws_kinesis_cname_propertie = prop.getProperty(AWS_KINESIS_CNAME);
		aws_kinesis_location_propertie = prop.getProperty(AWS_KINESIS_LOCATION);
		aws_kinesis_partition_key_propertie = prop
				.getProperty(AWS_KINESIS_PARTITION_KEY);

		try {
			aws_kinesis_debug_flag = Boolean.parseBoolean(prop
					.getProperty(DEBUG_FLAG));
			aws_raw_enabled = Boolean.parseBoolean(prop
					.getProperty(RAW_ENABLED));
		} catch (Exception ex) {
			aws_kinesis_debug_flag = false;
			aws_raw_enabled = false;
		}
		if (aws_kinesis_debug_flag) {
			log.info("Debug enabled");
		}
		Set<?> keys = prop.keySet();
		String tempStr;
		for (Object object : keys) {
			if (object instanceof String) {
				tempStr = (String) object;
				if (tempStr.contains(AWS_KINESIS_LOG_PREFIX)) {
					processor_log_files.add(prop.getProperty(tempStr));
				}
				
			}
		}
		prepared = true;
	
	}

	private Integer tailer_buffer_size;
	private boolean aws_kinesis_debug_flag;
	private boolean aws_raw_enabled;
	private String partner;
	private String raw_file;
	private String aws_skey_propertie;
	private String aws_akey_propertie;
	private String aws_kinesis_stream_propertie;
	private String aws_kinesis_raw_stream_propertie;
	private String aws_kinesis_endpoint_propertie;
	private String aws_kinesis_cname_propertie;
	private String aws_kinesis_location_propertie;
	private String aws_kinesis_partition_key_propertie;
	private String aws_dynamo_table_propertie;
	private String mongo_db_url_propertie;
	private String mongo_db_port_propertie;
	private String mongo_db_name_propertie;
	private String mongo_db_collection_propertie;
	private String mongo_db_user_propertie;
	private String mongo_db_passwd_propertie;
	private List<String> processor_log_files = new ArrayList<String>();
	private List<String> envSettings = new ArrayList<String>();

	private static boolean prepared = false;

	public String getAwsSkey() {
		return aws_skey_propertie;
	}

	public String getAwsAkey() {
		return aws_akey_propertie;
	}

	public String getAwsKinesisStream() {
		return aws_kinesis_stream_propertie;
	}

	public String getAwsDynamoTable() {
		return aws_dynamo_table_propertie;
	}

	public String getMongoDbUrl() {
		return mongo_db_url_propertie;
	}

	public String getMongoDbPort() {
		return mongo_db_port_propertie;
	}

	public String getMongoDbUser() {
		return mongo_db_user_propertie;
	}

	public String getMongoDbPasswd() {
		return mongo_db_passwd_propertie;
	}

	public String getMongoDbName() {
		return mongo_db_name_propertie;
	}

	public String getMongoDbCollection() {
		return mongo_db_collection_propertie;
	}

	public String getServiceEndpoint() {
		return aws_kinesis_endpoint_propertie;
	}

	public String getServiceCanonicalName() {
		return aws_kinesis_cname_propertie;
	}

	public String getServiceLocation() {
		return aws_kinesis_location_propertie;
	}

	public List<String> getLogFiles() {
		return processor_log_files;
	}

	/*
	 * public String getAwsKinesisPartitionKey() { return
	 * aws_kinesis_partition_key_propertie; }
	 */

	public boolean getDebugFlag() {

		return aws_kinesis_debug_flag;
	}

	public String getAwsKinesisRawStream() {
		return aws_kinesis_raw_stream_propertie;
	}

	public boolean getAwsRawEnabled() {
		return aws_raw_enabled;
	}

	public String getRawFile() {
		return raw_file;
	}

	public String getDefaultRawFile() {
		return DEFAULT_LOG_FILE;
	}

	public String getPartner() {
		return partner;
	}

	public void setPartner(String partner) {
		this.partner = partner;
	}
	
	public int getTailerBufferSize() {
		return tailer_buffer_size;
	}

}
