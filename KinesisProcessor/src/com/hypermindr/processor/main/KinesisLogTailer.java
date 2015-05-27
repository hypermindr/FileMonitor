package com.hypermindr.processor.main;

import java.io.File;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient;
import com.hypermindr.processor.TailerFeatureSet;
import com.hypermindr.processor.tailer.TailerContext;
import com.hypermindr.processor.util.KinesisIntegrationUtil;
import com.hypermindr.processor.util.KinesisProcessorProperties;
import com.hypermindr.processor.util.TailerConfiguration;
import com.hypermindr.processor.util.TailerContextBuilder;

/**
 * Class to initialize a Tailer for each log file registered in config file
 * 
 * @author ricardo
 * 
 */
public class KinesisLogTailer {

	private static String accessKey = null;
	private static String secretKey = null;

	private static AWSCredentials credentials = null;
	
	private static AWSCredentialsProvider credentialsProvider = null;

	private static AmazonKinesisAsyncClient kinesisClient = null;

	private static Logger log = LogManager.getLogger(KinesisLogTailer.class
			.getName());

	private static File xmlFile = null;

	public static AmazonKinesisAsyncClient getKinesisClient() {
		return kinesisClient;
	}

	public static int THREAD_COUNT = 0;

	public static Logger getLog() {
		return log;
	}

	/**
	 * Main method - Receive config file as parameter
	 * This method launches a LogTailer instance, which will monitor log files according to configuration.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		Thread.currentThread().setName("Main Tailer");
		Thread.currentThread().setPriority(Thread.MIN_PRIORITY);
		try {
			log.info("Starting Tailer...");
			if (args != null && args.length > 0) {
				
				xmlFile = new File(args[0]); 
			} else {
				log.warn("Config file argument not found");
				System.exit(-2);
			}
			if (!xmlFile.exists()) {
				log.warn("Config file not found");
				System.exit(-2);
			}
			TailerConfiguration tailerConfig = null;
			try{
				tailerConfig = TailerContextBuilder.getInstance().getConfig(xmlFile);
			}catch(Exception e) {
				log.warn("Unable to extract Tailer basic configuration from config file. Shutting down.");
				System.exit(-2);
			}
			accessKey = tailerConfig.getAwsaccesskey();
			secretKey = tailerConfig.getAwssecretkey();
			credentials = new BasicAWSCredentials(accessKey, secretKey);
			
			credentialsProvider = new AWSCredentialsProvider() {
				
				@Override
				public void refresh() {

				}
				
				@Override
				public AWSCredentials getCredentials() {
					return credentials;
				}
			};
			ClientConfiguration config = new ClientConfiguration();

			config.setMaxConnections(20);
			config.setUseGzip(true);
			config.setUseReaper(false);
			kinesisClient = new AmazonKinesisAsyncClient(credentialsProvider,config);
			kinesisClient.setEndpoint(tailerConfig
					.getAwskinesisEndpoint(), tailerConfig.getAwskinesisCname(),
					tailerConfig.getAwskinesisLocation());
		
			List<TailerContext> contexts = KinesisLogTailer.setup(xmlFile);
			if (contexts.size() == 0) {
				log.warn("No environments found. Shutting down.");
				System.exit(-2);
			}
			for (TailerContext tailerContext : contexts) {
				tailerContext.initContext();
			}
			
			log.warn("Build date: "+KinesisIntegrationUtil.aboutIt());
			log.warn("Features on this version:\n"+TailerFeatureSet.getFeatures());
			log.info("Running");
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-3);
		}
	}

	private static List<TailerContext> setup(File xml) {
		return TailerContextBuilder.getInstance().init(xml);
		
	}

	public static AWSCredentials getCredentials() {
		return credentials;
	}

	public static void setCredentials(AWSCredentials credentials) {
		KinesisLogTailer.credentials = credentials;
	}

}
