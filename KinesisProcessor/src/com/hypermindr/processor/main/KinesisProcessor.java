package com.hypermindr.processor.main;

import java.io.File;
import java.util.logging.LogManager;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.hypermindr.processor.KinesisRawDownloader;
import com.hypermindr.processor.KinesisDownloader;
import com.hypermindr.processor.ProcessorFeatureSet;
import com.hypermindr.processor.util.KinesisIntegrationUtil;
import com.hypermindr.processor.util.KinesisProcessorProperties;

/**
 * Class to initialize a Processor to read data from Kinesis and persist into a
 * MongoDB.
 * 
 * @author ricardo
 * 
 */
public class KinesisProcessor {

	public static boolean dynamoIntegration = false;
	
	private static File configFile = null;
	
	private static Logger log = Logger.getLogger(KinesisProcessor.class.getName());

	/**
	 * Main method - Receive config file as parameter, a boolean to indicate
	 * Dynamo integration
	 * 
	 * This method launches a Processor instance, which will read from all streams and shards in configuration.
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length == 0) {
			log.fatal("Config file is a mandatory argument");
			System.exit(-1);
		} else {
			configFile = new File(args[0]);
			if (!configFile.exists()) {
				log.debug("Config file not found");
				System.exit(-2);
			}
		}
		if (args[1] != null) {
			try {
				dynamoIntegration = Boolean.parseBoolean(args[1]);
				if (dynamoIntegration) {
					log.warn("Dynamo integration on-line");
				} else {
					log.warn("Dynamo integration off-line");
				}
			} catch (Exception e) {
				log.debug("Invalid dynamo parameter");
				System.exit(-3);
			}

			log.warn("Build date: "+KinesisIntegrationUtil.aboutIt());
			log.warn("Features on this version:\n"+ProcessorFeatureSet.getFeatures());
			runProcessor();
		}
		
	}

	/**
	 * Start the KinesisRunner, leaving the business rules to a thread.
	 * 
	 * @param configFile
	 */

	private static void runProcessor() {
		KinesisProcessor.setup(configFile);
		Thread tDownloader = new Thread(new KinesisDownloader(),"KinesisDownloader");
		tDownloader.start();
		if (KinesisProcessorProperties.getInstance().getAwsRawEnabled()) {
			Thread tRawDownloader = new Thread(new KinesisRawDownloader(),"KinesisRawDownloader");
			tRawDownloader.start();
		}

	}

	private static void setup(File configFile) {
		KinesisProcessorProperties.getInstance().prepare(configFile);

	}

}
