package com.hypermindr.processor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.DailyRollingFileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.net.SyslogAppender;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.hypermindr.processor.bean.BeaconRawBean;
import com.hypermindr.processor.bean.LogBean;
import com.hypermindr.processor.nosql.DynamoDBConnector;
import com.hypermindr.processor.util.KinesisProcessorProperties;

/**
 * Main thread responsible to read from Kinesis raw data and save into
 * filesystem.
 * 
 * Raw data are every data logged from a component, system or even a subsystem.
 * All this data are stored into a consolidated file, hourly rotated.
 * 
 * The data is also sent to local syslogd, enabling integration with loggly,
 * splunk and other Log helpers.
 * 
 * @author ricardo
 * 
 */
public class KinesisRawDownloader implements Runnable {

	private static String accessKey = KinesisProcessorProperties.getInstance()
			.getAwsAkey();
	private static String secretKey = KinesisProcessorProperties.getInstance()
			.getAwsSkey();

	public static AWSCredentials credentials;

	private boolean debug = KinesisProcessorProperties.getInstance()
			.getDebugFlag();

	private static Logger log = Logger.getLogger(KinesisRawDownloader.class
			.getName());

	public static AmazonKinesisClient kinesisClient;

	private boolean runOnce = false;

	public AmazonKinesisClient getKinesisClient() {
		return kinesisClient;
	}

	public void setKinesisClient(AmazonKinesisClient kinesisClient) {
		KinesisRawDownloader.kinesisClient = kinesisClient;
	}

	public AWSCredentials getCredentials() {
		return credentials;
	}

	public void setCredentials(AWSCredentials credentials) {
		KinesisRawDownloader.credentials = credentials;
	}

	public boolean isRunOnce() {
		return runOnce;
	}

	public void setRunOnce(boolean runOnce) {
		this.runOnce = runOnce;
	}

	private DailyRollingFileAppender rollingAppender;

	private SyslogAppender syslogAppender;

	private Logger processorLogger;

	@Override
	public void run() {

		if (credentials == null) {
			credentials = new BasicAWSCredentials(accessKey, secretKey);
		}
		if (kinesisClient == null) {
			kinesisClient = new AmazonKinesisClient(credentials);
			kinesisClient.setEndpoint(KinesisProcessorProperties.getInstance()
					.getServiceEndpoint(), KinesisProcessorProperties
					.getInstance().getServiceCanonicalName(),
					KinesisProcessorProperties.getInstance()
							.getServiceLocation());
		}

		String fileName = KinesisProcessorProperties.getInstance().getRawFile();
		if (fileName == null || fileName.length() == 0) {
			fileName = KinesisProcessorProperties.getInstance()
					.getDefaultRawFile();
		}
		PatternLayout layout = new PatternLayout();
		// String conversionPattern = "[%p] %d %c %M - %m%n";

		String conversionPattern = "%m%n";

		layout.setConversionPattern(conversionPattern);
		rollingAppender = new DailyRollingFileAppender();
		rollingAppender.setFile(fileName);
		rollingAppender.setDatePattern("'.'yyyy-MM-dd-HH");
		rollingAppender.setLayout(layout);
		rollingAppender.setAppend(true);
		rollingAppender.setImmediateFlush(true);
		rollingAppender.activateOptions();

		syslogAppender = new SyslogAppender();
		syslogAppender.setName("Hypermindr-logger");
		syslogAppender.setLayout(layout);
		syslogAppender.setFacility("LOCAL5");
		syslogAppender.setHeader(true);
		syslogAppender.setSyslogHost("localhost");
		syslogAppender.setThreshold(Level.DEBUG);

		syslogAppender.activateOptions();

		processorLogger = Logger.getLogger("com.processor");
		processorLogger.setLevel(Level.DEBUG);
		processorLogger.setAdditivity(false);
		processorLogger.addAppender(syslogAppender);
		processorLogger.addAppender(rollingAppender);

		List<Shard> shards = new ArrayList<Shard>();
		DescribeStreamRequest describeStreamRequest = null;
		DescribeStreamResult describeStreamResult = null;

		shards.clear();
		describeStreamRequest = new DescribeStreamRequest();
		describeStreamRequest.setStreamName(KinesisProcessorProperties
				.getInstance().getAwsKinesisRawStream());
		describeStreamResult = kinesisClient
				.describeStream(describeStreamRequest);
		shards.addAll(describeStreamResult.getStreamDescription().getShards());
		for (final Shard shard : shards) {

			String id = shard.getShardId();
			RawWorker T = new RawWorker(id);
			T.setDaemon(false);
			T.start();
		}
	}

	private void flushToDisk(final List<String> itensToWrite) {

		if (itensToWrite.size() == 0) {
			return;
		}
		String temp = "";
		for (String s : itensToWrite) {
			temp += s + "\n";
		}
		processorLogger.debug(temp);

	}

	/**
	 * Each shard from Kinesis stream is continously read from an instance of
	 * this thread
	 * 
	 * @author ricardo
	 * 
	 */
	private class RawWorker extends Thread {

		private String id;

		public RawWorker(String id) {
			this.id = id;
			this.setName("Raw-Worker-" + id);
		}

		/**
		 * Run method continuously reads the given shard and process the data.
		 */
		public void run() {
			String seqNumber = null;
			String lastSeqNumber = null;
			String kinesisData = null;
			List<String> itensToWrite = null;
			String shardIterator;
			while (true) {
				try {

					GetRecordsRequest getRecordRequest = new GetRecordsRequest();
					getRecordRequest.setLimit(1000);

					GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
					getShardIteratorRequest
							.setStreamName(KinesisProcessorProperties
									.getInstance().getAwsKinesisRawStream());

					getShardIteratorRequest.setShardId(id);
					if (seqNumber == null) {

						String horizon = DynamoDBConnector.getInstance()
								.getCurrentRawBeacon(id);
						if (horizon != null) {
							getShardIteratorRequest
									.setShardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER);
							getShardIteratorRequest
									.setStartingSequenceNumber(horizon);
						} else {
							getShardIteratorRequest
									.setShardIteratorType(ShardIteratorType.TRIM_HORIZON);
						}
					} else {
						getShardIteratorRequest
								.setShardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER);
						getShardIteratorRequest
								.setStartingSequenceNumber(seqNumber);
					}
					GetShardIteratorResult getShardIteratorResult = null;
					try {
						getShardIteratorResult = kinesisClient
								.getShardIterator(getShardIteratorRequest);
					} catch (com.amazonaws.services.kinesis.model.InvalidArgumentException iae) {
						getShardIteratorRequest.setStartingSequenceNumber(null);
						getShardIteratorRequest
								.setShardIteratorType(ShardIteratorType.TRIM_HORIZON);
						getShardIteratorResult = kinesisClient
								.getShardIterator(getShardIteratorRequest);

					}
					shardIterator = getShardIteratorResult.getShardIterator();
					getRecordRequest.setShardIterator(shardIterator);
					GetRecordsResult getRecordResult = kinesisClient
							.getRecords(getRecordRequest);
					if (debug) {
						log.debug("Raw Get chunk");
					}
					itensToWrite = new ArrayList<String>();

					for (Record iterable_element : getRecordResult.getRecords()) {

						kinesisData = new String(iterable_element.getData()
								.array());
						kinesisData = kinesisData.trim();
						if (debug) {
							log.debug(iterable_element.getSequenceNumber()
									+ " - " + kinesisData);
						}
						seqNumber = iterable_element.getSequenceNumber();
						if (lastSeqNumber == null || lastSeqNumber != seqNumber) {
							if (kinesisData != null && kinesisData.length() > 0) {
								itensToWrite.add(kinesisData + "\n");
							}
							lastSeqNumber = seqNumber;
						} else {
							if (debug)
								log.warn("Duplicated seqNumber received: "
										+ seqNumber);
						}
					}
					if (debug) {
						log.debug("Saving (raw) " + itensToWrite.size()
								+ " itens");
					}
					if (itensToWrite.size() > 0) {
						flushToDisk(new ArrayList<String>(itensToWrite));
						if (seqNumber != null) {
							DynamoDBConnector
									.getInstance()
									.updateKinesisRawBeacon(
											new BeaconRawBean(id, seqNumber,
													System.currentTimeMillis()));
							if (debug) {
								log.debug(">>> Raw beacon updated");

							}
						}

					}

				} catch (Exception e) {
					log.error("Error caught on worker: " + this.getName());
					log.error(e);
					log.warn("Possible loss of log data. Check log file. Last sequence number: "
							+ lastSeqNumber);

				}

				if (runOnce)
					break;
			}
		}
	}

}
