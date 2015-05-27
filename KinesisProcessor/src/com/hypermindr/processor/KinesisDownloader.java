package com.hypermindr.processor;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.cloudfront.model.InvalidArgumentException;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchAsyncClient;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
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
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.gson.Gson;
import com.hypermindr.processor.bean.BeaconBean;
import com.hypermindr.processor.bean.LogBean;
import com.hypermindr.processor.main.KinesisProcessor;
import com.hypermindr.processor.model.PerformanceLineModel;
import com.hypermindr.processor.model.PerformanceLineRecord;
import com.hypermindr.processor.model.Stage;
import com.hypermindr.processor.nosql.DynamoDBConnector;
import com.hypermindr.processor.nosql.MongoConnector;
import com.hypermindr.processor.util.KinesisProcessorProperties;

/**
 * Main thread responsible to read from Kinesis performance data and save into
 * MongoDB and Dynamo(optional)
 * 
 * Performance data are every data used to build the performance database,
 * enabling other application to draw charts based on environment performance
 * 
 * @author ricardo
 * 
 */
public class KinesisDownloader implements Runnable {

	private static String accessKey = KinesisProcessorProperties.getInstance()
			.getAwsAkey();
	private static String secretKey = KinesisProcessorProperties.getInstance()
			.getAwsSkey();

	private static String partner = KinesisProcessorProperties.getInstance()
			.getPartner();
	public static AWSCredentials credentials;

	private static AmazonDynamoDBClient dynamoClient;

	private static DynamoDBMapper mapper = new DynamoDBMapper(dynamoClient);

	private boolean debug = KinesisProcessorProperties.getInstance()
			.getDebugFlag();

	private static Logger log = Logger.getLogger(KinesisDownloader.class
			.getName());

	public static AmazonKinesisClient kinesisClient;

	private boolean runOnce = false;

	public AmazonKinesisClient getKinesisClient() {
		return kinesisClient;
	}

	public void setKinesisClient(AmazonKinesisClient kinesisClient) {
		KinesisDownloader.kinesisClient = kinesisClient;
	}

	public AWSCredentials getCredentials() {
		return credentials;
	}

	public void setCredentials(AWSCredentials credentials) {
		KinesisDownloader.credentials = credentials;
	}

	public boolean isRunOnce() {
		return runOnce;
	}

	public void setRunOnce(boolean runOnce) {
		this.runOnce = runOnce;
	}

	/**
	 * The run method is responsible for list all performance shards from
	 * Kinesis, and fires a @PerformanceWorker to read all data from each shard.
	 */
	@Override
	public void run() {

		if (credentials == null) {
			credentials = new BasicAWSCredentials(accessKey, secretKey);
		}
		dynamoClient = new AmazonDynamoDBClient(credentials);
		if (kinesisClient == null) {
			kinesisClient = new AmazonKinesisClient(credentials);
			kinesisClient.setEndpoint(KinesisProcessorProperties.getInstance()
					.getServiceEndpoint(), KinesisProcessorProperties
					.getInstance().getServiceCanonicalName(),
					KinesisProcessorProperties.getInstance()
							.getServiceLocation());
		}

		List<Shard> shards = new ArrayList<Shard>();
		DescribeStreamRequest describeStreamRequest = null;
		DescribeStreamResult describeStreamResult = null;

		shards.clear();
		describeStreamRequest = new DescribeStreamRequest();
		describeStreamRequest.setStreamName(KinesisProcessorProperties
				.getInstance().getAwsKinesisStream());
		describeStreamResult = kinesisClient
				.describeStream(describeStreamRequest);
		shards.addAll(describeStreamResult.getStreamDescription().getShards());
		for (Shard shard : shards) {

			String id = shard.getShardId();
			PerformanceWorker T = new PerformanceWorker(id);
			T.setDaemon(false);
			T.start();
		}

	}

	@SuppressWarnings("unused")
	private void writeToDynamoDB(List<LogBean> kinesisData) {
		Object[] array = (Object[]) kinesisData.toArray();
		mapper.batchSave(array);

	}

	/**
	 * Each shard from Kinesis stream is continously read from an instance of
	 * this thread
	 * 
	 * @author ricardo
	 * 
	 */
	private class PerformanceWorker extends Thread {

		private String id;
		LoadingCache<String, PerformanceLineModel> cache;

		public PerformanceWorker(String id) {
			this.id = id;
			this.setName("Performance-Worker-" + id);
		}

		/**
		 * Run method continuously reads the given shard and process the data.
		 */
		public void run() {
			Gson gson = new Gson();
			String seqNumber = null;
			String lastSeqNumber = null;
			String kinesisData = null;
			List<String> itensToWrite = new ArrayList<String>();
			List<String> singleLineList = new ArrayList<String>();
			List<PerformanceLineRecord> cloudWatchData = new ArrayList<PerformanceLineRecord>();
			List<String> cloudWatchUniqueMetrics = new ArrayList<String>();

			String shardIterator;
			ClientConfiguration config = new ClientConfiguration();

			config.setMaxConnections(5);
			AWSCredentialsProvider credentialsProvider = new AWSCredentialsProvider() {

				@Override
				public void refresh() {

				}

				@Override
				public AWSCredentials getCredentials() {
					return credentials;
				}
			};

			AmazonCloudWatchAsyncClient cloudWatchClient = new AmazonCloudWatchAsyncClient(
					credentialsProvider, config);
			cache = CacheBuilder.newBuilder()
					.expireAfterWrite(1, TimeUnit.HOURS)
					.build(new CacheLoader<String, PerformanceLineModel>() {
						public PerformanceLineModel load(String key)
								throws Exception {
							// We are not dealing with compute cache, so there
							// is nothing to do,just return
							return null;
						}
					});
			/**
			 * The infinity loop. TODO: Build a mechanism to stop one or more
			 * shard readers
			 */
			while (true) {
				try {
					if (debug) {
						log.debug("Loop - " + this.getName());
					}
					GetRecordsRequest getRecordRequest = new GetRecordsRequest();
					getRecordRequest.setLimit(1000);

					GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
					getShardIteratorRequest
							.setStreamName(KinesisProcessorProperties
									.getInstance().getAwsKinesisStream());

					getShardIteratorRequest.setShardId(id);

					if (seqNumber == null) {
						String horizon = DynamoDBConnector.getInstance()
								.getCurrentBeacon(id);
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
						log.debug("Get chunk");
					}
					itensToWrite.clear();
					singleLineList.clear();
					cloudWatchData.clear();
					cloudWatchUniqueMetrics.clear();
					for (Record iterable_element : getRecordResult.getRecords()) {
						kinesisData = new String(iterable_element.getData()
								.array());
						if (debug) {
							log.debug(this.getName() + "-"
									+ iterable_element.getSequenceNumber()
									+ " - " + kinesisData);
						}
						seqNumber = iterable_element.getSequenceNumber();
						if (lastSeqNumber == null || lastSeqNumber != seqNumber) {

							itensToWrite.add(kinesisData);
							try {
								PerformanceLineModel model = gson
										.fromJson(kinesisData,
												PerformanceLineModel.class);

								if (model.getStage().equals(
										Stage.BEGIN.getType())) {
									cache.put(model.getPairedID(), model);
								} else if (model.getStage().equals(
										Stage.END.getType())) {
									PerformanceLineModel pairModel = cache
											.getIfPresent(model.getPairedID());
									if (pairModel != null) {
										PerformanceLineRecord record = new PerformanceLineRecord(
												pairModel,
												model.getTimestamp(),
												pairModel.getTimestamp(),
												new String[] {
														pairModel.getMessage(),
														model.getMessage() });
										record.setStage(Stage.RECORD.getType());
										if (!cloudWatchUniqueMetrics
												.contains(record
														.getPerformanceDatumKey())) {
											cloudWatchData.add(record);
											cloudWatchUniqueMetrics.add(record
													.getPerformanceDatumKey());
										}
										singleLineList.add(gson.toJson(record));
										cache.invalidate(model.getPairedID());
									}
								}

								// ######################
								// **DYNAMO INTEGRATION**>>>>>>
								// ######################
								if (KinesisProcessor.dynamoIntegration) {
									final LogBean bean = LogBean
											.createFromJson(kinesisData);
									if (bean != null) {
										new Thread(new Runnable() {

											@Override
											public void run() {
												mapper.save(bean);
												if (debug)
													log.debug("Record stored");
												return;
											}
										}).start();
									}
								}

								lastSeqNumber = seqNumber;
							} catch (Exception e) {
								e.printStackTrace();
							}
						} else {
							if (debug)
								System.out
										.println("Duplicated seqNumber received: "
												+ seqNumber);

						}
					}

					// / END FOR
					// / NOW CLOUD WATCH!

					List<MetricDatum> metricDatum = new ArrayList<MetricDatum>();
					PutMetricDataRequest data = new PutMetricDataRequest();
					for (PerformanceLineRecord p : cloudWatchData) {

						MetricDatum datum = new MetricDatum();

						datum.setMetricName(p.getPerformanceDatumKey());
						datum.setValue(p.getTimeElapsed());
						datum.setTimestamp(new Date(new BigDecimal(p
								.getTimestamp() * 1000).longValue()));
						datum.setUnit(StandardUnit.Milliseconds);

						metricDatum.add(datum);

						if (metricDatum.size() >= 19) {
							data.setNamespace("HLogger/" + partner);
							data.setMetricData(metricDatum);
							data.setRequestCredentials(credentials);
							cloudWatchClient
									.putMetricDataAsync(
											data,
											new AsyncHandler<PutMetricDataRequest, Void>() {

												@Override
												public void onSuccess(
														PutMetricDataRequest arg0,
														Void arg1) {
													if (debug)
														log.debug("Info sent to cloudwatch for date "
																+ arg0.getMetricData()
																		.get(0)
																		.getTimestamp());

												}

												@Override
												public void onError(
														Exception arg0) {
													log.error(arg0);

												}
											});
							metricDatum.clear();
						}

					}
					new ArrayList<String>();
					if (metricDatum.size() > 0) {
						data.setNamespace("HLogger/" + partner);
						data.setMetricData(metricDatum);
						data.setRequestCredentials(credentials);
						cloudWatchClient.putMetricDataAsync(data,
								new AsyncHandler<PutMetricDataRequest, Void>() {

									@Override
									public void onSuccess(
											PutMetricDataRequest arg0, Void arg1) {
										if (debug)
											log.debug("Info sent to cloudwatch for date "
													+ arg0.getMetricData()
															.get(0)
															.getTimestamp());

									}

									@Override
									public void onError(Exception arg0) {
										log.error(arg0);

									}
								});
						metricDatum.clear();
					}
					// END CLOUDWATCH
					if (debug) {
						log.debug("Saving " + itensToWrite.size() + " itens");
					}

					String res = MongoConnector.getInstance()
							.savePerformanceLog(
									singleLineList,
									KinesisProcessorProperties.getInstance()
											.getMongoDbCollection());
					if (debug)
						log.debug("Got from Mongo save routine: " + res);

					if (seqNumber != null) {
						DynamoDBConnector.getInstance().updateKinesisBeacon(
								new BeaconBean(id, seqNumber, System
										.currentTimeMillis()));
						if (debug) {
							log.debug(this.getName() + " - Beacon updated");

						}
					}

				} catch (Exception e) {
					e.printStackTrace();
				}

				if (runOnce)
					break;
			}
		}

	}

}
