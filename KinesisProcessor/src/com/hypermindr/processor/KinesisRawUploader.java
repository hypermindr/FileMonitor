package com.hypermindr.processor;

import java.util.Calendar;
import java.util.concurrent.Future;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.hypermindr.processor.util.KinesisProcessorProperties;

/**
 * Simple way to send a log line to Kinesis
 * 
 * @author ricardo
 * 
 */
public class KinesisRawUploader {

	private AmazonKinesisAsyncClient kinesisClient;

	public AmazonKinesisAsyncClient getKinesisClient() {
		return kinesisClient;
	}

	public void setKinesisClient(AmazonKinesisAsyncClient kinesisClient) {
		this.kinesisClient = kinesisClient;
	}

	private boolean debug = false;

	private static Logger logger = LogManager
			.getLogger(KinesisRawUploader.class.getName());

	/**
	 * Non-blocking method responsible to upload data into Kinesis.
	 * 
	 * @param chunk
	 */
	public boolean handle(final PutRecordsRequest chunk) {
		// Now send data to AWS Kinesis!
		try {
			// TODO: Implement reconnect method
			Future<PutRecordsResult> putRecordsResult = kinesisClient
					.putRecordsAsync(
							chunk,
							new AsyncHandler<PutRecordsRequest, PutRecordsResult>() {

								@Override
								public void onError(Exception e) {
									logger.error(e);
								}

								@Override
								public void onSuccess(
										PutRecordsRequest request,
										PutRecordsResult response) {
									try {
										String seqNum = response
												.getRecords()
												.get(response.getRecords()
														.size() - 1)
												.getSequenceNumber();
										if (debug) {
											logger.debug(Calendar.getInstance().getTime());
											logger.debug("Fail for (raw): "
													+ response
															.getFailedRecordCount());

											logger.debug("SeqNum (raw): "
													+ seqNum);
										}
									} catch (Exception e) {
										logger.error(e);
									}
									System.gc();
									return;

								}
							});
		} catch (Exception e) {
			logger.error(e);
		}

		return true;
	}

	public boolean isDebug() {
		return debug;
	}

	public void setDebug(boolean debug) {
		this.debug = debug;
	}
	
	

}
