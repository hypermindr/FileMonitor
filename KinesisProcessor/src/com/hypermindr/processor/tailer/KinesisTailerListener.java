package com.hypermindr.processor.tailer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListener;
import org.apache.log4j.Logger;

import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.google.gson.Gson;
import com.hypermindr.processor.KinesisRawUploader;
import com.hypermindr.processor.KinesisUploader;
import com.hypermindr.processor.main.KinesisLogTailer;
import com.hypermindr.processor.model.PerformanceLineModel;
import com.hypermindr.processor.util.KinesisIntegrationUtil;


/**
 * Custom tail listener for Hypermindr API / Barbante log files
 * This class is instantiated for each monitored file, handling every line wrote of file. 
 * Those lines are uploaded to Kinesis using the proper Stream (performance or raw)
 * 
 * @author ricardo
 * 
 */
public class KinesisTailerListener implements TailerListener {

	private KinesisUploader k = new KinesisUploader();
	private KinesisRawUploader kraw = new KinesisRawUploader();
	private static Gson gson = new Gson();

	private int MAX_RECORDS_SIZE = 400;

	private ArrayList<PutRecordsRequestEntry> listItens = new ArrayList<PutRecordsRequestEntry>();
	private ArrayList<PutRecordsRequestEntry> listRawItens = new ArrayList<PutRecordsRequestEntry>();

	private boolean debug = false;

	private boolean checkPerformanceSintaxModel = false;

	private boolean rawEnabled = true;

	private static Logger log = Logger.getLogger(KinesisTailerListener.class
			.getName());

	private long lastTimeFired = System.currentTimeMillis();

	private long lastTimeFiredRaw = System.currentTimeMillis();

	private long TIME_FRAME = 60000;

	private String awsKinesisStream;

	private String awsKinesisRawStream;

	public KinesisTailerListener(TailerContext context) {
		log.info("Building KinesisTailerListener. Context: "
				+ context.getName());
		k.setKinesisClient(KinesisLogTailer.getKinesisClient());
		kraw.setKinesisClient(KinesisLogTailer.getKinesisClient());
		log.info("KinesisTailerListener construction complete.");
		awsKinesisStream = context.getPerformanceStreamName();
		awsKinesisRawStream = context.getRawStreamName();
		try {
			rawEnabled = Boolean.parseBoolean(context.getRawEnabled().trim());
		} catch (Exception e) {
		}

		try {
			MAX_RECORDS_SIZE = Integer.parseInt(context.getTailerBufferSize()
					.trim());
		} catch (NumberFormatException nfe) {
		}
		
		try {
			debug =  Boolean.parseBoolean(context.isDebug());
		} catch (NumberFormatException nfe) {
		}
		k.setDebug(debug);
		kraw.setDebug(debug);

	}

	@Override
	public void fileNotFound() {
		// TODO Auto-generated method stub

	}

	@Override
	public void fileRotated() {
		// TODO Auto-generated method stub

	}

	/**
	 * Method to deal with each captured line. TODO: Enhance valid log line
	 * detection
	 */
	@Override
	public void handle(String line) {
		String rawLine = new String(line);
		line = line.trim();
		if (line != null && line.length() > 0) {
			//CHECK IF INCOMMING LINE IS A PERFORMANCE LINE
			if (KinesisIntegrationUtil.isPerformanceLine(line)) {
				int openBracketIndex = line
						.indexOf(KinesisIntegrationUtil.OPEN_BRACKET);
				if (openBracketIndex != -1) {
					line = line
							.substring(
									openBracketIndex,
									line.lastIndexOf(KinesisIntegrationUtil.CLOSE_BRACKET) + 1);

					try {
						if (checkPerformanceSintaxModel) {
							PerformanceLineModel model = gson.fromJson(line,
									PerformanceLineModel.class);
							if (debug) {
								log.trace(model);
							}
						}
						// Partition Key must be tracerid to keep pairing and
						// ordering records
						PutRecordsRequestEntry putRecordsRequestEntry = new PutRecordsRequestEntry();
						putRecordsRequestEntry.setData(ByteBuffer.wrap(line
								.getBytes()));
						putRecordsRequestEntry
								.setPartitionKey(returnTracerId(line));
						listItens.add(putRecordsRequestEntry);

						// Enrich raw with performance information
						PutRecordsRequestEntry putRecordsRequestEntryRaw = new PutRecordsRequestEntry();
						putRecordsRequestEntryRaw.setData(ByteBuffer
								.wrap(rawLine.getBytes()));
						putRecordsRequestEntryRaw.setPartitionKey(String
								.valueOf(System.currentTimeMillis()));
						listRawItens.add(putRecordsRequestEntryRaw);

						if (listItens.size() >= MAX_RECORDS_SIZE
								|| (lastTimeFired + TIME_FRAME) < System
										.currentTimeMillis()) {

							if (listItens.size() > 0) {
								if (debug) {
									log.debug("About to send PERFORMANCE lines to kinesis.");
								}
								k.handle(new PutRecordsRequest().withRecords(
										listItens).withStreamName(
										this.getAwsKinesisStream()));
								listItens.clear();
								listItens.trimToSize();
								System.gc();
								lastTimeFired = System.currentTimeMillis();
							} else {
								if (debug) {
									log.debug("No records to send.");
								}
							}
						}

					} catch (com.google.gson.JsonSyntaxException ex) {

						PutRecordsRequestEntry putRecordsRequestEntry = new PutRecordsRequestEntry();
						putRecordsRequestEntry.setData(ByteBuffer.wrap(line
								.getBytes()));
						putRecordsRequestEntry.setPartitionKey(String
								.valueOf(System.currentTimeMillis()));
						listRawItens.add(putRecordsRequestEntry);

						System.gc();

						return;
					}
				}

			} else if (!line.contains(KinesisIntegrationUtil.AWS_STR) && this.isRawEnabled()) {

				PutRecordsRequestEntry putRecordsRequestEntry = new PutRecordsRequestEntry();
				putRecordsRequestEntry
						.setData(ByteBuffer.wrap(line.getBytes()));
				putRecordsRequestEntry.setPartitionKey(String.valueOf(System
						.currentTimeMillis()));
				listRawItens.add(putRecordsRequestEntry);

				if (listRawItens.size() >= MAX_RECORDS_SIZE
						|| (lastTimeFiredRaw + TIME_FRAME) < System
								.currentTimeMillis()) {
					if (listItens.size() > 0) {
						if (debug) {
							log.debug("About to send RAW lines to kinesis.");
						}
						kraw.handle(new PutRecordsRequest().withRecords(
								listRawItens).withStreamName(
								this.getAwsKinesisRawStream()));
						listRawItens.clear();
						System.gc();
						lastTimeFiredRaw = System.currentTimeMillis();

					} else {
						if (debug) {
							log.debug("No records to send.");
						}
					}
				}
			}
		}

	}

	@Override
	public void handle(Exception arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void init(Tailer arg0) {
		// TODO Auto-generated method stub

	}

	// Naive method, but fast for the job
	public String returnTracerId(String line) {
		int mark = line.indexOf("tracerid");
		return line.substring(mark + 11, mark + 43);
	}

	public String getAwsKinesisStream() {
		return awsKinesisStream;
	}

	public void setAwsKinesisStream(String awsKinesisStream) {
		this.awsKinesisStream = awsKinesisStream;
	}

	public String getAwsKinesisRawStream() {
		return awsKinesisRawStream;
	}

	public void setAwsKinesisRawStream(String awsKinesisRawStream) {
		this.awsKinesisRawStream = awsKinesisRawStream;
	}

	public boolean isRawEnabled() {
		return rawEnabled;
	}

	public void setRawEnabled(boolean rawEnabled) {
		this.rawEnabled = rawEnabled;
	}
	
	

}
