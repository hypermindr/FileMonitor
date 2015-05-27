package test.processor;

import org.junit.Test;
import org.mockito.Mockito;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.hypermindr.processor.KinesisDownloader;
import com.hypermindr.processor.KinesisRawDownloader;

public class BasicLogicTestCase {

	@Test
	public void testHandleDownloader() {
		AmazonKinesisClient client = Mockito.mock(AmazonKinesisClient.class);
		AWSCredentials credentials = Mockito.mock(BasicAWSCredentials.class);
		KinesisDownloader downloader = new KinesisDownloader();
		downloader.setKinesisClient(client);
		downloader.setCredentials(credentials);
		downloader.setRunOnce(true);
		downloader.run();
	}

	@Test
	public void testHandleRawDownloader() {
		AmazonKinesisClient client = Mockito.mock(AmazonKinesisClient.class);
		AWSCredentials credentials = Mockito.mock(BasicAWSCredentials.class);
		KinesisRawDownloader downloader = new KinesisRawDownloader();
		downloader.setKinesisClient(client);
		downloader.setCredentials(credentials);
		downloader.setRunOnce(true);
		downloader.run();
	}
}
