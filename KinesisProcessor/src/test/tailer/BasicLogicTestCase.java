package test.tailer;

import java.util.ArrayList;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.hypermindr.processor.KinesisRawUploader;
import com.hypermindr.processor.KinesisUploader;
import com.hypermindr.processor.main.KinesisLogTailer;
import com.hypermindr.processor.util.KinesisIntegrationUtil;

public class BasicLogicTestCase {

	private static String regularLine = "Parameters: {'client_id'=>'543ee0317269636334000000', 'apikey'=>'8aeeaea12f0f185821782fdefa455f87bcafcc34b09a2c517cf3546e61befb49', 'user_id'=>'hmrtmpc4-6d15-3fee-7bf2-798705b0c6b9', 'activity'=>'view'} (pid:15951) (###TRACERID:###)";

	private static String performanceLine = "2015-03-05 17:06:59.760 [INFO] {'server':'ricardo-work','tracerid':'c4c6247c0d01ffa3fe2c21547bd75e55','endpoint':'track_activity','timestamp':1425051647.136,'module_name':'V2','class_name':'V2::ApiController','method_name':'track_activity','stage':'B','message':'START'} (pid:15951) (###TRACERID:c4c6247c0d01ffa3fe2c21547bd75e55###)";

	@Test
	public void testIsPerfomanceLine() {
	
		Assert.assertTrue(KinesisIntegrationUtil.isPerformanceLine(performanceLine));
		Assert.assertFalse(KinesisIntegrationUtil.isPerformanceLine(regularLine));
	}
		
	@Test
	public void testIsGeneralLine() {
	
		Assert.assertFalse(KinesisIntegrationUtil.isPerformanceLine(regularLine));
		
	}
	
	
	
	@Test
	public void testHandleUploader() {
		/*AmazonKinesisClient client = Mockito.mock(AmazonKinesisClient.class);
		KinesisUploader uploader = new KinesisUploader();
		uploader.setKinesisClient(client);
		ArrayList<String> chunk = new ArrayList<String>();
		chunk.add(regularLine);*/
		
	}
	
	
	@Test
	public void testHandleRawUploader() {
		AmazonKinesisClient client = Mockito.mock(AmazonKinesisClient.class);
		KinesisRawUploader rawUploader = new KinesisRawUploader();
		ArrayList<String> chunk = new ArrayList<String>();
		chunk.add(regularLine);
		
	
	}
}
