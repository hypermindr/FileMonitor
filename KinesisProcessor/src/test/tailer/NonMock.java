package test.tailer;

import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.LongSerializationPolicy;
import com.hypermindr.processor.model.PerformanceLineModel;
import com.hypermindr.processor.model.PerformanceLineRecord;
import com.hypermindr.processor.model.Stage;
import com.hypermindr.processor.nosql.MongoConnector;

public class NonMock {

	public static void main(String[] args) throws ExecutionException {
		
		ArrayList<String> list = new ArrayList<String>();
		LoadingCache<String, PerformanceLineModel>cache = CacheBuilder
				.newBuilder().expireAfterWrite(1, TimeUnit.DAYS)
				.build(new CacheLoader<String, PerformanceLineModel>() {
					public PerformanceLineModel load(String key) throws Exception {
						return null;
						
					}
				});
		
		GsonBuilder gsonBuilder = new GsonBuilder();
		gsonBuilder.serializeSpecialFloatingPointValues();
		Gson gson = gsonBuilder.create();
		PerformanceLineModel model1 = new PerformanceLineModel();
		model1.setClass_name("class");
		model1.setEndpoint("track_activity");
		model1.setTracerid("99a2dbed2b36c56e71ab6b3b4a798bd6");
		model1.setModule_name("module");
		model1.setTimestamp(1426817421.641);
							
		model1.setMessage("MSG1");
		model1.setServer("phish2");
		model1.setStage("B");
		
		
		
		PerformanceLineModel model2 = new PerformanceLineModel();
		model2.setClass_name("class");
		model2.setEndpoint("track_activity");
		model2.setTracerid("99a2dbed2b36c56e71ab6b3b4a798bd6");
		model2.setModule_name("module");
		model2.setTimestamp(1426817421.733);
		model2.setMessage("MSG2");
		model2.setServer("phish2");
		model2.setStage("E");
		
		
		if (model1.getStage() == Stage.BEGIN.getType()) {
			cache.put(model1.getPairedID(), model1);
			cache.put(model1.getPairedID(), model1);
		}
		
		if (model2.getStage() == Stage.END.getType()) {
			PerformanceLineModel pairModel = cache.getIfPresent(model1.getPairedID());
			if (pairModel != null) {
				PerformanceLineRecord record = new PerformanceLineRecord(pairModel, model2.getTimestamp(), pairModel.getTimestamp(),new String[]{model1.getMessage(),model2.getMessage()});
				System.out.println(gson.toJson(record));
				list.add(gson.toJson(record));
				cache.invalidate(model1.getPairedID());
				PerformanceLineModel pairModel2 = cache.getIfPresent(model1.getPairedID());
				if (pairModel2 == null)
					System.out.println("is null");
			}
		}

		//MongoConnector.savePerformanceLog(list, "Logs");
	}

}
