package com.hypermindr.processor.tailer;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListener;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.hypermindr.processor.main.KinesisLogTailer;

/**
 * Class that holds a Context for Tailer. A context enables Tailer to processes data for just one tenant, with proper isolation. 
 * Tailer may have many contexts during runtime, and each context coexists among other ones. 
 * @author ricardo
 *
 */
public class TailerContext {
	
	private static Logger log = LogManager.getLogger(TailerContext.class
			.getName());
	
	private String name;
	
	private String[] logFilesNames;
	
	private ArrayList<String> logFilesNamesNotFound;
	
	private String performanceStreamName;
	
	private String rawStreamName;
	
	private String rawEnabled;
	
	private String tailerBufferSize;
	
	private String debug;
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String[] getLogFilesNames() {
		return logFilesNames;
	}

	public void setLogFilesNames(String[] logFilesNames) {
		this.logFilesNames = logFilesNames;
	}

	public String getPerformanceStreamName() {
		return performanceStreamName;
	}

	public void setPerformanceStreamName(String performanceStreamName) {
		this.performanceStreamName = performanceStreamName;
	}

	public String getRawStreamName() {
		return rawStreamName;
	}

	public void setRawStreamName(String rawStreamName) {
		this.rawStreamName = rawStreamName;
	}

	public String getRawEnabled() {
		return rawEnabled;
	}

	public void setRawEnabled(String rawEnabled) {
		this.rawEnabled = rawEnabled;
	}

	public String getTailerBufferSize() {
		return tailerBufferSize;
	}

	public void setTailerBufferSize(String tailerBufferSize) {
		this.tailerBufferSize = tailerBufferSize;
	}
	

	public String isDebug() {
		return debug;
	}

	public void setDebug(String debug) {
		this.debug = debug;
	}

	@Override
	public String toString() {
		return "TailerContext [name=" + name + ", logFilesNames="
				+ Arrays.toString(logFilesNames) + ", performanceStreamName="
				+ performanceStreamName + ", rawStreamName=" + rawStreamName
				+ ", rawEnabled=" + rawEnabled + ", tailerBufferSize="
				+ tailerBufferSize + "]";
	}
	
	/**
	 * Initialize context, raising Tailers for each file. 
	 */
	public void initContext() {
		log.info("###Init context: "+this.getName());
		log.info("Setting up files. " + logFilesNames.length + " files in context.");
		if (logFilesNames != null && logFilesNames.length > 0) {
			log.info("Rising tailers for files on context: "+this.getName());
			for (String fileName : logFilesNames) {
				log.info("Checking " + fileName + " in fs.");
				File currenteTailedFile = new File(fileName);
				log.info("Checking " + currenteTailedFile.getName()
						+ " . Do exists?");
				if (currenteTailedFile.exists()) {
					log.info("File " + currenteTailedFile.getName()
							+ " exists. Proceed.");
					TailerListener listener = new KinesisTailerListener(this);
					log.info("TailerListener created.");
					Tailer tailer = new Tailer(currenteTailedFile,

							listener, 300, true);

					log.info("Tailer created. Creating new Thread for it...");
					Thread T = new Thread(tailer, this.getName()+":"+fileName);
					log.info("Starting for: " + fileName+"...");
					T.start();
					log.info("Started tailer for: " + fileName);
				} else {
					log.warn("File " + fileName + " not found.");
				}
			}
		}
		
	}
	

	
}
