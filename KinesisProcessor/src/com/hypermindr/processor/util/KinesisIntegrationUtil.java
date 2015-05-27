package com.hypermindr.processor.util;

import java.io.File;
import java.net.URISyntaxException;
import java.util.Date;

/**
 * Helper class to identify performance records (ugly, but fast(?), way)
 * @author ricardo
 *
 */
public class KinesisIntegrationUtil {

	public static final String TRACERID_STR = '"'+"tracerid"+'"'+":";
	public static final String CLASSNAME_STR ='"'+"class_name"+'"'+":";;
	public static final String ENDPOINT_STR = '"'+"endpoint"+'"'+":";;
	public static final String SERVER_STR = '"'+"server"+'"'+":";;
	public static final String STAGE_STR = '"'+"stage"+'"'+":";;
	public static final String AWS_STR = "AWS";
	public static final String OPEN_BRACKET = "{";
	public static final String CLOSE_BRACKET = "}";
	
	public static boolean isPerformanceLine(String line) {
		return line.contains(CLASSNAME_STR) && line.contains(OPEN_BRACKET) && line.contains(CLOSE_BRACKET) && line.contains(STAGE_STR)
				&& line.contains(SERVER_STR) && line.contains(TRACERID_STR)
				&& line.contains(ENDPOINT_STR) && !line.contains(AWS_STR);
	}
	
	public static String aboutIt() {
		try {
			File jarFile = new File
			(KinesisIntegrationUtil.class.getProtectionDomain().getCodeSource().getLocation().toURI());
			return new Date(jarFile.lastModified()).toString();
		} catch (URISyntaxException e1) {
		}
		return "";
	}
}
