package training;

import java.io.File;
import java.util.Date;

public class CascadeSVMPathHelper {
	/**
	 * ${workDir}/subset.1.0
	 * @param dir
	 * @param iter
	 * @param id
	 * @return
	 */
	public static String getIdListPath(String dir, int iter, int id) {
		return dir + "/subset." + Integer.toString(iter) + "." + Integer.toString(id);
	}
	
	/**
	 * @param dir
	 * @return
	 */
	// All methods with Date should be called only once. 
	// Singleton
	public static String getSubsetListPath(String dir) {
		Date d = new Date();
		return dir + "/subset.list." + Long.toString(d.getTime());
	}
	
	public static String getModelPath(String dir, int iter, int id) {
		return dir + "/model." + Integer.toString(iter) + "." + Integer.toString(id);
	}
	
	public 	static String getSVPath(String dir, int iter, int id) {
		return dir + "/SV." + Integer.toString(iter) + "." + Integer.toString(id);
	}
	
	public static String getLDPath(String dir, int iter, int id) {
		return dir + "/LD." + Integer.toString(iter) + "." + Integer.toString(id);
	}
	
	public static String getSchedulerParameterPath(String dir) {
		Date d = new Date();
		return dir + "/scheduler.parameter." + Long.toString(d.getTime()); 
	}
	
	public static String getNodeParameterPath(String dir) {
		Date d = new Date();
		return dir + "/node.parameter." + Long.toString(d.getTime());
	}
	
	public static String getLocalWorkDir() {
		String path = "./tmp";
		File workDir = new File(path);
		if (!workDir.exists()) {
			workDir.mkdir();
		}
		return(path);
	}
}
