package training;

import java.io.File;
import java.io.FileReader;
import java.util.Date;

import org.apache.hadoop.mapred.JobConf;

public class CascadeSVMPathHelper {
	public static String getIdListPath(String dir, int iter, int id) {
		return dir + "/subset." + Integer.toString(iter) + "." + Integer.toString(id);
	}
	
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
	
	public static String getHadoopWorkDir() {
		JobConf conf = new JobConf();
		return conf.get("hadoop.tmp.dir");
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
