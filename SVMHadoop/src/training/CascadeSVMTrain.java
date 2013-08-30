package training;

import java.io.IOException;

import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.NullOutputFormat;

public class CascadeSVMTrain {
	private static String subsetListPath;
	
	private static void runPartitionerJob(CascadeSVMTrainParameter parameter) throws IOException {
		JobConf conf1 = new JobConf(CascadeSVMPartitioner.class);
		conf1.setJobName("CascadeSVM Partitioner");
		conf1.set("nSubset", Integer.toString(parameter.nSubset));
		subsetListPath = CascadeSVMPathHelper.getSubsetListPath(CascadeSVMPathHelper.getHadoopWorkDir());
		conf1.set("subsetListPath", subsetListPath);
		
		FileInputFormat.addInputPaths(conf1, parameter.idlistPath);
		
		conf1.setMapperClass(CascadeSVMPartitioner.class);
		conf1.setReducerClass(CascadeSVMPartitioner.class);
		
		conf1.setOutputFormat(NullOutputFormat.class);
		
		JobClient.runJob(conf1);
	}
	
	private static void writeSchedulerParameter(CascadeSVMTrainParameter trainParameter) {
		CascadeSVMSchedulerParameter schedulerParameter = new CascadeSVMSchedulerParameter(trainParameter);
		schedulerParameter.iterationId = 0;
		schedulerParameter.lastLD = 0;
		schedulerParameter.lastSVPath = "";
		schedulerParameter.subsetListPath = subsetListPath;
		
		String schedulerParameterPath = CascadeSVMPathHelper.getSchedulerParameterPath(CascadeSVMPathHelper.getHadoopWorkDir(), 0);
		
	}
	
	private static void runSchedulerJob(CascadeSVMTrainParameter parameter) {}
	
	public static void main(String[] args) {
		try {
			CascadeSVMTrainParameter parameter = CascadeSVMTrainParameter.parseArgs(args);
			runPartitionerJob(parameter);
			writeSchedulerParameter(parameter);
			runSchedulerJob(parameter);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
}
