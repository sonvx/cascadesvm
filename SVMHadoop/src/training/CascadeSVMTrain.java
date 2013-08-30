package training;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.lib.NullOutputFormat;

public class CascadeSVMTrain {
	private static String runPartitionerJob(CascadeSVMTrainParameter parameter) throws IOException {
		JobConf conf = new JobConf(CascadeSVMPartitioner.class);
		conf.setJobName("CascadeSVM Partitioner");
		conf.set("nSubset", Integer.toString(parameter.nSubset));
		String subsetListPath = CascadeSVMPathHelper.getSubsetListPath(CascadeSVMPathHelper.getHadoopWorkDir());
		conf.set("subsetListPath", subsetListPath);
		
		FileInputFormat.addInputPath(conf, new Path(parameter.idlistPath));
		
		conf.setMapperClass(CascadeSVMPartitioner.class);
		conf.setReducerClass(CascadeSVMPartitioner.class);
		
		conf.setOutputFormat(NullOutputFormat.class);
		
		JobClient.runJob(conf);
		return subsetListPath;
	}
	
	/**
	 * @param trainParameter
	 * @param subsetListPath
	 * @return schedulerParameterPath
	 * @throws IOException 
	 */
	private static String writeSchedulerParameter(CascadeSVMTrainParameter trainParameter, String subsetListPath) throws IOException {
		CascadeSVMSchedulerParameter schedulerParameter = new CascadeSVMSchedulerParameter(trainParameter);
		schedulerParameter.iterationId = 0;
		schedulerParameter.lastLD = 0;
		schedulerParameter.lastSVPath = "";
		schedulerParameter.subsetListPath = subsetListPath;
		
		String workDir = CascadeSVMPathHelper.getHadoopWorkDir();
		String schedulerParameterPath = CascadeSVMPathHelper.getSchedulerParameterPath(workDir);
		CascadeSVMIOHelper.writeSchedulerParameterHadoop(schedulerParameterPath, schedulerParameter);
		return schedulerParameterPath;
	}
	
	public static void main(String[] args) {
		try {
			CascadeSVMTrainParameter parameter = CascadeSVMTrainParameter.parseArgs(args);
			// subsetListPath = runPartitionerJob(parameter);
			String subsetListPath = CascadeSVMPartitioner.partitionIdListHadoop(parameter);
			String schedulerParameterPath = writeSchedulerParameter(parameter, subsetListPath);
			CascadeSVMScheduler.runSchedulerJob(schedulerParameterPath);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
}
