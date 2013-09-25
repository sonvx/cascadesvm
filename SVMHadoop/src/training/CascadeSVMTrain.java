package training;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

public class CascadeSVMTrain {
	public static Logger logger = Logger.getLogger(CascadeSVMTrain.class);
	/**
	 * @param trainParameter
	 * @return schedulerParameterPath
	 * @throws IOException
	 * Write scheduler parameter for first iteration into HDFS.
	 */
	private static String writeSchedulerParameter(CascadeSVMTrainParameter trainParameter) throws IOException {
		logger.info("[BEGIN]writeSchedulerParameter()");
		CascadeSVMSchedulerParameter schedulerParameter = new CascadeSVMSchedulerParameter(trainParameter);
		schedulerParameter.iterationId = 1;
		schedulerParameter.lastLD = 0;
		schedulerParameter.lastSVPath = CascadeSVMSchedulerParameter.NULL_LIST_PATH;
		
		String schedulerParameterPath = CascadeSVMPathHelper.getSchedulerParameterPath(trainParameter.workDir);
		CascadeSVMIOHelper.writeSchedulerParameterHadoop(schedulerParameterPath, schedulerParameter);
		logger.info(schedulerParameter.toString());
		logger.info("[END]writeSchedulerParameter()");
		return schedulerParameterPath;
	}
	
	/**
	 * @param args
	 * The main function for user.
	 * 1. Create a CascadeSVMTrainParameter.
	 * 2. Partition id list.
	 * 3. Run scheduler job.
	 */
	public static void main(String[] args) {
		logger.info("[BEGIN]main()");
		try {
			CascadeSVMTrainParameter parameter = new CascadeSVMTrainParameter(args);
			ArrayList<Integer> idList = CascadeSVMIOHelper.readIdListHadoop(parameter.idlistPath);
			parameter.nData = idList.size();
			CascadeSVMPartitioner.partitionIdListHadoop(parameter, idList);
			String schedulerParameterPath = writeSchedulerParameter(parameter);
			CascadeSVMScheduler.runSchedulerJob(schedulerParameterPath, new JobConf(CascadeSVMScheduler.class));
		}
		catch (CascadeSVMParameterFormatError e) {
			logger.info(e.toString());
			logger.info("\n" + CascadeSVMTrainParameter.helpText);
		} 
		catch (Exception e) {
			e.printStackTrace();
		}
		logger.info("[END]main()");
	}
}
