package training;

import java.io.IOException;

import org.apache.log4j.Logger;

public class CascadeSVMTrain {
	public static Logger logger = Logger.getLogger(CascadeSVMTrain.class);
	/**
	 * @param trainParameter
	 * @param subsetListPath
	 * @return schedulerParameterPath
	 * @throws IOException 
	 */
	private static String writeSchedulerParameter(CascadeSVMTrainParameter trainParameter, String subsetListPath) throws IOException {
		logger.info("[BEGIN]CascadeSVMTrain.writeSchedulerParameter()");
		CascadeSVMSchedulerParameter schedulerParameter = new CascadeSVMSchedulerParameter(trainParameter);
		schedulerParameter.iterationId = 1;
		schedulerParameter.lastLD = 0;
		schedulerParameter.lastSVPath = null;
		schedulerParameter.subsetListPath = subsetListPath;
		
		String schedulerParameterPath = CascadeSVMPathHelper.getSchedulerParameterPath(trainParameter.workDir);
		CascadeSVMIOHelper.writeSchedulerParameterHadoop(schedulerParameterPath, schedulerParameter);
		logger.info("[END]CascadeSVMTrain.writeSchedulerParameter()");
		return schedulerParameterPath;
	}
	
	public static void main(String[] args) {
		logger.info("[BEGIN]CascadeSVMTrain.main()");
		try {
			CascadeSVMTrainParameter parameter = new CascadeSVMTrainParameter(args);
			// subsetListPath = runPartitionerJob(parameter);
			String subsetListPath = CascadeSVMPartitioner.partitionIdListHadoop(parameter);
			String schedulerParameterPath = writeSchedulerParameter(parameter, subsetListPath);
			CascadeSVMScheduler.runSchedulerJob(schedulerParameterPath);
		}
		catch (CascadeSVMParameterFormatError e) {
			logger.info(e.toString());
		} 
		catch (Exception e) {
			e.printStackTrace();
		}
		logger.info("[END]CascadeSVMTrain.main()");
	}
}
