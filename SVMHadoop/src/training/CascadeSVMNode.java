package training;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.log4j.Logger;

import libsvm.svm;
import libsvm.svm_model;
import libsvm.svm_node;
import libsvm.svm_parameter;
import libsvm.svm_problem;
import local.KernelProjector;

public class CascadeSVMNode extends MapReduceBase 
		implements Mapper<IntWritable, Text, IntWritable, Text>,
		Reducer<IntWritable, Text, NullWritable, NullWritable> {
	private static Logger logger;
	private static double[] tune_C; // = {0.125d};
	private static double[] tune_gamma;
	private static int randomSeed;
	static {
		logger = Logger.getLogger(CascadeSVMNode.class);
		tune_C = new double[5];
		for (int i = 0; i < 5; i++)
			tune_C[i] = Math.pow(2, i-2);
		tune_gamma = new double[3];
		for (int i = 0; i < 3; i++)
			tune_gamma[i] = Math.pow(2, i-1);
		randomSeed = 0;
	}

	@Override
	public void map(IntWritable key, Text value,
			OutputCollector<IntWritable, Text> output, Reporter reporter)
			throws IOException {
		logger.info("[BEGIN]map()");
		logger.info(value.toString());
		CascadeSVMNodeParameter parameter = null;
		try {
			parameter = new CascadeSVMNodeParameter(value.toString());
		} catch (CascadeSVMParameterFormatError e) {
			logger.info(e.toString());
			logger.info(CascadeSVMNodeParameter.helpText);
			return ;
		}
		logger.info(parameter.toString());
		svm.rand.setSeed(randomSeed);
		
		reporter.setStatus("read id list");
		ArrayList<Integer> idList = CascadeSVMIOHelper.readIdListHadoop(parameter.idlistPath);
		
		reporter.setStatus("read label");
		double[] labels = CascadeSVMIOHelper.readLabelHadoop(parameter.labelPath, idList);
		
		reporter.setStatus("project kernel");
		float[][] kernel = projectKernelHadoop(parameter, idList);
		logger.info("kernel size = " + kernel.length + " x " + kernel[0].length);
		logger.info("kernel[0][0] = " + Float.toString(kernel[0][0]));
		logger.info("kernel[0][1] = " + Float.toString(kernel[0][1]));
		CascadeSVMIOHelper.printMemory();
		
		reporter.setStatus("create problem");
		svm_problem problem = createSVMProblem(idList, kernel, labels);
		
		CascadeSVMIOHelper.printMemory();
		kernel = null; // GC
		
		reporter.setStatus("cross validation");
		svm_parameter param = crossValidationHadoop(problem, parameter.nFold, reporter);
		
		reporter.setStatus("svm train");
		svm_model model = svm.svm_train(problem, param);
		
		reporter.setStatus("write model");
		CascadeSVMIOHelper.writeModelHadoop(parameter.modelPath, model);
		
		reporter.setStatus("write support vector");
		CascadeSVMIOHelper.writeSVIdListHadoop(parameter.SVPath, model, idList);
		
		reporter.setStatus("write LD");
		double LD = computeLD(model, problem);
		CascadeSVMIOHelper.writeLDHadoop(parameter.LDPath, LD);
		
		output.collect(new IntWritable(0), new Text("done."));
		logger.info("[END]map()");
	}
	
	@Override
	public void reduce(IntWritable key, Iterator<Text> value,
			OutputCollector<NullWritable, NullWritable> output, Reporter reporter)
			throws IOException {
		// Nothing to do.
	}
	
	public static void runNodeJob(CascadeSVMSchedulerParameter parameter, String nodeParameterPath, int startId, int endId, JobConf oldConf) throws IOException {
		logger.info("[BEGIN]runNodeJob()");
		// JobConf conf = new JobConf(CascadeSVMNode.class);
		JobConf conf = new JobConf(oldConf); // inherit job conf from old conf
		// CascadeSVMNode (iteration 1, nodes 0-7)
		conf.setJobName("CascadeSVMNode (iteration " + Integer.toString(parameter.iterationId) + ", nodes " + startId + "-" + endId + ")");
		FileInputFormat.setInputPaths(conf, new Path(nodeParameterPath));
		conf.setMapperClass(CascadeSVMNode.class);
		conf.setReducerClass(CascadeSVMNode.class);
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(NullOutputFormat.class);
		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(Text.class);
		
    	conf.set("mapred.child.java.opts", "-Xmx1600m");		//cannot be too large <2000m
		
		conf.set("mapred.cluster.map.memory.mb","2048");
		conf.set("mapred.cluster.reduce.memory.mb","2048");
		
		conf.set("mapred.job.map.memory.mb","2048");
		conf.set("mapred.job.reduce.memory.mb","2048");
		conf.set("mapred.tasktracker.map.tasks.maximum", Integer.toString(endId-startId+1));
		conf.set("mapred.map.max.attempts","8");
		conf.set("mapred.reduce.max.attempts","8");
		
		JobClient client = new JobClient(conf);
		client.submitJob(conf);
		logger.info("[END]runNodeJob()");
	}
	
	public float[][] projectKernelHadoop(CascadeSVMNodeParameter parameter, ArrayList<Integer> idList) throws IOException {
		logger.info("[BEGIN]projectKernelHadoop()");
		KernelProjector projector = new KernelProjector();
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		float[][] kernel = projector.projectHadoop(fs, parameter.kernelPath, idList, parameter.nData);
		logger.info("[END]projectKernelHadoop()");
		return kernel;
	}
	
	@SuppressWarnings("unchecked")
	public svm_parameter crossValidationHadoop(svm_problem problem, int nrFold, Reporter reporter) {
		 logger.info("[BEGIN]crossValidation()");
		 double[] target = new double[problem.y.length];
		 ArrayList<Double>[] probabilities = new ArrayList[problem.l+1];
		 for (int i = 0; i < problem.l + 1; i++)
			 probabilities[i] = new ArrayList<Double>();
		 
		 double bestMap = 0;
		 double bestGamma = 1;
		 double bestC = 1;
		 svm_parameter parameter = getDefaultParams(); 
		 double scale;
		 for (int i = 0; i < tune_gamma.length; i++) {
			 scale = (i==0 ? tune_gamma[i] : tune_gamma[i] / tune_gamma[i - 1]);
			 changeProblem(problem, scale);
			 for (int j = 0; j < tune_C.length; j++) {
				 reporter.setStatus("cross validation: gamma = " + Double.toString(tune_gamma[i]) + "C = " + Double.toString(tune_C[i]));
				 parameter.C = tune_C[i];
				 svm.svm_cross_validation(problem, parameter, nrFold, target, probabilities);
				 logger.info("probabilities[0]" + probabilities[0].toString());
				 logger.info("probabilities[1]" + probabilities[1].toString());
				 double map = computeAveragePrecision(problem.y, probabilities);
				 if (map > bestMap)
				 {
					 bestMap = map;
					 bestGamma = tune_gamma[i];
					 bestC = tune_C[j];
				 }
			 }
		 }
		 /*Change problem to best parameter*/
		 scale = bestGamma / tune_gamma[tune_gamma.length - 1];
		 changeProblem(problem, scale);
		 svm_parameter bestParameter = getDefaultParams();
		 bestParameter.C = bestC;
		 logger.info("bestMap: "+Double.toString(bestMap));
		 logger.info("bestGamma = " + Double.toString(bestGamma));
		 logger.info("bestC: " + Double.toString(bestC));
		 logger.info("[END]crossValidation()");
		 return bestParameter;
	}

	public svm_problem createSVMProblem(ArrayList<Integer> idList, float[][] kernel, double[] labels) {
		logger.info("[BEGIN]createSVMProblem");
		svm_problem prob = new svm_problem();
		
		prob.l = kernel.length;
		int m = kernel[0].length + 1;
		
		prob.x = new svm_node[prob.l][];
		for (int i = 0; i < prob.l; i++) {
			prob.x[i] = new svm_node[m];
			prob.x[i][0] = new svm_node();
			prob.x[i][0].index = 0;
			prob.x[i][0].value = i + 1;
			for (int j = 1; j < m; j++) {
				prob.x[i][j] = new svm_node();
				prob.x[i][j].index = j;
				prob.x[i][j].value = Math.exp(kernel[i][j - 1]);
			}
		}
		
		prob.y = labels;
		logger.info("[END]createSVMProblem");
		return prob;
	}

	
	public double computeLD(svm_model model, svm_problem problem) {
		logger.info("[BEGIN]computeLD()");
		double LD = 0;
		int[] sv_indices = new int[model.l];
		for (int i = 0; i < model.l; i++) {
			sv_indices[i] = (int)model.SV[i][0].value - 1; // sv id BEGINs from 1
			LD += model.sv_coef[0][i] * problem.y[sv_indices[i]] * model.label[0]; // [BUG FIX]: Sometimes the label will be -1 1, need to multiply -1 to ensure the right sign for sv_coef
		}
		for (int i = 0; i < model.l; i++)
			for (int j = 0; j < model.l; j++) {
				LD = LD - 0.5 * model.sv_coef[0][i] * model.sv_coef[0][j] * problem.x[sv_indices[i]][sv_indices[j]+1].value;
			}
		logger.info("[BEGIN]computeLD()");
		return LD;
	}
	

	
	public svm_parameter getDefaultParams() {
		svm_parameter param = new svm_parameter();
		param.svm_type = svm_parameter.C_SVC;
		param.kernel_type = svm_parameter.PRECOMPUTED;
		param.degree = 3;
		param.gamma = 0.125;	// 1/num_features, don't make it zero...
		param.coef0 = 0;
		param.nu = 0.5;
		param.cache_size = 100;
		param.C = 1;
		param.eps = 1e-3;
		param.p = 0.1;
		param.shrinking = 1;
		param.probability = 1;
		param.nr_weight = 0;
		param.weight_label = new int[0];
		param.weight = new double[0];
		return param;
	}
	
	public void changeProblem(svm_problem prob, double scale) {
		logger.info("[BEGIN]changeProblem()");
		for (int i = 0; i < prob.x.length; i++) {
			for (int j = 1; j < prob.x[i].length; j++) {
				prob.x[i][j].value = Math.exp(Math.log1p(prob.x[i][j].value - 1) * scale);
			}
		}
		logger.info("[End]changeProblem()");
	}
	
	/**
	 * Format of probability
	 * 1.0 0.0
	   0.36625498614020674 0.6337450138597933 4.0
       0.04081900003189424 0.959180999968106 4.0
     *
	 * @param probabilities
	 * @return
	 */
	public double computeAveragePrecision(double[] label, ArrayList<Double>[] probabilities) {
		logger.info("[BEGIN]computeAveragePrecision()");
		int labelIndex = 0;
		if (probabilities[0].get(1) == 1) {
			labelIndex = 1;
		}
		
		ArrayList<CrossValidationItem> crossValidationItems = new ArrayList<CrossValidationItem>(1024);
		for (int i = 1; i < probabilities.length; i++) {
			double prob = probabilities[i].get(labelIndex);
			int foldno = (int) probabilities[i].get(2).doubleValue();
			int trueLabel = (int) label[i - 1];
			crossValidationItems.add(new CrossValidationItem(prob, trueLabel, foldno));
		}
		
		ArrayList<ArrayList<CrossValidationItem>> CrossValidationItemBySorts = CrossValidationItem.getfolds(crossValidationItems);
		double map = 0.0d;
		for (int i = 0; i < CrossValidationItemBySorts.size(); i++) {
			map += calculateAP(CrossValidationItemBySorts.get(i))[0];
		}
		map /= CrossValidationItemBySorts.size();
		logger.info("[END]computeAveragePrecision()");
		return map;
	}

	public double[] calculateAP(ArrayList<CrossValidationItem> crossValidationItems) {
		// logger.info("[BEGIN]calculateAP()");
		double[] result = new double[2];
		result[0] = 0;
		result[1] = -1;
		double tp = 0;
		int pos = 0;
		//count the positive samples
		for(int i = 0 ; i < crossValidationItems.size() ; i++) {
			if(crossValidationItems.get(i).label == 1)	pos++;
		}
		
		double deltaRecall = 1.0d / pos;
		
		Collections.sort(crossValidationItems);
		
		for(int i = 0 ; i < crossValidationItems.size() ; i++) {
			if(crossValidationItems.get(i).label == 1)	{
				tp++;
				double cur_precision = tp/(i+1);
				result[0] += cur_precision*deltaRecall;
			}
		}
		// logger.info("[END]calculateAP()");
		return result;
	}
	
	// Local version

	public void trainLocal(CascadeSVMNodeParameter parameter) throws IOException {
		logger.info("[Begin]trainLocal()");
		svm.rand.setSeed(randomSeed);
		ArrayList<Integer> idList = CascadeSVMIOHelper.readIdListLocal(parameter.idlistPath);
		double[] labels = CascadeSVMIOHelper.readLabelLocal(parameter.labelPath, idList);
		float[][] kernel = projectKernelLocal(parameter, idList);
		logger.info("kernel[0][0] = " + Float.toString(kernel[0][0]));
		logger.info("kernel[0][1] = " + Float.toString(kernel[0][1]));
		svm_problem problem = createSVMProblem(idList, kernel, labels);
		kernel = null;  // GC
		svm_parameter param = crossValidationLocal(problem, parameter.nFold);
		svm_model model = svm.svm_train(problem, param); 													 // train
		CascadeSVMIOHelper.writeModelLocal(parameter.modelPath, model);												 // output model
		CascadeSVMIOHelper.writeSVIdListLocal(parameter.SVPath, model, idList);
		double LD = computeLD(model, problem);
		CascadeSVMIOHelper.writeLDLocal(parameter.LDPath, LD);
		logger.info("[End]trainLocal()");
	}
	
	public float[][] projectKernelLocal(CascadeSVMNodeParameter parameter, ArrayList<Integer> idList) throws IOException {
		logger.info("[BEGIN]projectKernelHadoop()");
		KernelProjector projector = new KernelProjector();
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		float[][] kernel = projector.projectHadoop(fs, parameter.kernelPath, idList, parameter.nData);
//		KernelProjector projector = new KernelProjector();
//		float[][] kernel = projector.project(new File(parameter.kernelPath), idList, parameter.nData);
		logger.info("[END]projectKernelHadoop()");
		return kernel;
	} 
	
	@SuppressWarnings("unchecked")
	public svm_parameter crossValidationLocal(svm_problem problem, int nrFold) {
		 logger.info("[BEGIN]crossValidation()");
		 double[] target = new double[problem.y.length];
		 ArrayList<Double>[] probabilities = new ArrayList[problem.l+1];
		 for (int i = 0; i < problem.l + 1; i++)
			 probabilities[i] = new ArrayList<Double>();
		 
		 double bestMap = 0;
		 double bestGamma = 1;
		 double bestC = 1;
		 svm_parameter parameter = getDefaultParams(); 
		 double scale;
		 for (int i = 0; i < tune_gamma.length; i++) {
			 scale = (i==0 ? tune_gamma[i] : tune_gamma[i] / tune_gamma[i - 1]);
			 changeProblem(problem, scale);
			 for (int j = 0; j < tune_C.length; j++) {
				 parameter.C = tune_C[i];
				 svm.svm_cross_validation(problem, parameter, nrFold, target, probabilities);
				 logger.info("probabilities[0]" + probabilities[0].toString());
				 logger.info("probabilities[1]" + probabilities[1].toString());
				 double map = computeAveragePrecision(problem.y, probabilities);
				 if (map > bestMap)
				 {
					 bestMap = map;
					 bestGamma = tune_gamma[i];
					 bestC = tune_C[j];
				 }
			 }
		 }
		 /*Change problem to best parameter*/
		 scale = bestGamma / tune_gamma[tune_gamma.length - 1];
		 changeProblem(problem, scale);
		 svm_parameter bestParameter = getDefaultParams();
		 bestParameter.C = bestC;
		 logger.info("bestMap: "+Double.toString(bestMap));
		 logger.info("bestGamma = " + Double.toString(bestGamma));
		 logger.info("bestC: " + Double.toString(bestC));
		 logger.info("[END]crossValidation()");
		 return bestParameter;
	}
	
	// TEST Hadoop
//	public static void main(String[] args) {
//		logger.info("[BEGIN]main()");
//		try {
//			CascadeSVMNodeParameter parameter = new CascadeSVMNodeParameter();
//			parameter.modelPath  = "shicheng/cascadesvm/workDir/model.1.0";
//			parameter.SVPath     = "shicheng/cascadesvm/workDir/sv.1.0";
//			parameter.LDPath     = "shicheng/cascadesvm/workDir/LD.1.0";
//			parameter.kernelPath = "shicheng/cascadesvm/sin10000";
//			parameter.labelPath  = "shicheng/cascadesvm/10000.labels";
//			parameter.idlistPath = "shicheng/cascadesvm/100.idlist.sequence";
//			parameter.workDir	 = "shicheng/cascadesvm/workDir";
//			parameter.nFold		 = 5;
//			parameter.nData 	 = 10000;
//			String nodeParameterPath = CascadeSVMPathHelper.getNodeParameterPath(parameter.workDir);
//			CascadeSVMIOHelper.writeNodeParameterHadoop(nodeParameterPath, parameter);
//
//			JobConf conf = new JobConf(CascadeSVMNode.class);
//			conf.setJobName("CascadeSVMNode Test");
//			FileInputFormat.addInputPath(conf, new Path(nodeParameterPath));
//			conf.setMapperClass(CascadeSVMNode.class);
//			conf.setReducerClass(CascadeSVMNode.class);
//			conf.setInputFormat(SequenceFileInputFormat.class);
//			conf.setOutputFormat(NullOutputFormat.class);
//			conf.setMapOutputKeyClass(IntWritable.class);
//			conf.setMapOutputValueClass(Text.class);
//			
//	    	conf.set("mapred.child.java.opts", "-Xmx1200m");		//cannot be too large <2000m
//			
//			conf.set("mapred.cluster.map.memory.mb","2000");
//			conf.set("mapred.cluster.reduce.memory.mb","2000");
//			
//			conf.set("mapred.job.map.memory.mb","2000");
//			conf.set("mapred.job.reduce.memory.mb","2000");
//			conf.set("mapred.tasktracker.map.tasks.maximum","1");
//			conf.set("mapred.map.max.attempts","3");
//			conf.set("mapred.reduce.max.attempts","3");
//			
//			JobClient.runJob(conf);
//		}
//		catch (IOException e) {
//			// e.printStackTrace();
//			logger.info("[ERROR]IO Exception.");
//		}
//		logger.info("[END]main()");
//	}
	
	// Test Local
	public static void main(String[] args) {
		CascadeSVMNodeParameter parameter = new CascadeSVMNodeParameter();
		parameter.modelPath  = "C:\\Users\\Light\\JavaJar\\workDir\\model.1.0";
		parameter.SVPath     = "C:\\Users\\Light\\JavaJar\\workDir\\sv.1.0";
		parameter.LDPath     = "C:\\Users\\Light\\JavaJar\\workDir\\LD.1.0";
		parameter.kernelPath = "C:\\Users\\Light\\JavaJar\\sin10000";
		parameter.labelPath  = "C:\\Users\\Light\\JavaJar\\10000.labels";
		parameter.idlistPath = "C:\\Users\\Light\\JavaJar\\100.idlist";
		parameter.workDir	 = "C:\\Users\\Light\\JavaJar\\workDir";
		parameter.nFold		 = 5;
		parameter.nData 	 = 10000;
		try {
			new CascadeSVMNode().trainLocal(parameter);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
//	/**
//	 * This is the self-debug version.
//	 * @param kerneLDath
//	 * @param idListPath
//	 * @return
//	 * @throws IOException 
//	 * @throws NumberFormatException 
//	 */
//	private float[][] projectKernel(BufferedReader kernelFile, ArrayList<Integer> idList) throws NumberFormatException, IOException {
//		logger.info("[Begin]projectKernel");
//		int n = idList.size();
//		float[][] kernel = new float[n][n];
//		int row = 0;
//		int i = 0;
//		String line;
//		while ((line = kernelFile.readLine()) != null) {
//			if (idList.get(i).intValue() - 1 == row) {
//				String[] valueArray = line.split(" ");
//				for (int j = 0; j < n; j++)
//					try {
//						kernel[i][j] = Float.parseFloat(valueArray[idList.get(j).intValue() - 1]);
//					} catch (ArrayIndexOutOfBoundsException e) {
//						logger.info("[Error]array index out of bound");
//						logger.info(Integer.toString(idList.get(j).intValue() - 1));
//					}
//				i++;
//				if (i == n)
//					break;
//			}
//			row++;
//		}
//		kernelFile.close();
//		logger.info("[End]projectKernel");
//		return kernel;
//	}
}
