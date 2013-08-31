package training;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import libsvm.svm;
import libsvm.svm_model;
import libsvm.svm_node;
import libsvm.svm_parameter;
import libsvm.svm_problem;
import local.KernelProjector;

public class CascadeSVMNode extends MapReduceBase 
		implements Mapper<IntWritable, Text, IntWritable, Text>,
		Reducer<IntWritable, Text, IntWritable, Text> {
	private static boolean verbose;
	private static Logger logger;
	private static double[] tune_parameter; // = {0.125d};
	private static int randomSeed;
	static {
		verbose = true;
		logger = Logger.getLogger(CascadeSVMNode.class);
		tune_parameter = new double[11];
		for (int i = -10; i <= 10; i += 2)
			tune_parameter[i] = Math.pow(2, i);
		randomSeed = 0;
	}

	@Override
	public void map(IntWritable key, Text value,
			OutputCollector<IntWritable, Text> output, Reporter reporter)
			throws IOException {
		CascadeSVMNodeParameter parameter = null;
		try {
			parameter = new CascadeSVMNodeParameter(value.toString());
		} catch (CascadeSVMParameterFormatError e) {
			return ;
		}
		svm.rand.setSeed(randomSeed);
		ArrayList<Integer> idList = CascadeSVMIOHelper.readIdListHadoop(parameter.idlistPath);
		double[] labels = CascadeSVMIOHelper.readLabelHadoop(parameter.labelPath, idList);
		float[][] kernel = projectKernelHadoop(parameter, idList);						        // project kernel
		svm_problem problem = convert2SVMProblem(idList, kernel, labels);									// create svm problem
		kernel = null; // GC
		svm_parameter param = crossValidation(problem, parameter.nFold, randomSeed);						// cross validation
		svm_model model = svm.svm_train(problem, param); 													// train
		CascadeSVMIOHelper.writeModelHadoop(parameter.modelPath, model);
		CascadeSVMIOHelper.writeSVIdListHadoop(parameter.SVPath, model, idList);
		double LD = computeLD(model, problem);
		CascadeSVMIOHelper.writeLDHadoop(parameter.LDPath, LD);
	}
	
	@Override
	public void reduce(IntWritable key, Iterator<Text> value,
			OutputCollector<IntWritable, Text> output, Reporter reporter)
			throws IOException {
		// Nothing to do.
	}
	
	public static float[][] projectKernelHadoop(CascadeSVMNodeParameter parameter, ArrayList<Integer> idList) throws IOException {
		KernelProjector projector = new KernelProjector();
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		float[][] kernel = projector.projectHadoop(fs, parameter.kernelPath, idList, parameter.dimension);
		return kernel;
	} 
	
	public static double computeLD(svm_model model, svm_problem problem) {
		if (verbose) logger.info("[Begin]Compute LD");
		double LD = 0;
		int[] sv_indices = new int[model.l];
		for (int i = 0; i < model.l; i++) {
			sv_indices[i] = (int)model.SV[i][0].value - 1; // sv id starts from 1
			LD += model.sv_coef[0][i] * problem.y[sv_indices[i]] * model.label[0]; // [BUG FIX]: Sometimes the label will be -1 1, need to multiply -1 to ensure the right sign for sv_coef
		}
		for (int i = 0; i < model.l; i++)
			for (int j = 0; j < model.l; j++) {
				LD = LD - 0.5 * model.sv_coef[0][i] * model.sv_coef[0][j] * problem.x[sv_indices[i]][sv_indices[j]+1].value;
			}
		if (verbose) logger.info("[End]Compute LD");
		return LD;
	}

	public static svm_problem convert2SVMProblem(ArrayList<Integer> idList, float[][] kernel, double[] labels) {
		if (verbose) logger.info("[Start]convert2SVMProblem");
		svm_problem prob = new svm_problem();
		
		prob.l = kernel.length;
		int m = kernel[0].length + 1;
		
		prob.x = new svm_node[prob.l][m];
		for (int i = 0; i < prob.l; i++) {
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
		if (verbose) logger.info("[End]convert2SVMProblem");
		return prob;
	}
	
	public static svm_parameter crossValidation(svm_problem problem, int nrFold, double randomSeed) {
		 if (verbose) logger.info("[Start]crossValidation");
		 double[] target = new double[problem.y.length];
		 @SuppressWarnings("unchecked")
		 ArrayList<Double>[] probabilities = new ArrayList[problem.l+1];
		 for (int i = 0; i < problem.l + 1; i++)
			 probabilities[i] = new ArrayList<Double>();
		 
		 double bestMap = 0;
		 double bestGamma = 1;
		 double bestC = 1;
		 svm_parameter parameter = getDefaultParams(); 
		 double scale;
		 for (int i = 0; i < tune_parameter.length; i++) {
			 scale = (i==0 ? tune_parameter[i] : tune_parameter[i] / tune_parameter[i - 1]);
			 changeProblem(problem, scale);
			 for (int j = 0; j < tune_parameter.length; j++) {
				 parameter.C = tune_parameter[i];
				 svm.svm_cross_validation(problem, parameter, nrFold, target, probabilities);
				 if (verbose) logger.info("probabilities[0]" + probabilities[0].toString());
				 if (verbose) logger.info("probabilities[1]" + probabilities[1].toString());
				 double map = computeAveragePrecision(problem.y, probabilities);
				 if (map > bestMap)
				 {
					 bestMap = map;
					 bestGamma = tune_parameter[i];
					 bestC = parameter.C;
				 }
			 }
		 }
		 /*Change problem to best parameter*/
		 scale = bestGamma / tune_parameter[tune_parameter.length - 1];
		 changeProblem(problem, scale);
		 svm_parameter bestParameter = getDefaultParams();
		 bestParameter.C = bestC;
		 if (verbose) logger.info("[End]crossValidation");
		 return bestParameter;
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
	public static double computeAveragePrecision(double[] label, ArrayList<Double>[] probabilities) {
		if (verbose) logger.info("[Start]computeAveragePrecision");
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
		if (verbose) logger.info("[End]computeAveragePrecision");
		return map;
	}
	
	public static void changeProblem(svm_problem prob, double scale) {
		if (verbose) logger.info("[Start]changeProblem");
		for (int i = 0; i < prob.x.length; i++) {
			for (int j = 1; j < prob.x[i].length; j++) {
				prob.x[i][j].value = Math.exp(Math.log1p(prob.x[i][j].value - 1) * scale);
			}
		}
		if (verbose) logger.info("[End]changeProblem");
	}
	
	public static svm_parameter getDefaultParams() {
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

	public static double[] calculateAP(ArrayList<CrossValidationItem> crossValidationItems) {
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
		return result;
	}
	
	// TEST

//	public CascadeSVMNode(
//			String kerneLDath, 
//			String labeLDath, 
//			String idListPath, 
//			int nrFold, 
//			String modelOutputPath,
//			String SVIdOutputPath)
//					throws NumberFormatException, FileNotFoundException, IOException {
//		if (verbose) logger.info("[Begin]CascadeSVMNode");
//		svm.rand.setSeed(randomSeed);
//		idList = readIdList(new BufferedReader(new FileReader(idListPath))); // read id list
//		double[] labels = readLabel(idList, new BufferedReader(new FileReader(labeLDath)));		// read label
//		float[][] kernel = projectKernel(new BufferedReader(new FileReader(kerneLDath)), idList);								// project kernel
//		problem = convert2SVMProblem(idList, kernel, labels);									// create svm problem
//		kernel = null; // GC
//		svm_parameter param = crossValidation(problem, nrFold, randomSeed);						// cross validation
//		model = svm.svm_train(problem, param); 													// train
//		svm.svm_save_model(modelOutputPath, model);												// output model
//		writeSVId(new BufferedWriter(new FileWriter(SVIdOutputPath)));
//		if (verbose) logger.info("[End]CascadeSVMNode");
//	}
	
//	public void writeLD(String LDOutputPath) throws IOException {
//		BufferedWriter writer = new BufferedWriter(new FileWriter(LDOutputPath));
//		double LD = computeLD();
//		writer.write(Double.toString(LD));
//		writer.close();
//	}
//	
//	private ArrayList<Integer> readIdList(BufferedReader idListFile)
//			throws NumberFormatException, IOException {
//		if (verbose) logger.info("[Begin]readIdList");
//		ArrayList<Integer> idList = new ArrayList<Integer>();
//		String line;
//		while ((line = idListFile.readLine()) != null) {
//			int id = Integer.parseInt(line);
//			idList.add(new Integer(id));
//		}
//		idListFile.close();
//		// Sort the id list
//		Collections.sort(idList);
//		if (verbose) logger.info("[End]readIdList");
//		return idList;
//	}
//	
//	private void writeSVId(BufferedWriter SVIdOutputFile) throws IOException {
//		if (verbose) logger.info("[Start]writeSVId");
//		for (int i = 0; i < model.l; i++) {
//			SVIdOutputFile.write(Integer.toString(idList.get((int)model.SV[i][0].value - 1)));
//			SVIdOutputFile.newLine();
//		}
//		SVIdOutputFile.close();
//		if (verbose) logger.info("[End]writeSVId");
//	}
//	
//	/**
//	 * This is the self-debug version.
//	 * @param kerneLDath
//	 * @param idListPath
//	 * @return
//	 * @throws IOException 
//	 * @throws NumberFormatException 
//	 */
//	private float[][] projectKernel(BufferedReader kernelFile, ArrayList<Integer> idList) throws NumberFormatException, IOException {
//		if (verbose) logger.info("[Begin]projectKernel");
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
//		if (verbose) logger.info("[End]projectKernel");
//		return kernel;
//	}
//	
//	public static void main(String[] args) {
//		String kerneLDath = "/home/lightxu/Work/cascadesvm_note/train_single/kernel.txt";
//		String labeLDath = "/home/lightxu/Work/cascadesvm_note/train_single/label.txt"; 
//		String idListPath = "/home/lightxu/Work/cascadesvm_note/train_single/idlist.txt";
//		int nrFold = 5;
//		String modelOutputPath = "/home/lightxu/Work/cascadesvm_note/train_single/model.txt";
//		String SVIdOutputPath = "/home/lightxu/Work/cascadesvm_note/train_single/svid.txt";
//		try {
//			CascadeSVMNode svm = new CascadeSVMNode(kerneLDath, labeLDath, idListPath, nrFold, modelOutputPath, SVIdOutputPath);
//			double LD = svm.computeLD();
//			System.out.println(LD);
//		} catch (NumberFormatException e) {
//			e.printStackTrace();
//		} catch (FileNotFoundException e) {
//			e.printStackTrace();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//	}
}
