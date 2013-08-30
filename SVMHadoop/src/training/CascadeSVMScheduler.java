package training;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

/*
 * Format of temporal files:
 * id list: subset.#.#
 * subset list: subset.{time_stamp}.list
 * models: model.#.#
 * support vectors: SV.#.#
 * dual form value: LD.#.#, How to obtain these LD? Which also indicate the completeness of training.
 */
public class CascadeSVMScheduler extends MapReduceBase 
		implements Mapper<LongWritable, Text, LongWritable, Text>,
		Reducer<LongWritable, Text, NullWritable, NullWritable> {
	public static boolean verbose = true;
	public static Logger logger;
	public static String workDir;
	
	static {
		workDir = CascadeSVMPathHelper.getHadoopWorkDir();
	}

	@Override
	public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter)
			throws IOException {
		CascadeSVMSchedulerParameter parameter = CascadeSVMSchedulerParameter.parseArgs(value.toString());
		// 1. Merge SVs id list
		String outputListPath = mergeSVIntoSubsets(parameter.idlistPath, parameter.lastSVPath, workDir);
		// 2. Start a new job
		JobConf conf = new JobConf(CascadeSVMTrain.class);
		conf.setJobName("Cascade SVM Train 0-"+Integer.toString(parameter.nSubset-1));
		conf.set("kernelpath", parameter.kernelPath);
		conf.set("labelPath", parameter.labelPath);
		FileInputFormat.addInputPath(conf, new Path(outputListPath));
		FileOutputFormat.setOutputPath(conf, new Path(workDir));
		conf.setMapperClass(CascadeSVMNode.class);
		conf.setReducerClass(CascadeSVMNode.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		JobClient.runJob(conf);

		output.collect(key, value);
	} 
	
	@Override
	public void reduce(LongWritable key, Iterator<Text> value,
			OutputCollector<NullWritable, NullWritable> output, Reporter reporter)
			throws IOException {
		while (value.hasNext()) {
			try {
				schedule(value.next().toString(), workDir);
			} catch (CascadeSVMParameterFormatError e) {
				// nothing we can do..
			}
		}
	}
	
	public static String mergeSVIntoSubsets(String subsetListPath, String SVPath, String outputDir) 
			throws IOException {
		/* It will write two kinds of files
		 * ${outputPathPrefix}.list: a list of id list path
		 * ${outputPathPrefix}.#: id list, # in [0..n)
		 */
		if (verbose) logger.info("[START] mergeSVIntoSubsets");
		if (SVPath == "") {
			if (verbose) logger.info("No support vector to be merged.");
			if (verbose) logger.info("[END] mergeSVIntoSubsets");
			return subsetListPath;
		}
		BufferedReader subsetsListReader = new BufferedReader(new FileReader(subsetListPath));
		String outputListPath = getSubsetListPath(outputDir);
		BufferedWriter outputListWriter = new BufferedWriter(new FileWriter(outputListPath));
		int id = 0;
		String idListPath;
		while ((idListPath = subsetsListReader.readLine()) != null) {
			String outputPath = getIdListPath(outputDir, 0, id);
			outputListWriter.write(outputPath);
			outputListWriter.newLine();
			mergeTwoSubsets(idListPath, SVPath, outputPath);
			id++;
		}
		outputListWriter.close();
		subsetsListReader.close();
		if (verbose) logger.info("[END] mergeSVIntoSubsets"); 
		return outputListPath;
	}

	public static void schedule(String argLine, String workDir) throws IOException, CascadeSVMParameterFormatError {
		if (verbose) logger.info("[START] schedule");
		CascadeSVMParameter param = new CascadeSVMParameter(argLine);
		double LD_max = 0;
		ArrayList<Integer> runningIds = new ArrayList<Integer>();
		ArrayList<Integer> finishIds = new ArrayList<Integer>();
		for (int i = 0; i < param.subsetSize; i++)
			runningIds.add(new Integer(i));
		int nowId = param.subsetSize;
		while (runningIds.size() != 0 || finishIds.size() != 1) {
			for (int i = 0; i < runningIds.size();) {
				try {
					String LDPath = getLDPath(workDir, param.iter, runningIds.get(i).intValue());
					BufferedReader LDReader = new BufferedReader(new FileReader(LDPath));
					double LD_temp = Double.parseDouble(LDReader.readLine().trim());
					if (LD_temp > LD_max) {
						LD_max = LD_temp;
					}
					LDReader.close();
					finishIds.add(runningIds.get(i));
					runningIds.remove(i);
				}
				catch (IOException e) {
					i++;
				}
			}
			if (finishIds.size() > 1) {
				String mergedListPath = mergeSubsets(finishIds, workDir, param.iter, nowId);
				int lastId = nowId;
				nowId = nowId + finishIds.size() / 2;
				for (int i = lastId; i < nowId; i++) {
					finishIds.remove(0);
					finishIds.remove(0);
					runningIds.add(new Integer(i));
				}
				// TODO: Start new Cascade SVM job
				// Local verbose:
				(new Thread(new FakeSubmitJob(param.kernelPath, param.labelPath, mergedListPath, 5, workDir))).start();
			}
			try {
				Thread.sleep(30*1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		if (Math.abs((LD_max - param.LD_last)/ param.LD_last) > 1e-5) {
			if (verbose) logger.info("LD = " + Double.toString(LD_max)); 
		}
		if (verbose) logger.info("[END] schedule");
	}
	
	public static String mergeSubsets(ArrayList<Integer> ids, String workDir, int iter, int startId) throws IOException {
		if (verbose) logger.info("[START] mergeSubsets"); 
		String mergedListPath = CascadeSVMPathHelper.getSubsetListPath(workDir);
		BufferedWriter mergedListWriter = new BufferedWriter(new FileWriter(mergedListPath));
		
		int i = 0;
		int nowId = startId;
		while (i + 1 < ids.size()) {
			String subIdPath1 = CascadeSVMPathHelper.getSVPath(workDir, iter, ids.get(i));
			String subIdPath2 = CascadeSVMPathHelper.getSVPath(workDir, iter, ids.get(i+1));
			i += 2;
			String unionIdPath = CascadeSVMPathHelper.getIdListPath(workDir, iter, nowId);
			nowId++;
			mergeTwoSubsets(subIdPath1, subIdPath2, unionIdPath);
			mergedListWriter.write(unionIdPath);
			mergedListWriter.newLine();
		}
		
		mergedListWriter.close();
		if (verbose) logger.info("[END] mergeSubsets"); 
		return mergedListPath;
	}
	
	public static void mergeTwoSubsets(String subPath1, String subPath2, String unionPath)
			throws IOException {
		if (verbose) logger.info("[START] mergeTwoSubsets");
		ArrayList<Integer> idList1 = readIdListLocal(subPath1);
		ArrayList<Integer> idList2 = readIdListLocal(subPath2);
		HashSet<Integer> idSet = new HashSet<Integer>(idList1);
		idSet.addAll(idList2);
		ArrayList<Integer> idList = new ArrayList<Integer>(idSet);
		// Collections.sort(idList);
		writeIdListLocal(unionPath, idList);
		if (verbose) logger.info("[END] mergeTwoSubsets");
	}
	
	public static ArrayList<Integer> readIdListLocal(String idListPath) throws IOException {
		ArrayList<Integer> idList = new ArrayList<Integer>();
		BufferedReader reader = new BufferedReader(new FileReader(idListPath));
		String line;
		while ((line = reader.readLine()) != null)
			idList.add(new Integer(line.trim()));
		reader.close();
		return idList;
	}
	
	public static void writeIdListLocal(String idListPath, ArrayList<Integer> idList) throws IOException {
		BufferedWriter writer = new BufferedWriter(new FileWriter(idListPath));
		for (int i = 0; i < idList.size(); i++) {
			writer.write(idList.get(i).toString());
			writer.newLine();
		}
		writer.close();
	} 
	
	// Test
	public static class FakeSubmitJob extends Thread {
		String kernelPath;
		String labelPath;
		String mergedListPath;
		int nrFold;
		String workDir;
		FakeSubmitJob(String kernelPath, String labelPath, String mergedListPath, int nrFold, String workDir) {
			this.kernelPath = kernelPath;
			this.labelPath = labelPath;
			this.mergedListPath = mergedListPath;
			this.nrFold = nrFold;
			this.workDir = workDir;
		}
		
		public void run() {
			if (verbose) logger.info("[START] FakeSubmitJob.run()");
			try {
				BufferedReader mergedListReader = new BufferedReader(new FileReader(mergedListPath));
				String idListPath;
				while ((idListPath = mergedListReader.readLine()) != null) {
					String[] names = idListPath.split("\\.");
					int iter = Integer.parseInt(names[names.length-2]);
					int id = Integer.parseInt(names[names.length-1]);
					String modelOutputPath = CascadeSVMPathHelper.getModelPath(workDir, iter, id);
					String SVIdOutputPath = CascadeSVMPathHelper.getSVPath(workDir, iter, id);
					CascadeSVMNode singleSVM = new CascadeSVMNode(kernelPath, labelPath, idListPath, nrFold, modelOutputPath, SVIdOutputPath);
					String LDPath = CascadeSVMPathHelper.getLDPath(workDir, iter, id);
					singleSVM.writeLD(LDPath);
				}
				mergedListReader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			if (verbose) logger.info("[END] FakeSubmitJob.run()");
		}
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		logger = Logger.getLogger(CascadeSVMScheduler.class);
		
		String iter = Integer.toString(1);
		String LD = Double.toString(0);
		String SVPath = "";
		String subsetSize = Integer.toString(8);
		String subsetListPath = "/home/lightxu/Work/cascadesvm_experiment/scheduler/subsets.list";
		String kernelPath = "/home/lightxu/Work/cascadesvm_experiment/scheduler/kernel.txt";
		String labelPath = "/home/lightxu/Work/cascadesvm_experiment/scheduler/label.txt";
		String nrFold = Integer.toString(5);
		String argLine = iter + " " + LD + " " + SVPath + " " + subsetSize + " " + subsetListPath + " " + kernelPath + " " + labelPath + " " + nrFold;
		String workDir = "/home/lightxu/Work/cascadesvm_experiment/scheduler";
		
		try {
			BufferedReader reader = new BufferedReader(new FileReader(subsetListPath));
			for (int i = 7; i < 8; i++) {
					String idListPath = reader.readLine().trim();
					String modelOutputPath = CascadeSVMPathHelper.getModelPath(workDir, 1, i);
					String SVIdOutputPath = CascadeSVMPathHelper.getSVPath(workDir, 1, i);
					String LDOutputPath = CascadeSVMPathHelper.getLDPath(workDir, 1, i);
					CascadeSVMNode svm = new CascadeSVMNode(kernelPath, labelPath, idListPath, 5, modelOutputPath, SVIdOutputPath);
					svm.writeLD(LDOutputPath);
			}
			reader.close();
			schedule(argLine, workDir);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (CascadeSVMParameterFormatError e) {
			e.printStackTrace();
		}
	}

}
