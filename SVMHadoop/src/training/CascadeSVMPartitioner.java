package training;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class CascadeSVMPartitioner extends MapReduceBase 
	implements Mapper<LongWritable, Text, LongWritable, Text>, 
		Reducer<LongWritable, Text, NullWritable, NullWritable> {
	private static Random rand = new Random();
	public int nSubset;
	public String subsetListPath;

	public void configure(JobConf conf) {
		nSubset = Integer.parseInt(conf.get("nSubset"));
		subsetListPath = conf.get("subsetListPath");
	}
	
	@Override
	public void reduce(LongWritable key, Iterator<Text> value,
			OutputCollector<NullWritable, NullWritable> collector, Reporter reporter)
			throws IOException {
		int subsetId = Integer.parseInt(key.toString());
		String workDir = CascadeSVMPathHelper.getHadoopWorkDir();
		String subsetPath = CascadeSVMPathHelper.getIdListPath(workDir, 0, subsetId);
		CascadeSVMIOHelper.writeIdListHadoop(subsetPath, value);
	}

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<LongWritable, Text> collector, Reporter reporter)
			throws IOException {
		collector.collect(new LongWritable(rand.nextInt(nSubset)), value);
	}
	
	/* Non MapReduce METHODS */
	/**
	 * @param parameter
	 * @return subsetListPath
	 * @throws IOException
	 */
	public static String partitionIdListHadoop(CascadeSVMTrainParameter parameter) throws IOException {
		ArrayList<Integer> idList = CascadeSVMIOHelper.readIdListHadoop(parameter.idlistPath);
		ArrayList< ArrayList<Integer> > subsets = partitionIdList(idList, parameter.nSubset);
		String workDir = CascadeSVMPathHelper.getHadoopWorkDir();
		String subsetListPath = CascadeSVMPathHelper.getSubsetListPath(workDir);
		ArrayList<String> subsetList = new ArrayList<String>();
		for (int i = 0; i < parameter.nSubset; i++) {
			String idListPath = CascadeSVMPathHelper.getIdListPath(workDir, 1, i);
			subsetList.add(idListPath);
			CascadeSVMIOHelper.writeIdListHadoop(idListPath, subsets.get(i));
		}
		CascadeSVMIOHelper.writeSubsetListHadoop(subsetListPath, subsetList);
		return subsetListPath;
	}
	
	public static String partitionIdListLocal(CascadeSVMTrainParameter parameter) throws IOException {
		ArrayList<Integer> idList = CascadeSVMIOHelper.readIdListLocal(parameter.idlistPath);
		ArrayList< ArrayList<Integer> > subsets = partitionIdList(idList, parameter.nSubset);
		String workDir = CascadeSVMPathHelper.getLocalWorkDir();
		String subsetListPath = CascadeSVMPathHelper.getSubsetListPath(workDir);
		ArrayList<String> subsetList = new ArrayList<String>();
		for (int i = 0; i < parameter.nSubset; i++) {
			String idListPath = CascadeSVMPathHelper.getIdListPath(workDir, 1, i);
			subsetList.add(idListPath);
			CascadeSVMIOHelper.writeIdListHadoop(idListPath, subsets.get(i));
		}
		CascadeSVMIOHelper.writeSubsetListHadoop(subsetListPath, subsetList);
		return subsetListPath;
	} 
	
	public static ArrayList< ArrayList<Integer> > partitionIdList(ArrayList<Integer> idList, int nSubset) {
		ArrayList< ArrayList<Integer> > subsets = new ArrayList< ArrayList<Integer> >();
		for (int i = 0; i < nSubset; i++) {
			subsets.add(new ArrayList<Integer>());
		}
		for (int i = 0; i < idList.size(); i++) {
			int subsetId = rand.nextInt(nSubset);
			subsets.get(subsetId).add(idList.get(i));
		}
		return subsets;
	}
}
