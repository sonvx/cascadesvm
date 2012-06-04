package hadoop;


import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import local.BoostingSVM;
import local.KernelCalculator;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.log4j.Logger;

import beans.FloatMatrix;
import beans.KernelRow;
import beans.KernelRowArrayWritable;
import beans.KernelRowWritable;
import beans.svm_node;


@SuppressWarnings("deprecation")
public class SVMHadoop {

	public static class SVMHadoopMapper extends MapReduceBase implements
			Mapper<IntWritable, KernelRowArrayWritable, IntWritable, FloatMatrix> {
		
		protected static Logger logger = Logger.getLogger(SVMHadoopMapper.class);
	
		private int id;
		private FileSystem fs;
		private KernelCalculator calculator;
		private Path[] pathes;
		private int row_a;
	
	       
		public void configure(JobConf job) {
			try {
				logger.info("memory for this task:" + job.getMemoryForMapTask() +"\r\n");
				logger.info("memory for mapper:" + job.getMemoryForMapTask() +"\r\n");
				logger.info("memory for reducer:" + job.getMemoryForReduceTask() +"\r\n");
				row_a = Integer.parseInt(job.get("row_a"));
				pathes = this.topathes(job.get("in_a_pathes"));
				
				logger.info("row_a:" + row_a +"\r\n");
				logger.info("pathes[0]:" + pathes[0] +"\r\n");
				logger.info("pathes[pathes.length-1]:" + pathes[pathes.length-1] +"\r\n");

				fs = FileSystem.get(job);
				calculator = new KernelCalculator();
				printMemory();
			
				
			} catch (IOException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
	    }
		
		
		public void loadAMatrix(Path filepath) throws IOException {
			calculator.inmatrix = null;
			System.gc();		//control the memory while loading the block
			
			String filename = filepath.getName();
			int lineno = Integer.parseInt(filename.substring(filename.lastIndexOf("-")+1, filename.length()));
			calculator.inmatrix = new KernelRow[lineno];
			DataInputStream br = new DataInputStream(new BufferedInputStream(fs.open(filepath),1024*8*16));
			
			for(int i = 0 ; i < lineno ; i ++) {
				int thislineno = br.readInt();
				int arraysize = br.readInt();
				KernelRow thisrow = new KernelRow(thislineno, arraysize);
				for(int j = 0 ; j < arraysize ; j ++) {
					thisrow.indexes[j] = br.readShort();
					thisrow.values[j] = br.readFloat();
				}
				calculator.inmatrix[i] = thisrow;

					
			}
			logger.info("loaded "+filename+":"+lineno);
			printMemory();
			br.close();
			
		}
		
	
		
		
		
		@Override
		public void map(IntWritable key, KernelRowArrayWritable values,
				OutputCollector<IntWritable, FloatMatrix> output, Reporter reporter)
				throws IOException {
			logger.info(id+"--------------------------------:entered map loop-----------------------------------\r\n");
			printMemory();
			Writable[] input = values.get();
			if(input == null || input.length == 0) {
				output.collect(key, null);
				return;
			}
			
			float[][] small = new float[input.length][];
			logger.info("before create big matrix");
			printMemory();
			float[][] big = new float[input.length][row_a];
			logger.info("after create big matrix");
			printMemory();
			FloatWritable[][] results = new FloatWritable[input.length][row_a];
			logger.info("after create floatwritable matrix");
			printMemory();
			
			KernelRow[] inputB = new KernelRow[input.length];
			for(int i = 0 ; i < inputB.length ; i++) {
				KernelRowWritable thisWritable = (KernelRowWritable)input[i];
				KernelRow row = thisWritable.toKernelRow();
				inputB[i] = row;
				
			}
		
			
			logger.info(id+":calculating the kernel\r\n");
			logger.info("input:" + input.length);
			
			for(int p = 0  ; p < pathes.length ; p++) {
				loadAMatrix(pathes[p]);		//load A block
				for(int i = 0 ; i < input.length ; i++) {
					small[i] = calculator.chi2(inputB[i]);			//calculate the kernel	
				}
				
				reporter.setStatus("<br>\n I am still alive. Don't kill me...");
				logger.info("just calculated[" + calculator.inmatrix.length +"*" + inputB.length+"]");
				printMemory();
				addAppendToBig(big, small, p*small[0].length);
			}
			
			logger.info(id+":calculation done\r\n");
			printMemory();
			small = null;
			//check point test!
			//check point test!
			//check point test!
			//check point test!
			
			
			//convert the float[][] bigmatrix to FloatMatrix
			FloatMatrix outvalue = new FloatMatrix();
			for(int i = 0 ; i < big.length ; i++) {
				for(int j = 0 ; j < big[i].length ; j++) {
					results[i][j] = new FloatWritable(big[i][j]);
				}
			}
			outvalue.set(results);
			
			output.collect(key, outvalue);
			big = null;
			System.gc();

		}
		
		public void printMemory() {
			logger.info(id+":max-memory:"+(Runtime.getRuntime().maxMemory()/1024/1024)+
					"m:free-memory:"+(Runtime.getRuntime().freeMemory()/1024/1024)+
					"m:total"+(Runtime.getRuntime().totalMemory()/1024/1024)+"\r\n");
		}
		
		
		private void addAppendToBig(float[][] big, float[][] small, int startIndex) {
			for(int i = 0 ; i < big.length ; i++) {
				for(int j = 0 ; j < small[i].length ; j++) {
					big[i][startIndex+j] = small[i][j];		
				}
			}
		}
		
		private Path[] topathes(String in) {
			String[] temp = in.split("#");
			Path[] result = new Path[temp.length];
			for(int i = 0 ; i < temp.length ; i++) {
				result[i] = new Path(temp[i]);
			}
			
			return result;
		}
	}
	

	public static class SVMHadoopReducer extends MapReduceBase implements
			Reducer<IntWritable, FloatMatrix, IntWritable, Text> {
		
		protected static Logger logger = Logger.getLogger(SVMHadoopReducer.class);
		private BoostingSVM predictor;
		private FileSystem fs;
		private int b_block_size;
		private String model_location;
		
		public void configure(JobConf job) {
			try {
				
				fs = FileSystem.get(job);
				b_block_size = Integer.parseInt(job.get("b_block_size"));
				model_location = job.get("model_location");
				predictor = new BoostingSVM(fs.open(new Path(model_location)));
				
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		
			
			
			logger.info("loaded model:"+predictor.models.length+"\r\n");
			logger.info("model_location:" + model_location +"\r\n");	
			logger.info("b_block_size:" + b_block_size +"\r\n");
			printMemory();
	    }
		
		@Override
		public void reduce(IntWritable key, Iterator<FloatMatrix> values,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {
			logger.info("--------------------------------:entered reducer loop-----------------------------------\r\n");
			printMemory();
			//FSDataOutputStream out = fs.create(outFile);
			if(values == null)	return;
			if(values.hasNext()) {				
				logger.info("loaded model:"+predictor.models.length+"\r\n");
				logger.info("model_location:" + model_location +"\r\n");	
				logger.info("b_block_size:" + b_block_size +"\r\n");
				
				Path outFile = new Path("/user/lujiang/SIN-OUT/prediction" + "(" + key.get() +")");
				DataOutputStream  bw = new DataOutputStream(new BufferedOutputStream(fs.create(outFile)));
				Writable[][] big = values.next().get();
				
				for(int i = 0 ; i < big.length ; i++) {
					svm_node[] t = new svm_node[big[0].length+1];
					t[0] = new svm_node();
					t[0].index = 0;
					t[0].value = key.get()*b_block_size+i;	//#sample id
					for(int j = 1 ; j < t.length ; j++) {
						t[j] = new svm_node();
						t[j].index = j;
						t[j].value = ((FloatWritable)big[i][j-1]).get();
					}
					
					double[] tprediction = predictor.predictHadoop(t);
					writePrediction(bw, tprediction, key.get()*b_block_size+i);
					logger.info("predicted" + i);
					printMemory();
				}
				
				bw.flush();
				bw.close();		//close the local file
				
				//output.collect(key, new Text(buffer.toString()));
				output.collect(key, new Text("ok"));
			}
		}
		
		protected void writePrediction(DataOutputStream out, double[] predictions, int lineno) throws IOException {
			for(int i = 0 ; i < predictions.length ; i++) {
					out.writeDouble(predictions[i]);
			}
		}
		
		public void printMemory() {
			logger.info("max-memory:"+(Runtime.getRuntime().maxMemory()/1024/1024)+
					"m:free-memory:"+(Runtime.getRuntime().freeMemory()/1024/1024)+
					"m:total"+(Runtime.getRuntime().totalMemory()/1024/1024)+"\r\n");
		}
		
	}
	
	
	private static String sortAPath(FileSystem fs, String a_feature_binary_file) throws Exception {
		FileStatus[] status = fs.listStatus(new Path(a_feature_binary_file));
		ArrayList<String> a_feature_binary_name = new ArrayList<String>();
		for (int i = 0; i < status.length; i++) {
			Path thispath = status[i].getPath();
			if (thispath.getName().indexOf("inpart") != -1) {
				a_feature_binary_name.add(thispath.toString());
			}
		}
		ArrayList<Integer> a_feature_ids = new ArrayList<Integer>();

		// extract small file id from the a_feature_binary file name
		for (int i = 0; i < a_feature_binary_name.size(); i++) {
			String tfilename = a_feature_binary_name.get(i);
			int id = -1;
			try{
				id = Integer.parseInt(tfilename.substring(
						tfilename.indexOf("inpart") + "inpart".length(),
						tfilename.lastIndexOf("-")));
			} catch(NumberFormatException e) {
				throw new Exception("The input A feature file name is bad formateed:" + tfilename +"\r\n" + e.toString());
			}
			a_feature_ids.add(id);

		}

		// sort the a_feature_binary according to the id! index starting from 0
		String result = "";
		for (int i = 0; i < a_feature_binary_name.size(); i++) {
			boolean found = false;
			int j = 0;
			for (; j < a_feature_ids.size(); j++) {
				if (i == a_feature_ids.get(j)) {
					found = true;
					break;
				}
			}
			if (found) {
				result += a_feature_binary_name.get(j) + "#";
			} else {
				throw new Exception(
						"the input A feature is not consecutive! Missing:" + i);
			}
		}
		result = result.substring(0, result.length() - 1);
		return result;
		
	}
	
	public static void main(String[] args) throws Exception {
		//String [] filenames = new String[]{"SIN-SIFT/test-inpart0-1500"};
		
		String a_feature_binary_file = args[0];			//small a matrix dir
		String b_feature_sequence_file = args[1];		//sequence file of b
		
		JobConf conf = new JobConf(SVMHadoop.class);
		conf.set("model_location", args[2]);			//sin model location
		conf.set("row_a", args[3]);						//number of row a		236697
		conf.set("b_block_size", args[4]);				//the block size for sift or mosift or csift. not 3*the size
		
		FileSystem fs = FileSystem.get(conf);
		String in_a_pathes = sortAPath(fs,a_feature_binary_file);						//the pathes of all small a matrix
		
		//System.out.println(in_a_pathes);
		
        conf.set("in_a_pathes", in_a_pathes);	
		conf.set("mapred.child.java.opts", "-Xmx1200m");		//cannot be too large <2000m
		//conf.set("mapred.map.java.opts", "-Xmx1024m");
			
		conf.set("mapred.cluster.map.memory.mb","2000");
		conf.set("mapred.cluster.reduce.memory.mb","2000");
			
		conf.set("mapred.job.map.memory.mb","2000");
		conf.set("mapred.job.reduce.memory.mb","2000");
		conf.set("mapred.tasktracker.map.tasks.maximum","1");
		conf.set("mapred.map.max.attempts","4");
			
		conf.setJobName("casacade-svm");
			
			
		//out key and value for mapper
		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(FloatMatrix.class);
		//out key and value for reducer
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);
			
		conf.setMapperClass(SVMHadoopMapper.class);
		conf.setReducerClass(SVMHadoopReducer.class);
		conf.setNumMapTasks(350);
		conf.setNumReduceTasks(60);
			
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
			
		//String thispathname = pathes.get(i).toString();
		//int iteration_num = Integer.parseInt(thispathname.substring(thispathname.indexOf("inpart")+"inpart".length(), thispathname.lastIndexOf("-")));
		FileInputFormat.setInputPaths(conf, new Path(b_feature_sequence_file));
		
		FileOutputFormat.setOutputPath(conf, new Path("out"+File.separator+1688));;
	
		JobClient.runJob(conf);
		


	}
	
	
}
