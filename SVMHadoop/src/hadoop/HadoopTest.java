package hadoop;


import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import local.KernelCalculator;
import local.KernelProjector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
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
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.log4j.Logger;

import beans.FloatMatrix;
import beans.KernelRow;
import beans.KernelRowArrayWritable;
import beans.KernelRowWritable;

@SuppressWarnings("deprecation")
public class HadoopTest {

	public static class SVMHadoopMapper extends MapReduceBase implements
			Mapper<IntWritable, KernelRowArrayWritable, IntWritable, Text> {
		
		protected static Logger logger;
	
		private FileSystem fs;
		private KernelCalculator calculator;
		private Path[] pathes;
		private String kernelPath;
		private String buffer;					//the directory of the buffer
		private int row_a;						//the number of row in A matrix
		private String kernel_type;				//kernel type
		public boolean debug = true;			
	
		
		
	    
		public void configure(JobConf job) {
			try {
				logger = Logger.getLogger(SVMHadoopMapper.class);
				if(debug)	logger.info("memory for this task:" + job.getMemoryForMapTask() +"\r\n");
				if(debug)	logger.info("memory for mapper:" + job.getMemoryForMapTask() +"\r\n");
				if(debug)	logger.info("memory for reducer:" + job.getMemoryForReduceTask() +"\r\n");
				kernelPath = job.get("in_a_pathes");
				row_a = Integer.parseInt(job.get("row_a"));
				if(debug)   logger.info("ROW A:" + row_a +"\r\n");
				if(debug)   logger.info("kernel Path:" + kernelPath +"\r\n");
				
				
				fs = FileSystem.get(job);
				if(debug)	printMemory();
				
				
				
				
			
				
			} catch (IOException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
	    }
		
		
	
		
		
		@Override
		public void map(IntWritable key, KernelRowArrayWritable values,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {
			
			logger.info("--------------------------------:entered map loop-----------------------------------\r\n");
			KernelProjector projector = new KernelProjector();
			ArrayList<Integer> idlist = new ArrayList<Integer>();
			idlist.add(512);
			idlist.add(0);
			idlist.add(1);
			idlist.add(1707);
			idlist.add(1807);
			idlist.add(1107);
			idlist.add(7);
			idlist.add(104);
			idlist.add(107);
			idlist.add(707);
			
			printMemory();
			/*float[][] x = projector.projectHadoop(fs, kernelPath, idlist, row_a);
			printMemory();
			for(int i = 0 ; i < x.length ; i++) {
				for(int j = 0 ; j < x[i].length ; j++) {
					logger.info(x[i][j]+ " ");
				}
				logger.info("\n");
			}
		*/
			printMemory();
			logger.info("--------------------------------Map ok-----------------------------------\r\n");
			output.collect(key, new Text("ok"));
			System.gc();


		}
		
		public void printMemory() {
			logger.info(":max-memory:"+(Runtime.getRuntime().maxMemory()/1024/1024)+
					"m:free-memory:"+(Runtime.getRuntime().freeMemory()/1024/1024)+
					"m:total:"+(Runtime.getRuntime().totalMemory()/1024/1024)+"m\r\n");
		}
		
		
		
	}
	
	/**
	 * dummy reducer
	 * @author lujiang
	 *
	 */
	public static class SVMHadoopReducer extends MapReduceBase implements
			Reducer<IntWritable, FloatMatrix, NullWritable, NullWritable> {
		
		protected static Logger logger;
		
		public void configure(JobConf job) {
			logger = Logger.getLogger(SVMHadoopReducer.class);
	    }
		
		@Override
		public void reduce(IntWritable key, Iterator<FloatMatrix> values,OutputCollector<NullWritable, NullWritable> output, Reporter reporter) throws IOException {
				//output.collect(key, new Text("ok"));
				logger.info("--------------------------------Reduce ok-----------------------------------\r\n");
		}
	}
	
	
	
	
	
	public static void main(String[] args) throws Exception {
		
		String a_feature_binary_file = args[0];			//small a matrix dir
		String b_feature_sequence_file = args[1];		//sequence file of b
	/*	
		Configuration conf1 = new Configuration(); 
				
		FileSystem fs = FileSystem.get(conf1);
				
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf1, new Path("temp/filesystem.test1"),Text.class,  Text.class);
		writer.append(new Text("AAA"), new Text("AAA"));
		writer.close();
		*/
		
		
		
		JobConf conf = new JobConf(HadoopTest.class);
		conf.set("row_a", args[2]);						//number of row a		236697
		conf.set("kernelmatrix_buffer", args[3]);		//buffer that contains the kernel matrix chunk
		
		String in_a_pathes = a_feature_binary_file;						//the pathes of all small a matrix
		
		//System.out.println(in_a_pathes);
		
        conf.set("in_a_pathes", in_a_pathes);
    	conf.set("mapred.child.java.opts", "-Xmx256m");		//cannot be too large <2000m
		
		conf.set("mapred.cluster.map.memory.mb","1000");
		conf.set("mapred.cluster.reduce.memory.mb","1000");
		
		conf.set("mapred.job.map.memory.mb","1000");
		conf.set("mapred.job.reduce.memory.mb","1000");
		conf.set("mapred.tasktracker.map.tasks.maximum","1");
		conf.set("mapred.map.max.attempts","8");
		conf.set("mapred.reduce.max.attempts","8");
			
		conf.setJobName("Hadoop Tester");
			
			
		//out key and value for mapper
		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(Text.class);
		//out key and value for reducer
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);
			
		conf.setMapperClass(SVMHadoopMapper.class);
		conf.setReducerClass(SVMHadoopReducer.class);
		conf.setNumMapTasks(128);
		conf.setNumReduceTasks(1);
		
		
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(NullOutputFormat.class);
			
		//String thispathname = pathes.get(i).toString();
		//int iteration_num = Integer.parseInt(thispathname.substring(thispathname.indexOf("inpart")+"inpart".length(), thispathname.lastIndexOf("-")));
		FileInputFormat.setInputPaths(conf, new Path(b_feature_sequence_file));
		//FileOutputFormat.setOutputPath(conf, new Path(args[4]));
		if(args.length ==6) {
			conf.set("kernel_type", args[5].toLowerCase());
		} else {
			conf.set("kernel_type", "intersection");
		}
		
		
		
		
		JobClient.runJob(conf);
		


	}
	
	
}
