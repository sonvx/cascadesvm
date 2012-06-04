package hadoop;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import local.Merger;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
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
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.log4j.Logger;




@SuppressWarnings("deprecation")
public class MergerHadoop {
	public static class MergerMapper extends MapReduceBase
			implements Mapper<IntWritable, Text, IntWritable, IntWritable> {

		protected static Logger logger = Logger.getLogger(MergerMapper.class);
		private int total_iter;
		private String in_small_kernel_dir;
		private int total_sample_matrix_a;
		private Merger merger;
		private FileSystem fs;
		
		public void configure(JobConf job) {

				total_iter = Integer.parseInt(job.get("total_iter"));
				total_sample_matrix_a = Integer.parseInt(job.get("total_sample_matrix_a"));
				in_small_kernel_dir = job.get("in_small_kernel_dir");
				merger = new Merger();
				try {
					fs = FileSystem.get(job);
				} catch (IOException e) {
					e.printStackTrace();
				}
				logger.info( "finished construction!\r\n");
				printMemory();
		}

		@Override
		public void map(IntWritable key, Text values,
				OutputCollector<IntWritable, IntWritable> output,
				Reporter reporter) throws IOException {

		
			logger.info( "in:"+key.toString()+"\r\n");
			
			//merger.mergeHadoopSequence(in_small_kernel_dir, key.get(), total_iter,  total_sample_matrix_a,job,fs,logger);
			int cur_block_id = key.get();
			Path[] pathes = new Path[total_iter];
			for(int j = 0 ; j < pathes.length ; j++) {
				pathes[j] = new Path(in_small_kernel_dir + File.separator + "out" + "(" + j + "-" + cur_block_id +")");
			}
			merger.mergeHadoopBinary(pathes, new Path(in_small_kernel_dir +  File.separator + "fout" + cur_block_id), total_sample_matrix_a, fs,logger);
			
			
			logger.info( "finished merge!\r\n");
			output.collect(key, new IntWritable(0));


		}
		
		public void printMemory() {
			logger.info(":max-memory:"+(Runtime.getRuntime().maxMemory()/1024/1024)+
					"m:free-memory:"+(Runtime.getRuntime().freeMemory()/1024/1024)+
					"m:total"+(Runtime.getRuntime().totalMemory()/1024/1024)+"\r\n");
		}

	}
	
	
	/**
	 * dummy reducer
	 * @author lujiang
	 *
	 */
	public static class MergerReducer extends MapReduceBase implements
			Reducer<IntWritable, IntWritable, IntWritable, Text> {

		protected static Logger logger = Logger.getLogger(MergerReducer.class);

		public void configure(JobConf job) {
			
		}

		@Override
		public void reduce(IntWritable key, Iterator<IntWritable> values,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {

			long allocatedJVMMemory = Runtime.getRuntime().totalMemory();
			logger.info(key.toString() + ":memory:" + allocatedJVMMemory + "\r\n");
			output.collect(key, new Text("ok"));
		}

	}

	public static void main(String[] args) throws Exception {
		
		
			//String in_dir = args[0];
			String out_dir = args[1];
			String in_small_kernel_dir = args[2];
			int total_iter = Integer.parseInt(args[3]);
			int total_sample_matrix_a = Integer.parseInt(args[4]);	//236697
			
			int total_sample_matrix_b = 10000;
			int block_size_matrix_b = 300;
			
			Configuration pconf = new Configuration();
			FileSystem fs = FileSystem.get(pconf);
				
			
			//fs.create(new Path(out_dir + File.separator + (total_iter+1)));
			Path inpath = new Path(out_dir + File.separator + (total_iter+1)+"merge-in");
			SequenceFile.Writer writer = SequenceFile.createWriter(fs, pconf, inpath, IntWritable.class,  Text.class);
			String nonsense = "very long non sensevery long non sensevery long non sensevery long non sensevery long non sensevery long non sensevery long non sensevery long non sensevery long non sensevery long non sensevery long non sensevery long non sensevery long non sense" +
					"very long non sensevery long non sensevery long non sensevery long non sensevery long non sensevery long non sensevery long non sensevery long non sensevery long non sensevery long non sensevery long non sensevery long non sensevery long non sensevery long non sensevery long non sense" +
					"very long non sensevery long non sensevery long non sensevery long non sensevery long non sensevery long non sensevery long non sensevery long non sensevery long non sensevery long non sensevery long non sensevery long non sensevery long non sensevery long non sense" +
					"very long non sensevery long non sensevery long non sensevery long non sensevery long non sensevery long non sensevery long non sensevery long non sensevery long non sensevery long non sensevery long non sensevery long non sense" +
					"very long non sensevery long non sensevery long non sensevery long non sensevery long non sensevery long non sensevery long non sensevery long non sensevery long non sensevery long non sensevery long non sensevery long non sensevery long non sensevery long non sense" +
					"very long non sensevery long non sensevery long non sensevery long non sensevery long non sensevery long non sensevery long non sensevery long non sensevery long non sensevery long non sensevery long non sense" +
					"very long non sensevery long non sensevery long non sensevery long non sensevery long non sensevery long non sensevery long non sensevery long non sense!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!";
			
			nonsense = nonsense+nonsense+nonsense+nonsense+nonsense+nonsense+nonsense+nonsense+nonsense;
			nonsense = nonsense+nonsense+nonsense+nonsense+nonsense+nonsense+nonsense+nonsense+nonsense;
			nonsense = nonsense+nonsense+nonsense+nonsense+nonsense+nonsense+nonsense+nonsense+nonsense;
			for(int i = 0 ; i < (int) Math.ceil(total_sample_matrix_b/block_size_matrix_b) ; i++) {
				writer.append(new IntWritable(i), new Text(nonsense+i));
			}
			writer.close();
		
		
		
			

			//write the input file
			JobConf conf = new JobConf(MergerHadoop.class);
			conf.setJobName("hadoop-mergerv3");
			conf.set("mapred.child.java.opts", "-Xmx3000m");		//cannot be too large <2000m
			
			conf.set("mapred.cluster.map.memory.mb","4000");
			conf.set("mapred.cluster.reduce.memory.mb","4000");
			
			conf.set("mapred.job.map.memory.mb","4000");
			conf.set("mapred.job.reduce.memory.mb","4000");
			conf.set("mapred.map.max.attempts","4");
			// conf.set("mapreduce.map.java.opts","-Xmx2048m");
			// conf.set("mapreduce.job.reduce.memory.mb","2048");

			// out key and value for mapper
			conf.setMapOutputKeyClass(IntWritable.class);
			conf.setMapOutputValueClass(IntWritable.class);
			// out key and value for reducer
			conf.setOutputKeyClass(IntWritable.class);
			conf.setOutputValueClass(Text.class);

			conf.setMapperClass(MergerMapper.class);
			conf.setReducerClass(MergerReducer.class);
			
			conf.setNumMapTasks(300);
			conf.setNumReduceTasks(1);

			conf.setInputFormat(SequenceFileInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);
			
			
			
			FileInputFormat.setInputPaths(conf, inpath);
			//fs.create(new Path(out_dir + File.separator + (total_iter+2)));
			FileOutputFormat.setOutputPath(conf, new Path(out_dir + File.separator + (total_iter+2)));
			
			
			conf.set("total_sample_matrix_a", "" + total_sample_matrix_a);
			conf.set("total_iter", "" + total_iter);
			conf.set("in_small_kernel_dir", in_small_kernel_dir);
			
			JobClient.runJob(conf);
	}

}
