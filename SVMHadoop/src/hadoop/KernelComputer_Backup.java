package hadoop;


import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import local.KernelCalculator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.TwoDArrayWritable;
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


@SuppressWarnings("deprecation")
public class KernelComputer_Backup {

	public static class KernelComputerMapper extends MapReduceBase implements
			Mapper<IntWritable, KernelRowArrayWritable, IntWritable, FloatMatrix> {
		
		protected static Logger logger = Logger.getLogger(KernelComputerMapper.class);
	
		private int id;
		private FileSystem fs;
		private KernelCalculator calculator;
	       
		public void configure(JobConf job) {
			try {
				logger.info("memory for this task:" + job.getMemoryForMapTask() +"\r\n");
				logger.info("memory for mapper:" + job.getMemoryForMapTask() +"\r\n");
				logger.info("memory for reducer:" + job.getMemoryForReduceTask() +"\r\n");
				logger.info(id+":start:"+System.currentTimeMillis()+"\r\n");
				printMemory();
				calculator = new KernelCalculator();
				fs = FileSystem.get(job);
				Path filepath = new Path(job.get("iteration_filename"));
				String filename = filepath.getName();
				logger.info(filename +"\r\n");
				int lineno = Integer.parseInt(filename.substring(filename.lastIndexOf("-")+1, filename.length()));
				logger.info(id+":lineno:"+lineno+"\r\n");
				DataInputStream br = new DataInputStream(new BufferedInputStream(fs.open(filepath),1024*8*16));
			
				//read the block features from hdfs
				printMemory();
				logger.info(id+":read block\r\n");
				
				
				calculator.inmatrix = new KernelRow[lineno];
				
				for(int i = 0 ; i < lineno ; i ++) {

					int thislineno = br.readInt();
					int arraysize = br.readInt();
					KernelRow thisrow = new KernelRow(thislineno, arraysize);
					for(int j = 0 ; j < arraysize ; j ++) {
						thisrow.indexes[j] = br.readShort();
						thisrow.values[j] = br.readFloat();
					}
					calculator.inmatrix[i] = thisrow;
					if(i%1000==0) {
						logger.info("readed 1000"+"\r\n");
						printMemory();
					}
				}
				
				
				
				br.close();
				
				logger.info(id+":read done\r\n");
				printMemory();
				
				
			
				/*logger.info(id+"inmatrix length:" + calculator.inmatrix.length +":\r\n");
				logger.info(id+"inmatrix[0] lineid:" + calculator.inmatrix[0].lineid +":\r\n");
				logger.info(id+"inmatrix[0] length:" + calculator.inmatrix[0].indexes.length +":\r\n");
				logger.info(id+"inmatrix[250] lineid:" + calculator.inmatrix[250].lineid +":\r\n");
				logger.info(id+"inmatrix[250] length:" + calculator.inmatrix[250].indexes.length +":\r\n");
				logger.info(id+calculator.inmatrix[0].toString(10)+":\r\n");
				*/
				
				
			} catch (IOException e) {
				e.printStackTrace();
			}
	    }
		
		public void printMemory() {
			logger.info(id+":max-memory:"+(Runtime.getRuntime().maxMemory()/1024/1024)+
					"m:free-memory:"+(Runtime.getRuntime().freeMemory()/1024/1024)+
					"m:total"+(Runtime.getRuntime().totalMemory()/1024/1024)+"\r\n");
		}
		
		
		
		@Override
		public void map(IntWritable key, KernelRowArrayWritable values,
				OutputCollector<IntWritable, FloatMatrix> output, Reporter reporter)
				throws IOException {
			logger.info(id+":entered map loop\r\n");
			printMemory();
			
			//logger.info(id+":input:\r\n");
			Writable[] input = values.get();
			FloatWritable[][] results = new FloatWritable[input.length][];
			
			
			logger.info(id+":calculating the kernel\r\n");
			printMemory();
			for(int i = 0 ; i < input.length ; i++) {
				KernelRowWritable thisWritable = (KernelRowWritable)input[i];
				KernelRow row = thisWritable.toKernelRow();
				results[i] = calculator.chi2Writable(row);
				reporter.setStatus("<br>\n I am alive and don't kill me...");
				//logger.info(id+":column:"+calculator.inmatrix.length+"\r\n");
				
			}
			
			logger.info(id+":calculation done\r\n");
			printMemory();
			
			logger.info(id+":end:"+System.currentTimeMillis()+"\r\n");
			//String s = buffer.substring(0,100);
			FloatMatrix outvalue = new FloatMatrix();
			outvalue.set(results);
			output.collect(key, outvalue);
			//logger.error(id+":output:" +values.toString() + " "+ s+"\r\n");
			//buffer.delete(0, buffer.length());
			// parse the load feature file

		}

	}
	

	public static class KernelComputerReducer extends MapReduceBase implements
			Reducer<IntWritable, FloatMatrix, IntWritable, Text> {
		
		private FileSystem fs;
		protected static Logger logger = Logger.getLogger(KernelComputerReducer.class);
		private int id;
		public void configure(JobConf job) {
			try {
				String mapTaskId = job.get("mapred.task.id");
				id = Integer.parseInt(mapTaskId);
				fs = FileSystem.get(job);
			
			} catch (IOException e) {
				e.printStackTrace();
			}
	    }
		
		@Override
		public void reduce(IntWritable key, Iterator<FloatMatrix> values,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {
			
			long allocatedJVMMemory = Runtime.getRuntime().totalMemory();
			logger.info(id+":memory:"+allocatedJVMMemory+"\r\n");
			/*Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, new Path(out.getAbsolutePath()),IntWritable.class,  TwoDArrayWritable.class);
			*/
			
			Path outFile = new Path("/user/lujiang/SIN-OUT/out" + "("+id +"-" + key.get() +")");
			//FSDataOutputStream out = fs.create(outFile);
			FloatMatrix finalresult = new FloatMatrix();
			while (values.hasNext()) {
				finalresult = values.next();
				break;
			}
			
			writeMatrixSequenceFile(outFile,key, finalresult);

			//output.collect(key, new Text(buffer.toString()));
			output.collect(key, new Text("ok"));
		}
		
		
		protected void writeMatrixString(FSDataOutputStream out, TwoDArrayWritable inmatrix) throws IOException {
			Writable[][] matrix = inmatrix.get();
			for(int i = 0 ; i < matrix.length ; i++) {
				for(int j = 0 ; j < matrix[i].length ; j++) {
					float thisvalue = ((FloatWritable)matrix[i][j]).get();
					String temp = ""+thisvalue+" ";
					out.writeBytes(temp);
				}
				out.writeBytes("\r\n");
			}
			out.flush();
			out.close();
		}
		
		
		protected void writeMatrixSequenceFile(Path out, IntWritable key, FloatMatrix inmatrix) throws IOException {
			Configuration conf = new Configuration();
			SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, out,IntWritable.class,  FloatMatrix.class);
			writer.append(key, inmatrix);
			writer.close();
		}
	}
	
	public static void main(String[] args) throws Exception {
		//String [] filenames = new String[]{"SIN-SIFT/test-inpart0-1500"};
		
		String [] filenames = new String[]{"SIN-FEATURES-SIFT/sift-inpart3-29999"};
		for(int i = 0 ; i < 1 ; i ++) {
			JobConf conf = new JobConf(KernelComputer_Backup.class);
			
			conf.set("mapred.child.java.opts", "-Xmx2048m");		//cannot be too large <2000m
			//conf.set("mapred.map.java.opts", "-Xmx1024m");
			
			conf.set("mapred.cluster.map.memory.mb","3000");
			conf.set("mapred.cluster.reduce.memory.mb","1024");
			
			conf.set("mapred.job.map.memory.mb","3000");
			conf.set("mapred.job.reduce.memory.mb","1024");
			conf.set("mapred.tasktracker.map.tasks.maximum","1");
			conf.set("mapred.map.max.attempts","4");
			
			conf.setJobName("hadoop-svm");
			//conf.set("mapreduce.map.java.opts","-Xmx2048m");
			//conf.set("mapreduce.job.reduce.memory.mb","2048");

			
			//out key and value for mapper
			conf.setMapOutputKeyClass(IntWritable.class);
			conf.setMapOutputValueClass(FloatMatrix.class);
			//out key and value for reducer
			conf.setOutputKeyClass(IntWritable.class);
			conf.setOutputValueClass(Text.class);
			
			conf.setMapperClass(KernelComputerMapper.class);
			conf.setReducerClass(KernelComputerReducer.class);
			conf.setNumMapTasks(1);
			conf.setNumReduceTasks(1);
			
			conf.setInputFormat(SequenceFileInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);
			
			FileInputFormat.setInputPaths(conf, new Path(args[0]));
			FileOutputFormat.setOutputPath(conf, new Path(args[1]+File.separator+i));
			conf.set("iteration_num", ""+i);
			conf.set("iteration_filename", filenames[i]);
			JobClient.runJob(conf);
		}
		
		
		
		




	}
	
	
}
