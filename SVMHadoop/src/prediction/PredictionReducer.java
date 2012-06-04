package prediction;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import local.BoostingSVM;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
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

import beans.svm_node;




@SuppressWarnings("deprecation")
public class PredictionReducer {
	
	/**
	 * dummy mapper
	 * @author lujiang
	 *
	 */
	public static class MyMapper extends MapReduceBase
			implements Mapper<IntWritable, Text, IntWritable, IntWritable> {

		public void configure(JobConf job) {

		}

		@Override
		public void map(IntWritable key, Text values,
				OutputCollector<IntWritable, IntWritable> output,
				Reporter reporter) throws IOException {

			output.collect(key, new IntWritable(0));
		}

	}
	
	
	/**
	 * @author lujiang
	 *
	 */
	public static class MyReducer extends MapReduceBase implements
			Reducer<IntWritable, IntWritable, IntWritable, Text> {

		protected static Logger logger = Logger.getLogger(MyReducer.class);
		private BoostingSVM predictor;
		private FileSystem fs;
		private int id;
		private int row_a;
		private int total_reducer_num;
		private int b_block_size;
		private Path buffer;
		private String model_location;
		private String sin_prediction_location;
		private HashMap<String, String> blacklist;

		public void configure(JobConf job) {
			try {
				
				fs = FileSystem.get(job);
				String attemptname = job.get("mapred.task.id");
				id = Integer.parseInt(attemptname.substring(attemptname.lastIndexOf("_r_")+3,attemptname.lastIndexOf("_")));
				logger.info("attep:"+attemptname+"\r\n");
				b_block_size = Integer.parseInt(job.get("b_block_size"));
				model_location = job.get("model_location");
				predictor = new BoostingSVM(fs.open(new Path(model_location)));
				row_a = Integer.parseInt(job.get("row_a"));
				total_reducer_num = Integer.parseInt(job.get("total_reducer_num"));
				buffer = new Path(job.get("kernelmatrix_buffer"));
				sin_prediction_location = job.get("sin_prediction_location");
				blacklist = new HashMap<String, String>();
				
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		
			
			logger.info("id:"+id+"\r\n");
			logger.info("loaded model:"+predictor.models.length+"\r\n");
			logger.info("model_location:" + row_a +"\r\n");	
			logger.info("model_location:" + model_location +"\r\n");	
			logger.info("b_block_size:" + b_block_size +"\r\n");
			printMemory();
		}
		
		
		public void printMemory() {
			logger.info(":max-memory:"+(Runtime.getRuntime().maxMemory()/1024/1024)+
					"m:free-memory:"+(Runtime.getRuntime().freeMemory()/1024/1024)+
					"m:total:"+(Runtime.getRuntime().totalMemory()/1024/1024)+"m\r\n");
		}

		@Override
		public void reduce(IntWritable key, Iterator<IntWritable> values,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {
			logger.info("--------------------------------:entered reducer loop-----------------------------------\r\n");
			printMemory();
			
			
			while(true) {
			
					//lookup the available files in the buffer
			        FileStatus[] status = fs.listStatus(buffer);
			    	try {
						Thread.sleep(30000);	//sleep awhile to ensure the file is written out
					} catch (InterruptedException e) {
						e.printStackTrace();
					}			
			    	
			        for(int i = 0 ; i < status.length ; i++) {
			        	String thisname = status[i].getPath().getName();
			        	int b_block_id;
			        	reporter.setStatus("<br>\n I am processing.");
			        	if(blacklist.get(thisname) != null)		continue;	//skip this problematic file
			        	if(thisname.startsWith("kernelmatrix")) {
			        		
			        		int pos1 = thisname.indexOf("-c");
			        		int pos2 = thisname.indexOf("-r");
			        		b_block_id = Integer.parseInt(thisname.substring(pos1+2, pos2));
			        		int row_cnt = Integer.parseInt(thisname.substring(pos2+2, thisname.length()));
			        		if(b_block_id%total_reducer_num ==id) {
			        			//1) The reducer is responsible for the current file
			        			logger.info("find and process the file:" + thisname);
			        			double[][] result = new double[row_cnt][];
			        			DataInputStream br;
			        			try {
			        				FSDataInputStream in = fs.open(status[i].getPath());
			        				br = new DataInputStream(new BufferedInputStream(in));
			        			} catch (IOException e) {
			    					e.printStackTrace();
			    					logger.info("Cannot read the file" + status[i].getPath() + ". Break!");
			    					blacklist.put(thisname, "bad");
			    					break;
			    				}
			        			
			        			try{
				        			for(int p = 0 ; p < row_cnt ; p++) {
				        				svm_node[] t = new svm_node[row_a+1];
				        				t[0] = new svm_node();
				        				t[0].index = 0;
				        				t[0].value = b_block_id*b_block_size+p;	//#sample id
				        				//logger.info("b_block_id " +  b_block_id);
				        				//logger.info("b_block_size " +  b_block_size);
				        				for(int q = 1 ; q < t.length ; q ++) {
				        					t[q] = new svm_node();
				    						t[q].index = q;
				    						t[q].value = br.readFloat();
				        				}
				        				result[p] = predictor.predictHadoop(t);
				        				
				        				//logger.info("predicted" + p);
				    					//printMemory();
				        			}
			        			} catch (IOException e) {
			    					e.printStackTrace();
			    					logger.info("Cannot read float" + status[i].getPath() + ". Break!");
			    					blacklist.put(thisname, "bad");
			    					break;
			    				}
			        			br.close();
			        			
			        			//2) write out the prediction
			        			printMemory();
			        			String name = sin_prediction_location + "/prediction-p" + b_block_id;
			        			writePrediction(name,result,row_cnt);
			        			logger.info("write out the prediction out " +  name);
			        			
			        			
			        			//3) delete the temp matrix chunk
			        			boolean successful = fs.delete(status[i].getPath(), false);
			        			logger.info("delete the kernel matrix chunk and successful=" +  successful);
			        			logger.info("#Files in black list " +  blacklist.size());
			        			break;
			        		}
			        	}
			        }
			        
			        
					reporter.setStatus("<br>\n I fall asleep.");
					try {
						Thread.sleep(30000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					reporter.setStatus("<br>\n I am awake.");
					
				
			}
			
			//output.collect(key, new Text("ok"));
		}
		
		
		protected void writePrediction(String name, double[][] predictions, int lineno) throws IOException {
			Path outFile = new Path(name);
			DataOutputStream out = new DataOutputStream(new BufferedOutputStream(fs.create(outFile)));
			for(int i = 0 ; i < predictions.length ; i++) {
				for(int j = 0 ; j < predictions[i].length ; j++) {
					out.writeDouble(predictions[i][j]);
				}
			}
			
			out.flush();
			out.close();
		}
		

	}

	public static void main(String[] args) throws Exception {
		
		
			String out_dir = args[0];									//out dir
			String buffer = args[1];									//buffer dir		
			String prediction_model_location = args[2];					//sin model location
			String row_a = args[3];										//number of row a 236697
			String b_block_size = args[4];								//the block size for b-matrix (b_matrix_chunk) sift or mosift or csift. not 3*the size
			String total_reducer_num = args[5];							//total number of reducers
			String sin_prediction_location = args[6];
			
			Configuration pconf = new Configuration();
			FileSystem fs = FileSystem.get(pconf);
				
			
			//fs.create(new Path(out_dir + File.separator + (total_iter+1)));
			Path inpath = new Path(out_dir + File.separator + 8+"predictionin");
			SequenceFile.Writer writer = SequenceFile.createWriter(fs, pconf, inpath, IntWritable.class,  Text.class);
			String nonsense = "very long non sensevery long non sense.";
			nonsense = nonsense+nonsense+nonsense+nonsense+nonsense+nonsense+nonsense+nonsense+nonsense;
			nonsense = nonsense+nonsense+nonsense+nonsense+nonsense+nonsense+nonsense+nonsense+nonsense;
			nonsense = nonsense+nonsense+nonsense+nonsense+nonsense+nonsense+nonsense+nonsense+nonsense;
			nonsense = nonsense+nonsense+nonsense+nonsense+nonsense+nonsense+nonsense+nonsense+nonsense;
			
			for(int i = 0 ; i < 1000; i++) {
				writer.append(new IntWritable(i), new Text(nonsense+i));
			}
			writer.close();
		
			
			//write the input file
			JobConf conf = new JobConf(PredictionReducer.class);
			conf.set("mapred.child.java.opts", "-Xmx1200m");		//cannot be too large <2000m
				
			conf.set("mapred.cluster.map.memory.mb","2000");
			conf.set("mapred.cluster.reduce.memory.mb","2000");
				
			conf.set("mapred.job.map.memory.mb","2000");
			conf.set("mapred.job.reduce.memory.mb","2000");
			conf.set("mapred.tasktracker.map.tasks.maximum","1");
			conf.set("mapred.map.max.attempts","8");
			conf.set("mapred.reduce.max.attempts","8");	
			conf.setJobName("casacade-svm-prediction-reducer");
				
			// out key and value for mapper
			conf.setMapOutputKeyClass(IntWritable.class);
			conf.setMapOutputValueClass(IntWritable.class);
			// out key and value for reducer
			conf.setOutputKeyClass(IntWritable.class);
			conf.setOutputValueClass(Text.class);

			conf.setMapperClass(MyMapper.class);
			conf.setReducerClass(MyReducer.class);
			
			conf.setMaxReduceAttempts(32);
			
			conf.setNumMapTasks(1);
			conf.setNumReduceTasks(Integer.parseInt(total_reducer_num));

			conf.setInputFormat(SequenceFileInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);
			
			
			
			FileInputFormat.setInputPaths(conf, inpath);
			//fs.create(new Path(out_dir + File.separator + (total_iter+2)));
			FileOutputFormat.setOutputPath(conf, new Path(out_dir + File.separator+ 8+"out"));
			
			conf.set("b_block_size", b_block_size);			
			conf.set("model_location", prediction_model_location);
			conf.set("row_a", row_a);
			conf.set("total_reducer_num", total_reducer_num);
			conf.set("kernelmatrix_buffer", buffer);
			conf.set("sin_prediction_location", sin_prediction_location);
			
			JobClient.runJob(conf);
	}

}
