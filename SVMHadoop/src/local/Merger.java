package local;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import beans.FloatMatrix;


public class Merger {
	
	
	public void merge21File(File dir, int blocknum) throws IOException {
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(dir,"merged.txt"))));
			for(int j = 0 ; j < blocknum ; j++) {
				File thisfile = new File(dir, "fout"+j);
				
				
				BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(thisfile)),16*1024*1024);
				String line = br.readLine();
				//kernel matrix must be the dense matrix so we do not have to reset the matrix every time
				while(line != null) {
				
					bw.write(line);
					bw.write("\r\n");
					line = br.readLine();
				}
				br.close();
			}
		bw.flush();
		bw.close();
	}
	
	public void mergeSeq21File(File dir, int blocknum) throws IOException {
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(dir,"merged.txt"))));
		for(int j = 0 ; j < blocknum ; j++) {
			File thisfile = new File(dir, "fout"+j);
				bw.write(readSequenceFile(thisfile));
		}

		

		bw.flush();
		bw.close();
	}
	
	
	private void addAppendToBig(FloatWritable[][] big, Writable[][] small, int startIndex) {
		for(int i = 0 ; i < big.length ; i++) {
			for(int j = 0 ; j < small[i].length ; j++) {
				big[i][startIndex+j] = (FloatWritable) small[i][j];
			}
		}
	}
	
	private void addAppendToBig(float[][] big, Writable[][] small, int startIndex) {
		for(int i = 0 ; i < big.length ; i++) {
			for(int j = 0 ; j < small[i].length ; j++) {
				big[i][startIndex+j] = ((FloatWritable) small[i][j]).get();
			}
		}
	}
	
	

	
	/**
	 * called by a local thread
	 * @param dir
	 * @param iter_num
	 * @param block_size
	 * @param total_sample_number_a
	 * @param total_sample_numer_b
	 * @throws IOException
	 */
	public void mergeSequence(String dir, int iter_num, int block_size, int total_sample_number_a, int total_sample_numer_b) throws IOException {
		double blockSize = block_size;
		int looptime = (int) Math.ceil(total_sample_numer_b/blockSize);
		for(int i = 0 ; i < looptime ; i++) {
			Path[] pathes = new Path[iter_num];
			for(int j = 0 ; j < pathes.length ; j++) {
				pathes[j] = new Path(dir + File.separator + "out" + "(" + j + "-" + i +")");
			}
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			mergeHadoopSequence(pathes, new Path(dir +  File.separator + "fout" + i), total_sample_number_a, fs,new Configuration(),null);
		}
	}
	
	public String printMemory() {
		return(":max-memory:"+(Runtime.getRuntime().maxMemory()/1024/1024)+
				"m:free-memory:"+(Runtime.getRuntime().freeMemory()/1024/1024)+
				"m:total"+(Runtime.getRuntime().totalMemory()/1024/1024)+"\r\n");
	}
	
	
	
	public void mergeHadoopSequence(Path[] inpathes, Path out, int columnDim, FileSystem fs, Configuration conf, Logger logger ) throws IOException {
		
		//Configuration conf = new Configuration();
		SequenceFile.Reader [] brs = new SequenceFile.Reader[inpathes.length];
		
		//open all files in hdfs
		for(int i = 0 ; i < inpathes.length ; i++) {
			if(logger != null) logger.info(inpathes[i]);
			brs[i] = new SequenceFile.Reader(fs, inpathes[i], conf);
		}
		
		//read the first file to get the matrix size
		//all the matrixes have the same row and fixed column
		
		FloatMatrix value = new FloatMatrix();
		IntWritable key = new IntWritable();
		brs[0].next(key,value);
		Writable[][] matrix = value.get();
		int n,m;
		n = matrix.length;			//row
		m = matrix[0].length;		//column
			
			
		//create the big matrix buffer
		FloatWritable[][] finalvalues = new FloatWritable[n][columnDim];
		addAppendToBig(finalvalues, matrix, 0*m);			//starting index m*loop_index id
		matrix = null;			//release the matrix
		brs[0].close();			//close the reader
		
		//in memory process
		for(int i = 1 ; i < inpathes.length ; i++) {
			value = new FloatMatrix();
			key = new IntWritable();
			brs[i].next(key,value);
			matrix = value.get();
			addAppendToBig(finalvalues, matrix, i*m);	//starting index m*loop_index id
			matrix = null;			//release the matrix
			brs[i].close();			//close the reader
			if(logger!=null) logger.info("appended" + i);
			if(logger!=null) logger.info(printMemory());
		}
		
		
		//write out the sequence file
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, out,IntWritable.class, FloatMatrix.class);
		FloatMatrix bigmatrix = new FloatMatrix();
		bigmatrix.set(finalvalues);
		if(logger!=null) logger.info("bigmatrix[" + finalvalues.length + "," + finalvalues[0].length +"]");
		if(logger!=null) logger.info("writer = " + writer.toString());
		writer.append(key, bigmatrix);
		writer.close();
		
	}
	
	
	public void mergeHadoopBinary(Path[] inpathes, Path out, int columnDim, FileSystem fs, Logger logger ) throws IOException {
				Configuration conf = new Configuration();
				SequenceFile.Reader [] brs = new SequenceFile.Reader[inpathes.length];
				
				//open all files in hdfs
				for(int i = 0 ; i < inpathes.length ; i++) {
					if(logger != null) logger.info(inpathes[i]);
					brs[i] = new SequenceFile.Reader(fs, inpathes[i], conf);
				}
				
				//read the first file to get the matrix size
				//all the matrixes have the same row and fixed column
				
				FloatMatrix value = new FloatMatrix();
				IntWritable key = new IntWritable();
				brs[0].next(key,value);
				Writable[][] matrix = value.get();
				int n,m;
				n = matrix.length;			//row
				m = matrix[0].length;		//column
					
					
				//create the big matrix buffer
				float[][] finalvalues = new float[n][columnDim];
				addAppendToBig(finalvalues, matrix, 0*m);			//starting index m*loop_index id
				matrix = null;			//release the matrix
				brs[0].close();			//close the reader
				
				//in memory process
				for(int i = 1 ; i < inpathes.length ; i++) {
					value = new FloatMatrix();
					key = new IntWritable();
					brs[i].next(key,value);
					matrix = value.get();
					addAppendToBig(finalvalues, matrix, i*m);	//starting index m*loop_index id
					matrix = null;			//release the matrix
					brs[i].close();			//close the reader
					if(logger!=null) logger.info("appended" + i);
					if(logger!=null) logger.info(printMemory());
				}
				
				
				//write out the sequence file
				DataOutputStream  bw = new DataOutputStream(new BufferedOutputStream(fs.create(out),1024*8*16));
				if(logger!=null) logger.info("bigmatrix[" + finalvalues.length + "," + finalvalues[0].length +"]");
				if(logger!=null) logger.info("writer = " + bw.toString());
				for(int i = 0 ; i < finalvalues.length ; i++) {
					for(int j = 0 ; j < finalvalues[i].length ; j++) {
						bw.writeFloat(finalvalues[i][j]);
					}
					
				}
				bw.flush();
				bw.close();
		
	}	
		
	public String readSequenceFile(File in)  throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		SequenceFile.Reader br = new SequenceFile.Reader(fs, new Path(in.getAbsolutePath()), conf);
		IntWritable key = new IntWritable();
		FloatMatrix value = new FloatMatrix();
		StringBuffer sb = new StringBuffer();
		while(br.next(key,value)) {
			sb.append(printMatrix(value.get()));
		}
		return sb.toString();
	}
	
	
	
	public static void readSequenceFile(File in, File out) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		SequenceFile.Reader br = new SequenceFile.Reader(fs, new Path(in.getAbsolutePath()), conf);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(out)));
		IntWritable key = new IntWritable();
		FloatMatrix value = new FloatMatrix();
		
		while(br.next(key,value)) {
			bw.write(printMatrix(value.get()));
		}
		
		bw.flush();
		bw.close();
		
	}
	
	
	private static String printMatrix(Writable[][] matrix) {
		StringBuffer sb = new StringBuffer();
		for(int i = 0 ; i < matrix.length ; i++) {
			for(int j = 0 ; j < matrix[i].length ; j++) {
				sb.append(((FloatWritable)matrix[i][j]).get() + " ");
			}
			sb.append("\r\n");
		}
		return sb.toString();
	}
	
	
	
	
	public static void main(String args[]) throws Exception {
		Merger m = new Merger();
		m.mergeSequence("G:\\hadoopout\\sequence",3,20,252,252);
		//m.mergeSeq21File(new File("G:\\hadoopout\\1500(block=500)"), 3);
	
		//m.mergeHadoopSequence(paths, new Path(dir+"big"),252);
		//readSequenceFile(new File(dir, "big"), new File(dir, "big0"));
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}
