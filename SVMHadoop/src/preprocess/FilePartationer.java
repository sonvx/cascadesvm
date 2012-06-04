package preprocess;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;

import beans.FloatMatrix;
import beans.KernelRowArrayWritable;
import beans.KernelRowWritable;



public class FilePartationer {
	
	
	public static void partation(File in, File outdir, int partno, String prefix) throws IOException {
		int filecounter = 0;
		int linecounter = 0;
		
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(in)),1024*8*16);
		String line = br.readLine();
		DataOutputStream bw = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(new File(outdir, prefix+"-inpart" + filecounter)),1024*8*16));
		while(line != null) {
			linecounter++;
			if(linecounter%partno == 0) {
				bw.flush();
				bw.close();
				//rename
				File lastfile = new File(outdir, prefix+"-inpart" + filecounter);
				lastfile.renameTo(new File(outdir, File.separator + prefix+"-inpart" + filecounter + "-" +(linecounter-1)));
				
				linecounter = 1;
				filecounter++;
				bw = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(new File(outdir, prefix+"-inpart" + filecounter)),1024*8*16));
				
			}
			String[] temps = line.split("[ :]");
			bw.writeInt(Integer.parseInt(temps[0]));		//line no
			bw.writeInt((temps.length-2)/2);
			//skip the second position
			for(int i = 2 ; i < temps.length ; i=i+2) {
				short index = (short)(Integer.parseInt(temps[i])-1);
				bw.writeShort(index);
				bw.writeFloat(Float.parseFloat(temps[i+1]));
				
			}
			line = br.readLine();
		}
		
		bw.flush();
		bw.close();
		br.close();
		
		File lastfile = new File(outdir, prefix+"-inpart" + filecounter);
		lastfile.renameTo(new File(outdir, File.separator + prefix+"-inpart" + filecounter + "-" +linecounter));
	}
	
	public static void translate(File in) throws IOException {
		DataInputStream br = new DataInputStream(new FileInputStream(in));
		
		for(int i = 0 ; i < 52 ; i ++) {
			int lineno = br.readInt();
			int arraysize = br.readInt();
			StringBuffer print = new StringBuffer();
			print.append(lineno+" ");
			for(int j = 0 ; j < arraysize ; j ++) {
				print.append((br.readShort()+1)+":");
				print.append(br.readFloat()+" ");
			}
			System.out.println(print);
			print.delete(0, print.length());
		}
		
	}
	
	public static void translatePrediction(File in) throws IOException {
		DataInputStream br = new DataInputStream(new FileInputStream(in));
		while(br.available() > 0) {
			for(int i = 0 ; i < 346+1 ; i ++) {
				double prediction = br.readDouble();
				System.out.print(prediction + " ");
			}
			System.out.print("\r\n");
		}
		
	}
	
	
	public static void translateKernelChunk(File in, int dim) throws IOException {
		DataInputStream br = new DataInputStream(new FileInputStream(in));
		while(br.available() > 0) {
			for(int i = 0 ; i < dim ; i ++) {
				float prediction = br.readFloat();
				System.out.print(prediction + " ");
			}
			System.out.print("\r\n");
		}
	}
	
	
	
	
	
	
	
	
	
	public static void readSequenceFile(File in) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		SequenceFile.Reader br = new SequenceFile.Reader(fs, new Path(in.getAbsolutePath()), conf);
		
		IntWritable key = new IntWritable();
		KernelRowArrayWritable value = new KernelRowArrayWritable();
		
		while(br.next(key,value)) {
			//System.out.println(re);
			Writable[] vs =value.get();
			KernelRowWritable t1 = (KernelRowWritable)vs[0];
			System.out.print(t1.toString(2));
			System.out.print(key.toString() + "-" + value.get().length+"\r\n");
		}
		
	}
	
	
	public static void readFoutFile(File in) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		SequenceFile.Reader br = new SequenceFile.Reader(fs, new Path(in.getAbsolutePath()), conf);
		
		IntWritable key = new IntWritable();
		FloatMatrix value = new FloatMatrix();
		
		while(br.next(key,value)) {
			//System.out.println(re);
			Writable[][] vs =value.get();
			System.out.print(vs.length);
			System.out.print(key.toString() + "-" + value.get().length+"\r\n");
		}
		
	}
	
	
	public static void writeMergeSeq(File out) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		int total_sample_matrix_b = 10000;
		int block_size_matrix_b = 300;
		
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, new Path(out.getAbsolutePath()), IntWritable.class,  IntWritable.class);
		
		for(int i = 0 ; i < (int) Math.ceil(total_sample_matrix_b/block_size_matrix_b) ; i++) {
			writer.append(new IntWritable(i), new IntWritable(0));
		}
		
		writer.close();
	}
	
	public static void readMergeSeq(File in) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		SequenceFile.Reader br = new SequenceFile.Reader(fs, new Path(in.getAbsolutePath()), conf);
		
		IntWritable key = new IntWritable();
		IntWritable value = new IntWritable();
		
		while(br.next(key,value)) {
			//System.out.println(re);
			System.out.print(key.toString() + "-" + value.toString()+"\r\n");
		}
		
	}
	
	
	public static void main(String args[]) throws Exception {
		
			readSequenceFile(new File("G:\\ground-truth-check\\featuers\\evl\\fusion-seq\\fusion.seq-3"));
			//translateKernelChunk(new File("G:\\kernelmatrix-c0-r3"),236697);
			//readSequenceFile(new File("G:\\ground-truth-check\\featuers\\seq\\csift-bmatrix.seq"));
			//translatePrediction(new File("G:\\ground-truth-check\\test-prediction\\SIN-PREDICTION\\prediction-p9"));
		
		
			//long t1 = System.currentTimeMillis();
			//partation(new File(args[0]),new File(args[1]),Integer.parseInt(args[2]), args[3]);
			//System.out.println("usaga: svm-input-file out-sequence-filename blocksize");
			//writeSequenceFileFromText(new File(args[0]), new File(args[1]), Integer.parseInt(args[2]));
			//writeMergeSeq(new File("G:\\merge-in.seq"));
			//readMergeSeq(new File("G:\\merge-in.seq"));
			//readFoutFile(new File("G:\\prediction\\fout0"));
			//translate(new File("G:\\partationed\\test-inpart2-51"));
			//for(int i = 0 ; i <= 25 ; i++)
			//	translatePrediction(new File("G:\\bigtest\\SIN-PREDICTION\\prediction-p"+i));
			/*for(int i = 0 ; i <= 24 ; i++)
				translateKernelChunk(new File("G:\\smalltest\\value\\K\\kernelmatrix-c"+i+"-r10"),251);
			translateKernelChunk(new File("G:\\smalltest\\value\\K\\kernelmatrix-c"+25+"-r1"),251);*/
			//long t2 = System.currentTimeMillis();
			//System.out.println("run" + "-" +  (t2-t1)/1000 + "s");

	
	}
}
