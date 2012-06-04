package preprocess;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;

import beans.KernelRowArrayWritable;
import beans.KernelRowWritable;
import beans.ShortWritable;

public class SequenceWriterEarlyFusion {
	
	

	
	/**
	 * @param more than one files for early fusion from text files in svm format
	 * @param out the output sequence file
	 * @param blocksize the block size for sequence file
	 * @throws IOException 
	 */
	public static void writeSequence(File[] in, File out, int blocksize) throws IOException {
		writeSequenceFileFromTexts(in,out,blocksize);
	}

	
	/**
	 * Write the sequence file for the early fusion
	 * input files are the svm format files whose format is
	 * lineno index1:value1 index2:value2 ... \r\n
	 * @param in the text file in svm format
	 * @param out the output sequence file 
	 * @param blocksize the block size for sequence file
	 * @throws IOException 
	 */
	public static void writeSequenceFileFromTexts(File[] ins, File out, int blocksize) throws IOException{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, new Path(out.getAbsolutePath()),IntWritable.class,  KernelRowArrayWritable.class);
		BufferedReader[] brs = new BufferedReader[ins.length];
		
		for(int i = 0 ; i < brs.length ; i++) {
			brs[i] = new BufferedReader(new InputStreamReader(new FileInputStream(ins[i])),1024*8*1024);
		}
		
		String[] lines = new String[brs.length];
		
		//read line
		for(int i = 0 ; i < brs.length ; i++) {
			lines[i] = brs[i].readLine();
		}
				
		
		int blockid = 0;
		KernelRowWritable[] krs = new KernelRowWritable[blocksize*ins.length];			
		ShortWritable[] sw;
		FloatWritable[] fw;
		
		
		do {
			boolean reachEnd = false;
			int indexcnt = 0;
			for(int p = 0 ; p < ins.length ; p++) {					//index for the input file
				for(int q = 0 ; q < blocksize ; q++)	{			//read the number of line equaling to the block size
					if(lines[p] == null) {
						reachEnd = true;
						break;
					}
					KernelRowWritable kr = new KernelRowWritable();
					String[] temps = lines[p].split("[ :]");
					kr.setLineID(Integer.parseInt(temps[0]));		//line no
					int arraylength = (temps.length-2)/2;
					if(arraylength < 0) arraylength = 0;
					
					sw = new ShortWritable[arraylength];
					fw = new FloatWritable[arraylength];
					
					//skip the second position
					for(int i = 0 ; i < sw.length ; i++) {
						short index = (short)(Integer.parseInt(temps[2*i+2])-1);
						sw[i] = new ShortWritable(index);
						fw[i] = new FloatWritable(Float.parseFloat(temps[2*i+3]));
					}
					
					kr.setFloat(fw);
					kr.setShort(sw);
					krs[indexcnt] = kr;
					indexcnt++;
					lines[p] = brs[p].readLine();
				}
			}
			
			if(!reachEnd) {
				KernelRowArrayWritable block = new KernelRowArrayWritable();
				block.set(krs);
				writer.append(new IntWritable(blockid), block);
				blockid++;
				krs = new KernelRowWritable[blocksize*ins.length];
			}
			
		}  while(lines[0] != null);
		
		//flush the remaining kr in krs
		KernelRowArrayWritable block = new KernelRowArrayWritable();
		int size = 0;
		for(int i = 0 ; i < krs.length ; i++) {
			if(krs[i] != null) {
				size++;
			} else {
				break;
			}
		}
		
		if(size != 0) {
			KernelRowWritable[] reminder_krs = new KernelRowWritable[size];
			int indexcnt = 0;
			for(int i = 0 ; i < size ; i++) {
				reminder_krs[indexcnt] = krs[i];
				indexcnt++;
			}
			
			block.set(reminder_krs);
			writer.append(new IntWritable(blockid), block);
		}
		
		
		for(int i = 0 ; i < brs.length ; i++) {
			brs[i].close();
		}
		writer.close();
	}
	
	
	
	
	
	public static void main(String[] args) throws Exception {
		if(args == null || args.length != 3) {
			String help = "write B feature file into a sequence file\n"
					+ "  input files are text files with svm format whose format are"
					+ "  lineno index1:value1 index2:value2 ... \\n \n"
					+ "  class:preprocess.SequenceWriter\n"
					+ "  writeSequence(File[] in, File out, int blocksize) --> early fusion\n"
					+ "  a String of the directories files(seperated by #)\n"
					+ "  out the output sequence file\n"
					+ "  blocksize the block size for sequence file\n";
			System.out.print(help);
			System.exit(1);
		}
		
		String[] infilepathes = args[0].split("#");
		File[] infiledirs = new File[infilepathes.length];
		
		for(int i = 0 ; i < infiledirs.length ; i++) {
			infiledirs[i] = new File(infilepathes[i]);
		}
		
		writeSequence(infiledirs, new File(args[1]), Integer.parseInt(args[2]));
	}

}
