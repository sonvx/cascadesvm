package preprocess;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
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

public class SequenceWriter {
	public static final int BINARY_FILE_LINENUMBERED = 0, TEXT_SVM_FILE_LINENUMBERED = 1, TEXT_SVM_FILE_NOTNUMBERED = 2;
	
	/**
	 * @param in the input file
	 * @param out the output sequence file
	 * @param blocksize the block size for sequence file
	 * @param mode the working mode
	 * @throws IOException 
	 */
	public static void writeSequence(File in, File out, int blocksize, int mode) throws IOException {
		if(mode == BINARY_FILE_LINENUMBERED) {
			writeSequenceFromBinaryFile(in,out,blocksize);
		} else if(mode == TEXT_SVM_FILE_LINENUMBERED) {
			writeSequenceFileFromText(in,out,blocksize);
		} else if(mode == TEXT_SVM_FILE_NOTNUMBERED) {
		}
	}
	
	
	/**
	 * input file is a binary file whose format is
	 * lineno(int)array_size(int)index1(short)value1(float)...
	 * @param in a binary file
	 * @param out the output sequence file
	 * @param blocksize the block size for sequence file
	 * @throws IOException 
	 */
	public static void writeSequenceFromBinaryFile(File in, File out, int blocksize) throws IOException{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, new Path(out.getAbsolutePath()),IntWritable.class,  KernelRowArrayWritable.class);
		DataInputStream br = new DataInputStream(new BufferedInputStream(new FileInputStream(in),1024*1024*16));
		int indexcnt = 0;
		int blockid = 0;
		KernelRowWritable[] krs = new KernelRowWritable[blocksize];			
		ShortWritable[] sw;
		FloatWritable[] fw;
		KernelRowWritable kr;
		
		
		while(true) {
			if(br.available()== 0 ) break;
			Integer lineno = br.readInt();
			

			if(indexcnt==blocksize) {
				KernelRowArrayWritable block = new KernelRowArrayWritable();
				block.set(krs);
				writer.append(new IntWritable(blockid), block);
				indexcnt = 0;
				blockid++;
				krs = new KernelRowWritable[blocksize];			
			}
			
			
			kr = new KernelRowWritable();
			kr.setLineID(lineno);		//line no
			int arraylength = br.readInt();
			sw = new ShortWritable[arraylength];
			fw = new FloatWritable[arraylength];
			for(int j = 0 ; j < arraylength ; j ++) {
				
				sw[j] = new ShortWritable((short)(br.readShort()-1));
				fw[j] = new FloatWritable(br.readFloat());
			}
			
			kr.setFloat(fw);
			kr.setShort(sw);
			krs[indexcnt] = kr;
			indexcnt++;

		}
		
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
		
		KernelRowWritable[] reminder_krs = new KernelRowWritable[size];		
		for(int i = 0 ; i < size ; i++) {
			reminder_krs[i] = krs[i];
		}
		
		block.set(reminder_krs);
		writer.append(new IntWritable(blockid), block);
		br.close();
		writer.close();
	}
	
	
	
	/**
	 * input file is the svm format file whose format is
	 * lineno index1:value1 index2:value2 ... \r\n
	 * @param in the text file in svm format
	 * @param out the output sequence file 
	 * @param blocksize the block size for sequence file
	 * @throws IOException 
	 */
	public static void writeSequenceFileFromText(File in, File out, int blocksize) throws IOException{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, new Path(out.getAbsolutePath()),IntWritable.class,  KernelRowArrayWritable.class);
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(in)),1024*8*16);
		String line = br.readLine();
		int indexcnt = 0;
		int blockid = 0;
		KernelRowWritable[] krs = new KernelRowWritable[blocksize];			
		ShortWritable[] sw;
		FloatWritable[] fw;
		KernelRowWritable kr;
		do {
			
			kr = new KernelRowWritable();
			if(indexcnt==blocksize) {
				KernelRowArrayWritable block = new KernelRowArrayWritable();
				block.set(krs);
				writer.append(new IntWritable(blockid), block);
				indexcnt = 0;
				blockid++;
				krs = new KernelRowWritable[blocksize];			
			}
			
			
			
			String[] temps = line.split("[ :]");
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
			
		
			line = br.readLine();
			indexcnt++;
			
		}  while(line != null);
		
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
		
		KernelRowWritable[] reminder_krs = new KernelRowWritable[size];		
		for(int i = 0 ; i < size ; i++) {
			reminder_krs[i] = krs[i];
		}
		
		block.set(reminder_krs);
		writer.append(new IntWritable(blockid), block);
		
		
		br.close();
		writer.close();
	}
	
	public static void main(String[] args) throws Exception {
		if(args == null || args.length != 4) {
			String help = "write B feature file into a sequence file\n"
					+ "  class:preprocess.SequenceWriter\n"
					+ "  writeSequence(File in, File out, int blocksize, int mode)\n"
					+ "  in the input file\n"
					+ "  out the output sequence file\n"
					+ "  blocksize the block size for sequence file\n"
					+ "  mode specifies the input file type:\n"
					+ "     mode == 0 binary file with line number whose format is\n"
					+ "     lineno(int)array_size(int)index1(short)value1(float)...\n" 
					+ "     mode == 1 text file with svm format whose format is\n"
					+ "     lineno index1:value1 index2:value2 ... \\n \n"
					+ "     ";
			System.out.print(help);
			System.exit(1);
		}
		writeSequence(new File(args[0]), new File(args[1]), Integer.parseInt(args[2]), Integer.parseInt(args[3]));
	}

}
