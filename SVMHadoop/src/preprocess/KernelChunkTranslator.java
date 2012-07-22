package preprocess;

import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class KernelChunkTranslator {

	
	/**
	 * 
	 * translate the binary kernel file into a plain text file.(All float)
	 * @param in the input 
	 * @param outdir the output file i.e. the text file 
	 * @param dim the dimension of kernel (233667)
	 * @throws IOException
	 */
	public void translate(File in, File outfile, int dim) throws IOException {
		BufferedWriter bw = new BufferedWriter(new FileWriter(outfile),1024*8*16);
		DataInputStream br = new DataInputStream(new FileInputStream(in));
		while(br.available() > 0) {
			for(int i = 0 ; i < dim -1; i ++) {
				float kernel = br.readFloat();
				bw.write(kernel + " ");
			}
			
			bw.write(br.readFloat() + "\n");
		}
		bw.flush();
		bw.close();
	}
	
	
	
	/**
	 * 
	 * translate the binary kernel file into a plain text file.(All float)
	 * @param in the input 
	 * @param outdir the output file i.e. the text file 
	 * @param dim the dimension of kernel (233667)
	 * @throws IOException
	 */
	public void translateMinus(File in, File outfile, int dim) throws IOException {
		BufferedWriter bw = new BufferedWriter(new FileWriter(outfile),1024*8*16);
		DataInputStream br = new DataInputStream(new FileInputStream(in));
		while(br.available() > 0) {
			for(int i = 0 ; i < dim -1; i ++) {
				float kernel = br.readFloat() * -1.0f;
				bw.write(kernel + " ");
			}
			
			bw.write((-1.0f*br.readFloat()) + "\n");
		}
		bw.flush();
		bw.close();
	}
	
	
	public ArrayList<float[]> translateMinus(File in, int dim) throws IOException {
		ArrayList<float[]> result = new ArrayList<float[]>(32);
		DataInputStream br = new DataInputStream(new FileInputStream(in));
		while(br.available() > 0) {
			float[] thisfloat = new float[dim];
			for(int i = 0 ; i < dim -1; i ++) {
				thisfloat[i] = br.readFloat() * -1.0f;
			}
			thisfloat[dim-1] = (-1.0f*br.readFloat());
			result.add(thisfloat);
		}
		return result;
	}
	
	
	
	
	public void traslateMinusDir(File indir, File outfile, int kernel_dim) throws IOException {
		Pattern idpatern = Pattern.compile("kernelmatrix-c(\\d+)-r\\d+");
		HashMap<Integer, File> file_map = new HashMap<Integer, File>(109);
		//check whether all kernel files are ready
		File[] subfiles = indir.listFiles();
		int maxid = Integer.MIN_VALUE;
		int minid = Integer.MAX_VALUE;
		for(int i = 0 ; i < subfiles.length ; i++) {
			Integer thisid;
			Matcher m = idpatern.matcher(subfiles[i].getName());
			if(m.find()) {
				thisid = Integer.parseInt(m.group(1));
				file_map.put(thisid, subfiles[i]);
				if(thisid > maxid) maxid = thisid;
				if(thisid < minid) minid = thisid;
			}
		}
		
		
		
		int kernellinecnt = 1;		//svm kernel file line number starting from 1
		
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outfile)));
		for(int i = minid ; i <= maxid ; i++) {
			File thisfile = file_map.get(i);
			if(thisfile == null || !thisfile.exists()) {
				System.err.println("the id of the prediction file is not consecutive and " + "kernelmatrix-c" + i + " is missing.");
				System.exit(1);
			} else {
				ArrayList<float[]> thiskernel = translateMinus(thisfile, kernel_dim);
				for(int j = 0 ; j < thiskernel.size() ; j++) {
					bw.write("0:"+kernellinecnt);
					for(int k = 0  ; k < thiskernel.get(j).length ; k++) {
						bw.write(" " + (k+1) + ":" + thiskernel.get(j)[k]);
					}
					bw.write("\n");
					kernellinecnt++;
				}
				
			}
		}
		
		bw.flush();
		bw.close();
		
	}
	
	
	public static void main(String[] args) throws Exception {
		if(args == null ||  args.length != 3) {
			String help = "convert binary kernel files to a text file in libsvm format\n"
					+ "  traslateMinusDir(File indir, File outfile, int kernel_dim)\n";
			System.out.print(help);
			System.exit(1);
		}
		
		KernelChunkTranslator k = new KernelChunkTranslator();
		
		k.traslateMinusDir(new File(args[0]), new File(args[1]), Integer.parseInt(args[2]));

	}

}
