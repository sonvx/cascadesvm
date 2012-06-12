package preprocess;

import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;

public class KernelChunkTranslator {

	
	/**
	 * 
	 * translate the binary kernel file into a plain text file.(All float)
	 * @param in the input 
	 * @param outdir the output file i.e. the text file 
	 * @param dim the dimension of kernel (233667)
	 * @throws IOException
	 */
	public static void translate(File in, File outfile, int dim) throws IOException {
		BufferedWriter bw = new BufferedWriter(new FileWriter(outfile),1024*8*16);
		DataInputStream br = new DataInputStream(new FileInputStream(in));
		while(br.available() > 0) {
			for(int i = 0 ; i < dim -1; i ++) {
				float kernel = br.readFloat();
				bw.write(kernel + " ");
			}
			
			bw.write(br.readFloat() + "\r\n");
		}
		bw.flush();
		bw.close();
	}
	
	
	public static void main(String[] args) throws Exception {
		
		translate(new File(args[0]), new File(args[1]), Integer.parseInt(args[2]));

	}

}
