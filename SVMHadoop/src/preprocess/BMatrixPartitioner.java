package preprocess;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

public class BMatrixPartitioner {

	public static void partition(File in, File outdir, int chunksize, String filenameprefix) throws IOException {
		int linecounter = 0;
		int filecounter = 0;
		
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(in)),1024*8*1024);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(outdir,filenameprefix+"-inpart" + filecounter))),1024*8*16);
		String line = br.readLine();
		
		while(line != null) {
			linecounter++;
			if(linecounter%chunksize == 0) {
				bw.flush();
				bw.close();
				//rename
				File lastfile = new File(outdir, filenameprefix+"-inpart" + filecounter);
				lastfile.renameTo(new File(outdir, File.separator + filenameprefix+"-bmatrix" + filecounter + "-" +(linecounter-1)));
				
				linecounter = 1;
				filecounter++;
				bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(outdir,filenameprefix+"-inpart" + filecounter))),1024*8*16);
			}
			
			bw.write(line);
			bw.write("\r\n");	
			line = br.readLine();
		}
		
		bw.flush();
		bw.close();
		br.close();
		
		File lastfile = new File(outdir, filenameprefix+"-inpart" + filecounter);
		lastfile.renameTo(new File(outdir, File.separator + filenameprefix+"-bmatrix" + filecounter + "-" +linecounter));
	}
	 
	public static void main(String[] args) throws Exception {
		
		partition(new File(args[0]), new File(args[1]), Integer.parseInt(args[2]), args[3]);

	}

}
