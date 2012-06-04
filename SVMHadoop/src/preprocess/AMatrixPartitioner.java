package preprocess;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;

public class AMatrixPartitioner {
	
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
	
	public static void main(String[] args) throws Exception {
		
		partation(new File(args[0]), new File(args[1]), Integer.parseInt(args[2]), args[3]);

	}

}
