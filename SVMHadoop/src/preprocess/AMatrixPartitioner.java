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
	
	
	/**
	 * get the text A matrix for PDL.
	 * @param indir
	 * @param idlist
	 * @param outdir
	 * @param chunksize
	 * @param filenameprefix
	 * @throws IOException
	 */
	public void partition(File indir, File idlist, File outdir, int chunksize, String filenameprefix) throws IOException {
		int linecounter = 0;
		int filecounter = 0;
		
		BufferedReader idbr = new BufferedReader(new InputStreamReader(new FileInputStream(idlist)));
		DataOutputStream bw = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(new File(outdir, filenameprefix+"-inpart" + filecounter))));
		String idline = idbr.readLine();
		
		while(idline != null) {
			linecounter++;
			
			
			if(linecounter%chunksize == 0) {
				bw.flush();
				bw.close();
				//rename
				File lastfile = new File(outdir, filenameprefix+"-inpart" + filecounter);
				lastfile.renameTo(new File(outdir, File.separator + filenameprefix+"-inpart" + filecounter + "-" +(linecounter-1)));
				
				linecounter = 1;
				filecounter++;
				bw = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(new File(outdir, filenameprefix+"-inpart" + filecounter))));
			}
			
			
			String line;
			{
				String[] temps = idline.split(" ");
				
				File tfn = new File(indir, temps[0]+".spbof");
				
				BufferedReader tbr = new BufferedReader(new InputStreamReader(new FileInputStream(tfn)));
				line = tbr.readLine();
				if(line == null) line = "";
				line = line.trim();
				line = temps[1] + "  " + line;
				tbr.close();
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
			
			idline = idbr.readLine();
			
		}
		
		bw.flush();
		bw.close();
		
		
		File lastfile = new File(outdir, filenameprefix+"-inpart" + filecounter);
		lastfile.renameTo(new File(outdir, File.separator + filenameprefix+"-inpart" + filecounter + "-" +linecounter));
	}
	
	public void partation(File in, File outdir, int partno, String prefix) throws IOException {
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
		if(args == null ||  args.length != 5) {
			String help = "chunk A into files\n"
					+ "  partition(File indir, File idlist, File outdir, int chunksize, String filenameprefix)\n";
			System.out.print(help);
			System.exit(1);
		}
		AMatrixPartitioner par = new AMatrixPartitioner();
		par.partition(new File(args[0]), new File(args[1]), new File(args[2]), Integer.parseInt(args[3]), args[4]);
		//par.partation(new File(args[0]), new File(args[1]), Integer.parseInt(args[2]), args[3]);

	}

}
