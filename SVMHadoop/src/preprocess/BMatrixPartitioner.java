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
	
	/**
	 * get the text B matrix for PSC.
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
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(outdir,filenameprefix+"-inpart" + filecounter))));
		String idline = idbr.readLine();
		
		while(idline != null) {
			linecounter++;
			
			
			if(linecounter%chunksize == 0) {
				bw.flush();
				bw.close();
				//rename
				File lastfile = new File(outdir, filenameprefix+"-inpart" + filecounter);
				lastfile.renameTo(new File(outdir, File.separator + filenameprefix+"-bmatrix" + filecounter + "-" +(linecounter-1)));
				
				linecounter = 1;
				filecounter++;
				bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(outdir,filenameprefix+"-inpart" + filecounter))));
			}
			
			
		
			String[] temps = idline.split(" ");
			int tid = Integer.parseInt(temps[1]);
			File tfn = new File(indir, temps[0]+".spbof");
			BufferedReader tbr = new BufferedReader(new InputStreamReader(new FileInputStream(tfn)));
			bw.write(""+tid);
			bw.write(" ");
			if(tfn.exists()) {
				String tline = tbr.readLine();
				if(tline != null)	bw.write(tline);
			} else {
				System.out.println(tfn.getName() + " is missing!");
			}
			bw.write("\n");
			tbr.close();
			idline = idbr.readLine();
			
		}
		
		bw.flush();
		bw.close();
		
		
		File lastfile = new File(outdir, filenameprefix+"-inpart" + filecounter);
		lastfile.renameTo(new File(outdir, File.separator + filenameprefix+"-bmatrix" + filecounter + "-" +linecounter));
	}
	
	
	

	public void partition(File in, File outdir, int chunksize, String filenameprefix) throws IOException {
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
			bw.write("\n");	
			line = br.readLine();
		}
		
		bw.flush();
		bw.close();
		br.close();
		
		File lastfile = new File(outdir, filenameprefix+"-inpart" + filecounter);
		lastfile.renameTo(new File(outdir, File.separator + filenameprefix+"-bmatrix" + filecounter + "-" +linecounter));
	}
	 
	public static void main(String[] args) throws Exception {
		if(args == null ||  args.length != 5) {
			String help = "chunk A into files\n"
					+ "  partition(File indir, File idlist, File outdir, int chunksize, String filenameprefix)\n";
			System.out.print(help);
			System.exit(1);
		}
		BMatrixPartitioner par = new BMatrixPartitioner();
		par.partition(new File(args[0]), new File(args[1]), new File(args[2]), Integer.parseInt(args[3]), args[4]);
		//par.partition(new File(args[0]), new File(args[1]), Integer.parseInt(args[2]), args[3]);

	}

}
