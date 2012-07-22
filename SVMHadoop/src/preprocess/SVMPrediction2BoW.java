package preprocess;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

public class SVMPrediction2BoW {

	
	
	
	public void gen(File prediction, File idlist, File outdir, int dim) {
		double[] spbow = new double[dim];
		try {
			BufferedReader brid =  new BufferedReader(new InputStreamReader(new FileInputStream(idlist)));
			BufferedReader brfea =  new BufferedReader(new InputStreamReader(new FileInputStream(prediction)),1024*8);
			
			String idline = brid.readLine();
			String fealine = brfea.readLine();
			
			String lastVideo = idline.substring(0, idline.indexOf("/"));
			String thisvideoname = idline.substring(0, idline.indexOf("/"));
			int videocnt = 0;
			
			while(idline != null) {
				{
					int idinidfile = Integer.parseInt(idline.split(" ")[1]);
					int idinfeafile = -1;
					if(fealine.indexOf(" ") == -1) {
						System.out.println("empty line @" + fealine);
						idinfeafile = Integer.parseInt(fealine);
					} else {
						idinfeafile = Integer.parseInt(fealine.substring(0, fealine.indexOf(" ")));
						
					}
					
					if(idinidfile != idinfeafile) {
						System.out.println("some thing wrong at line number " + idinidfile);
					}
				}
				
				
				thisvideoname = idline.substring(0, idline.indexOf("/"));
				
				if(!lastVideo.equals(thisvideoname)) {
					
					//l1 norm
					for(int i = 0 ; i < spbow.length ; i++) {
						spbow[i] /= videocnt;
					}
					
					
					BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(outdir, lastVideo + ".spbof"))));
					bw.write(" ");
					for(int i = 0 ; i < (spbow.length-1) ; i++) {
						if(spbow[i] != 0)
							bw.write(""+(i+1)+":"+spbow[i]+" ");
					}
					
					bw.write(spbow.length+":"+spbow[spbow.length-1]);
					bw.flush();
					bw.close();
					
					//clear spbof
					for(int i = 0 ; i < spbow.length ; i++) {
						spbow[i] = 0;
					}
				
					lastVideo = thisvideoname;
					videocnt = 0;
					
				} 
					
				String[] temps = fealine.split(" ");
				for(int i = 0 ; i < (temps.length-1) ; i++) {
					spbow[i] = spbow[i] + Double.parseDouble(temps[i+1]);
				}
				videocnt++;
				
				idline = brid.readLine();
				fealine = brfea.readLine();
			}
			
			
			//flush the spbof
			{
				for(int i = 0 ; i < spbow.length ; i++) {
					spbow[i] /= videocnt;
				}
				BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(outdir, thisvideoname + ".spbof"))));
				bw.write(" ");
				for(int i = 0 ; i < (spbow.length-1) ; i++) {
					if(spbow[i] != 0)
						bw.write(""+(i+1)+":"+spbow[i]+" ");
				}
				
				bw.write(spbow.length+":"+spbow[spbow.length-1]);
				bw.flush();
				bw.close();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String dir = "E:\\SIN12\\MED12\\prediction\\med12dev";
		File prediction = new File(dir, "med12dev.txt");
		File idlist = new File(dir, "med12.dev.idlist");
		File outdir = new File(dir, "spbof");
		
		SVMPrediction2BoW svm = new SVMPrediction2BoW();
		svm.gen(prediction, idlist, outdir, 346);
		

	}

}
