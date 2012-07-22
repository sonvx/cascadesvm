package preprocess;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;



public class PredictionRbind {
	
	public static Pattern idpatern = Pattern.compile("prediction-p([0-9]+).txt");
	
	
	
	/**
	 * 
	 * @param indir dir of the prediction file in text format
	 * @throws IOException 
	 */
	public void rbind(File indir, File outfile) throws IOException {
		
		File[] subfiles = indir.listFiles();
		int maxid = Integer.MIN_VALUE;
		int minid = Integer.MAX_VALUE;
		for(int i = 0 ; i < subfiles.length ; i++) {
			Integer thisid;
			Matcher m = idpatern.matcher(subfiles[i].getName());
			if(m.find()) {
				thisid = Integer.parseInt(m.group(1));
				if(thisid > maxid) maxid = thisid;
				if(thisid < minid) minid = thisid;
			}
		}
		
		
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outfile)));
	
		for(int i = minid ; i <= maxid ; i++) {
			File thisfile = new File(indir, "prediction-p" + i + ".txt");
			if(!thisfile.exists()) {
				System.err.println("the id of the prediction file is not consecutive and " + "prediction-p" + i + " is missing.");
				System.exit(1);
			} else {
				BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(thisfile)));
				String line = br.readLine();
				while(line != null) {
					bw.append(line+"\n");
					line = br.readLine();
				}
				br.close();
			}
		}
		
		bw.flush();
		bw.close();
	}
	
	
	/**
	 * split the big prediction file into 346 prediction file for each concept
	 * @param conceptlist
	 * @param bigpredictionfile
	 * @param out
	 * @throws IOException
	 */
	public void splitBigPrediction(File bigpredictionfile, File conceptlist, File out) throws IOException {
		ArrayList<String> conarray = new ArrayList<String>();
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(conceptlist)));
	
		String line = br.readLine();
		while(line != null) {
			conarray.add(line);
			line = br.readLine();
		
		}
		br.close();
		
		BufferedWriter[] bws = new BufferedWriter[conarray.size()];
		
		for(int i = 0 ; i < conarray.size() ; i++) {
			File mydir = new File(out, conarray.get(i));
			if(!mydir.exists()) mydir.mkdir();
			bws[i] = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(mydir, conarray.get(i)+".prediction"))));
			bws[i].write("label 1 0\r\n");
		}
		
		
		br = new BufferedReader(new InputStreamReader(new FileInputStream(bigpredictionfile)));
		line = br.readLine();
		while(line != null) {
			String[] temps = line.split(" ");
			//temp[0] is the line number
			for(int j = 0 ; j < bws.length ; j++) {
				double pp = Double.parseDouble(temps[j+1]);
				double np = 1 - pp;
				int label = pp > np ? 1 : 0;
				bws[j].write(label + " " + pp + " " + np + "\r\n");
			}
			line = br.readLine();
		}
		
		
		for(int j = 0 ; j < bws.length ; j++) {
			bws[j].flush();
			bws[j].close();
		}
		
	}

	
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		PredictionRbind worker = new PredictionRbind();
		worker.rbind(new File(args[0]), new File(args[1]));
		
		
		/*
		worker.splitBigPrediction(new File("G:\\ground-truth-check\\test-prediction\\12\\sin12-prediction.txt"), 
				new File("G:\\ground-truth-check\\test-prediction\\12\\Visual_Concept_List.txt") ,
				new File("G:\\ground-truth-check\\test-prediction\\12\\concept\\"));*/
	}

}
