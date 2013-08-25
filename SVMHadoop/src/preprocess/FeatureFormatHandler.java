package preprocess;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;

public class FeatureFormatHandler {
	
	
	/**
	 * Read the Weka input file *.arff and produce one or more Libsvm format files.
	 * The input feature file have no nominal type feature and each dimension is normalized.
	 * No missing value is allowed(use ReplaceMissingValues filter first)
	 * @param inweka
	 * @throws IOException 
	 */
	public static void weka2libsvm(File inweka, File outdir) throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(inweka)));
		
		String line = br.readLine();
		
		ArrayList<String> classes = null;
		
		while(line != null) {
			if(line.toLowerCase().startsWith("@attribute class")) {
				String[] temps = line.substring(line.indexOf("{")+1, line.indexOf("}")).split("[, ]+");
				classes = new ArrayList<String>(Arrays.asList(temps));
			} else if(line.toLowerCase().startsWith("@data")) {			
				break;
			}
			line = br.readLine();
		}
		
		BufferedWriter[] bws = new BufferedWriter[classes.size()];
		for(int i = 0 ; i < bws.length ; i++) {
			bws[i] = new BufferedWriter(new FileWriter(new File(outdir, classes.get(i)+".feat")));
		}
		
		line = br.readLine();
		
		while(line != null) {
			String thislabel = line.substring(line.lastIndexOf(",")+1);
			String thisfeatline = line.substring(0, line.lastIndexOf(","));
			String outfeatline = wekaline2svmline(thisfeatline);
			
			for(int i = 0 ; i < bws.length ; i++) {
				String outlabel = classes.get(i).equals(thislabel) ? "1" : "-1";
				bws[i].write(outlabel + " " + outfeatline+"\n");
			}
			
			
			line = br.readLine();
		}
		
		
		for(int i = 0 ; i < bws.length ; i++) {
			bws[i].flush();
			bws[i].close();
		}
	}
	
	
	/**
	 * Pack the input feature file according to the idlist file in libsvm format.
	 * @param featDir
	 * @param idlistFile
	 * @param outfile
	 * @throws IOException
	 */
	public static void packFeat(File featDir, File idlistFile, File outfile, String postfix) throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(idlistFile)));
		String line = br.readLine();
		ArrayList<String> idlist = new ArrayList<String>();
		
		while(line != null) {
			idlist.add(line.split("[ \t]+")[0]);
			line = br.readLine();
		}
		
		br.close();
		
		BufferedWriter bw = new BufferedWriter(new FileWriter(outfile));
		for(int i = 0 ; i < idlist.size() ; i++) {
			if(i % 1000 == 0) System.out.print(i);
			File featfile = new File(featDir, idlist.get(i)+postfix);
			if(featfile.exists()) {
				BufferedReader brfeat = new BufferedReader(new InputStreamReader(new FileInputStream(featfile)));
				String featline = brfeat.readLine();
				bw.write(featline.trim());
				brfeat.close();
			}
			bw.write("\n");
		}
		
		bw.flush();
		bw.close();
	}
	
	
	
	/**
	 * Column bind the packed feature file and idlist file.
	 * @param onePackedFeatureFile
	 * @param idFile
	 * @param outfile
	 * @throws IOException
	 */
	public static void cbindLabelFeat(File onePackedFeatureFile, File idFile, File outfile) throws IOException {
		BufferedReader brf = new BufferedReader(new InputStreamReader(new FileInputStream(onePackedFeatureFile)));
		BufferedReader brid = new BufferedReader(new InputStreamReader(new FileInputStream(idFile)));
		BufferedWriter bw = new BufferedWriter(new FileWriter(outfile));
		
		
		String idline = brid.readLine();
		String featline = brf.readLine();
		
		while(featline != null) {
			int kid = Integer.parseInt(idline.split("[ ]+")[0]);
			bw.write(kid==1 ? "1" : "-1");
			bw.write(" ");
			bw.write(featline);
			bw.write("\n");
			featline = brf.readLine();
			idline = brid.readLine();
		}
		
		bw.flush();
		bw.close();
		
	}
	
	
	private static String wekaline2svmline(String line) {
		String[] tokens = line.split("[, ]+");
		StringBuffer buffer = new StringBuffer();
		for(int i = 0 ; i < tokens.length ; i++) {
			buffer.append((i+1)+":"+Double.parseDouble(tokens[i]) +" ");
		}
		return buffer.toString().trim();
	}


	public static void main(String[] args) throws IOException {
		//weka2libsvm(new File("G:\\a\\f\\mfeat-factors-normalized.arff"), new File("G:\\a\\f\\mfeat-factors"));
		//packFeat(new File(args[0]), new File(args[1]), new File(args[2]), args[3]);
		cbindLabelFeat(new File(args[0]), new File(args[1]), new File(args[2]));

	}

}
