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

public class SingleSVMFileGenerator {
	
	
	public void formatCheck(File indir, File idlist) throws IOException{
		ArrayList<String> shotIDindex = new ArrayList<String>();
		shotIDindex.add("dumb");
		
		BufferedReader br =  new BufferedReader(new InputStreamReader(new FileInputStream(idlist)),1024*8);
		String line = br.readLine();
		while(line != null) {
			shotIDindex.add(line);
			line = br.readLine();
		}
		br.close();
		int missingshot = 0;
		for(int i = 1 ; i < shotIDindex.size() ; i++) {
			File thisfile = new File(indir, shotIDindex.get(i)+".spbof");
			if(thisfile.exists()) {
				BufferedReader brlocal =  new BufferedReader(new InputStreamReader(new FileInputStream(thisfile)));
				String svm = brlocal.readLine();
				if(!svmFormatCheck(thisfile.getName(), svm)) System.exit(1);
				brlocal.close();
				
			} else {
				missingshot++;
				System.out.println(missingshot + " missing " + shotIDindex.get(i));
			}
			
			if(i%10000==0) {
				System.out.println("checked" + i);
			}
		}
	}
	
	private boolean svmFormatCheck(String filename, String line) {
		String[] temps = line.split("[ :]+");
		int i = 1;
		try {
			//Integer.parseInt(temps[0]);		//line no
			//skip the second position
			for(; i < temps.length ; i=i+2) {
				Integer.parseInt(temps[i]);
				Float.parseFloat(temps[i+1]);
			}
		} catch(Exception e) {
			System.out.println("format error in file " + filename + "indexnumber " + i);
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	public void concate(File indir, File idlist, File outfile) throws IOException {
		
		ArrayList<String> shotIDindex = new ArrayList<String>();
		shotIDindex.add("dumb");
		
		BufferedReader br =  new BufferedReader(new InputStreamReader(new FileInputStream(idlist)),1024*8);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outfile)),1024*8);
		int missingshot = 0;
		String line = br.readLine();
		while(line != null) {
			shotIDindex.add(line);
			line = br.readLine();
		}
		
		for(int i = 1 ; i < shotIDindex.size() ; i++) {
			File thisfile = new File(indir, shotIDindex.get(i)+".spbof");
			if(thisfile.exists()) {
				BufferedReader brtemp =  new BufferedReader(new InputStreamReader(new FileInputStream(thisfile)));
				String svm = brtemp.readLine();
				bw.write(""+i);
				bw.write(" ");
				bw.write(svm);
				bw.write("\r\n");
				brtemp.close();
			} else {
				missingshot++;
				System.out.println(missingshot + " missing " + shotIDindex.get(i));
				bw.write(""+i);
				bw.write(" ");
				bw.write("\r\n");
			}
		}
		
		bw.flush();
		bw.close();
	}
	
	
	

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		if(args.length < 3) { 
			System.out.println("usage: infiledir, idlist, outfilename, [mode = 0 (without format check), = 1 (with format check default)]");
		}
		
		SingleSVMFileGenerator g = new SingleSVMFileGenerator();
		File infileidr = new File(args[0]);
		File idlist = new File(args[1]);
		File outfile = new File(args[2]);
		if(args.length !=4 || !args[3].equals("0")) {
			g.formatCheck(infileidr, idlist);
		}
		
		g.concate(infileidr, idlist, outfile);
	}

}
