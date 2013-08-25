package preprocess;

import java.io.BufferedOutputStream;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;

import beans.IDEntry;




class AMatrixPartitioneThread extends Thread {
	public static int FEATURE_DIMENSION_SHORT = 0; 
	public static int FEATURE_DIMENSION_INT = 1; 
	public int featureDimType = FEATURE_DIMENSION_SHORT;
	public int threadID = -1;
	public File inFeatdir;
	public File outfile;
	private static int threadIDCount = -1;
	private ArrayList<IDEntry> idlist;
	
	public AMatrixPartitioneThread() {
		super();
	}
	
	
	public AMatrixPartitioneThread(ThreadGroup tg, String threadname) {
	    super(tg, threadname);
}
	
	public synchronized void initialize(ArrayList<IDEntry> subidlist, File inFeatdir, File outfile) {
		initialize(subidlist, FEATURE_DIMENSION_SHORT, inFeatdir, outfile);
	}
	
	public synchronized void initialize(ArrayList<IDEntry> subidlist, int featureDimType, File inFeatdir, File outfile) {
		threadID = getThreadID();
		this.featureDimType = featureDimType;
		this.idlist = subidlist;
		this.inFeatdir = inFeatdir;
		this.outfile = outfile;
	}
	
	
	public static synchronized int getThreadID() {
		return threadIDCount++;
	}
	
	
	
	public void run() {
		try {
			partition(inFeatdir, idlist, outfile);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	/**
	 * Input feature file has no label information. Each file has a single line e.g.
	 *  2:1.029854e-05 5:7.127242e-05 8:1.647766e-04 9:1.945279e-05
	 * @param inFeatdir
	 * @param idlist
	 * @param outfile
	 * @throws IOException
	 */
	public void partition(File inFeatdir, ArrayList<IDEntry> idlist, File outfile) throws IOException {
		
		DataOutputStream bw = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(outfile)));
		
		
		for(int i = 0 ; i < idlist.size() ; i++) {
			File singleLineFeatureFile = new File(inFeatdir, idlist.get(i).getFeatureFileName());
			String line = "";
			
			
			if(singleLineFeatureFile.exists()) {
				BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(singleLineFeatureFile)));
				line = br.readLine();
				br.close();
			}
			String[] temps = new String[0];
			if(line != null && !line.equals("")) {
				line = line.trim();
				temps = line.split("[ :]+");
			}
			
			bw.writeInt(idlist.get(i).lineid);		//line no
			bw.writeInt((temps.length)/2);
			
	
			if(featureDimType == FEATURE_DIMENSION_SHORT) {
				for(int j = 0 ; j < temps.length ; j=j+2) {
					short index = (short)(Integer.parseInt(temps[j])-1);
					bw.writeShort(index);
					bw.writeFloat(Float.parseFloat(temps[j+1]));	
				}
			} else if(featureDimType == FEATURE_DIMENSION_INT) {
				for(int j = 0 ; j < temps.length ; j=j+2) {
					int index = (Integer.parseInt(temps[j])-1);
					bw.writeInt(index);
					bw.writeFloat(Float.parseFloat(temps[j+1]));	
				}
			} else {
				throw new IOException("featureDimType not known");
			}
		}
		
		bw.flush();
		bw.close();
	}
	
	
	/**
	 * @deprecated deprecate the single thread version
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
				
				File tfn = new File(indir, temps[0]);
				
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
				int index = (Integer.parseInt(temps[i])-1);
				bw.writeInt(index);								//here we write Int
				bw.writeFloat(Float.parseFloat(temps[i+1]));
				
			}
			
			idline = idbr.readLine();
			
		}
		
		bw.flush();
		bw.close();
		
		File lastfile = new File(outdir, filenameprefix+"-inpart" + filecounter);
		lastfile.renameTo(new File(outdir, File.separator + filenameprefix+"-inpart" + filecounter + "-" +linecounter));
	}
	
	/**
	 * @deprecated Not efficient
	 * @param in
	 * @param outdir
	 * @param partno
	 * @param prefix
	 * @throws IOException
	 */
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
	

}


public class AMatrixPartitioner {
	
	
	class AJob {
		ArrayList<IDEntry> idlist;
		File outfile;
		
		public AJob() {
			idlist = new ArrayList<IDEntry>();
			outfile = null;
		}
	}
	
	
	
	
	public ArrayList<IDEntry> loadIDList(File idfile) throws IOException {
		ArrayList<IDEntry> idlist = new ArrayList<IDEntry>();
		BufferedReader idbr = new BufferedReader(new InputStreamReader(new FileInputStream(idfile)));
		String idline = idbr.readLine();
		int linenumber = 0;
		while(idline != null) {
			linenumber++;
			IDEntry entry = new IDEntry();
			String[] temps = idline.split(" ");
			entry.id = temps[0];
			entry.lineid = linenumber;
			idlist.add(entry);
			idline = idbr.readLine();
		}
		return idlist;
	}
	
	
	public void run(File inFeatDir, File idfile, File outdir, int chunksize, String filenameprefix) {
		run(inFeatDir, idfile, outdir, chunksize, filenameprefix, 3);
	}
	
	
	public void run( File inFeatDir, File idfile, File outdir, int chunksize, String filenameprefix, int threadNumber) {
		try {
			
			ArrayList<IDEntry> idlist = loadIDList(idfile);
			ArrayList<AJob> jobs = new ArrayList<AJob>();
			int linecounter = 0;
			int jobcounter = 0;
			
			jobs.add(new AJob());
			for(int i = 0 ; i < idlist.size() ; i++) {
				linecounter++;
				
				
				if(linecounter%chunksize == 0) {
					jobs.get(jobcounter).outfile = new File(outdir,filenameprefix+"-inpart" + jobcounter + "-" + (linecounter-1));
					jobs.add(new AJob());
					linecounter = 1;
					jobcounter++;
				}
				jobs.get(jobcounter).idlist.add(idlist.get(i));
			}
			
			jobs.get(jobcounter).outfile = new File(outdir,filenameprefix+"-inpart" + jobcounter + "-" +linecounter);
			
			
			
			Queue<AJob> queue = new LinkedList<AJob>();
			queue.addAll(jobs);
			
			ThreadGroup group = new ThreadGroup("AMatrixPartitioner");
			while(queue.peek() != null) {
				try {
					//System.err.println("Current Active Threads Number:" + group.activeCount());
					if(group.activeCount() < threadNumber) {
						//Thread.sleep(100);
						AMatrixPartitioneThread newthread = new AMatrixPartitioneThread(group,"ajob");
						AJob job = queue.poll();
						newthread.initialize(job.idlist, inFeatDir, job.outfile);
						newthread.start();
					}
					Thread.sleep(100);
						
				} catch (Exception e) {
					e.printStackTrace();
				}
					
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
	}
	
	
	public static void main(String args[]) {
		if(args == null) {
			String help = "chunk A into files\n"
					+ "  partition(File indir, File idlist, File outdir, int (chunksize-1), String filenameprefix)\n";
			System.out.print(help);
			System.exit(1);
		}
		AMatrixPartitioner partitioner = new AMatrixPartitioner();
		partitioner.run(new File(args[0]), new File(args[1]), new File(args[2]), Integer.parseInt(args[3]), args[4]);
	
	}

}
