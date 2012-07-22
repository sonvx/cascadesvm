package local;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;


import beans.KernelRow;
import beans.svm_node;

/**
 * Local version for SIN prediction.
 * @author lujiang
 *
 */
public class SVMLocal {
	
	public static final int SIFT = 0, CSIFT = 1, MOSIFT = 2; 
	private KernelCalculator calculator;			//calculator that calculate kernel
	public int row_a;								//the number of row in A matrix(for early fusion the row number in all matrices should be same)
	private String a_matrix_sift_dir;				//the dir that contains A matrix sift
	private String a_matrix_csift_dir;				//the dir that contains A matrix csift
	private String a_matrix_mosift_dir;				//the dir that contains A matrix mosift
	private String model_file_location;				//the file location of model file 346 concepts
	private File[][] pathes;						//internal variable storing the dirs for A matrices.
	private int b_chunk_size = 1;					//the chunk size of B
	private String b_matrix_sift_file;				//the input file of B matrix sift
	private String b_matrix_csift_file;				//the input file of B matrix csift
	private String b_matrix_mosift_file;			//the input file of B matrix mosift
	private String out_prediction_file;				//the file name for the output
	private BoostingSVM predictor;					//the cascade predictor
	
	
	public SVMLocal() throws IOException {
		calculator = new KernelCalculator();
		pathes = new File[3][];
	
	}
	
	/**
	 * load a part of A matrix from the given file path. The part matrix should be in binary format.
	 * @param filepath
	 * @throws IOException
	 */
	public void loadAMatrix(File filepath) throws IOException {
		calculator.inmatrix = null;
		System.gc();		//control the memory while loading the block
		
		String filename = filepath.getName();
		int lineno = Integer.parseInt(filename.substring(filename.lastIndexOf("-")+1, filename.length()));
		calculator.inmatrix = new KernelRow[lineno];
		DataInputStream br = new DataInputStream(new BufferedInputStream(new FileInputStream(filepath),1024*32*1024));
		
		for(int i = 0 ; i < lineno ; i ++) {
			int thislineno = br.readInt();
			int arraysize = br.readInt();
			KernelRow thisrow = new KernelRow(thislineno, arraysize);
			for(int j = 0 ; j < arraysize ; j ++) {
				thisrow.indexes[j] = br.readShort();
				thisrow.values[j] = br.readFloat();
			}
			calculator.inmatrix[i] = thisrow;
		}
		//printMemory();
		br.close();
	}
	
	/**
	 * Calculate the kernel distance for early fusion
	 * @param input each row is a type of feature SIFT=0, CSIFT=1, MOSIFT=2 and each column is a line feature of that type.
	 * @param big the big matrix storing the kernel distance
	 * @return the svm node storing the kernel distance
	 * @throws IOException
	 */
	public ArrayList<svm_node[]> calDistanceEarlyFusion(KernelRow[][] input, float[][] big) throws IOException {
		//printMemory();
		float[][] small = new float[input[0].length][];
		int chunk_a_size = 0;
		
		for(int q = 0 ; q < 3 ; q++) {
			System.out.println("--working with " + pathes[q][0].getName());
			for(int p = 0  ; p < pathes[q].length ; p++) {
				loadAMatrix(pathes[q][p]);		//load a block from A usng the path pathes[q]
			
				for(int i = 0 ; i < input[0].length ; i++) {
					small[i] = calculator.chi2(input[q][i]);			//calculate the kernel	
				}
				if(p ==0 ) chunk_a_size = small[0].length;				//need this!
				//printMemory();
				addAppendToBig(big, small, p*chunk_a_size);				//early fusion add small to big
				//System.out.println("working with " + pathes[q][p].getName());
			}
			
			/*
			//debug
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File("G:\\dis"+q+".txt"))));
			
			for(int i = 0 ; i < big.length ; i++) {
				for(int j = 0 ; j < big[i].length ; j++) {
					bw.write((-1*(big[i][j]))+" ");
				}
				bw.write("\n");
			}	
			bw.flush();
			bw.close();
			
			for(int i = 0 ; i < big.length ; i++) {
				for(int j = 0 ; j < big[i].length ; j++) {
					//big[i][j] =0f;
				}
			}	
			//debug
			*/
		}
		
		
		
		for(int i = 0 ; i < big.length ; i++) {
			for(int j = 0 ; j < big[i].length ; j++) {
				big[i][j] = big[i][j]/3.0f;
			}
		}	
		
		
		
		
		//create svm node from big
		ArrayList<svm_node[]> out = new ArrayList<svm_node[]>();
		for(int i = 0 ; i < big.length ; i++) {
			svm_node[] nodearray = new svm_node[big[i].length+1];
			nodearray[0] = new svm_node();
			nodearray[0].index = 0;
			nodearray[0].value = input[0][i].lineid;
			
			for(int j = 0 ; j < big[i].length ; j++) {
				nodearray[j+1] = new svm_node();
				nodearray[j+1].index = (j+1);
				nodearray[j+1].value = big[i][j];
			}
			out.add(nodearray);
		}
		
		printMemory();
		return out;
	}
	
	
	/**
	 * Calculate kernel distance
	 * @param input each row is a type of feature SIFT=0, CSIFT=1, MOSIFT=2 and each column is a line feature of that type.
	 * @param big the big matrix storing the kernel distance
	 * @return the svm node storing the kernel distance
	 * @throws IOException
	 */
	public ArrayList<svm_node[]> calDistance(KernelRow[] input, float[][] big) throws IOException {
		printMemory();
		float[][] small = new float[input.length][];
		int chunk_a_size = 0;
		for(int p = 0  ; p < pathes.length ; p++) {
			loadAMatrix(pathes[p][0]);		//load A block
		
			for(int i = 0 ; i < input.length ; i++) {
				small[i] = calculator.chi2(input[i]);			//calculate the kernel	
			}
			if(p ==0 ) chunk_a_size = small[0].length;			//need this!
			printMemory();
			addAppendToBig(big, small, p*chunk_a_size);
			System.out.println("working with " + pathes[p][0].getName());
		}
		
		/*for(int i = 0 ; i < big.length ; i++) {
			for(int j = 0 ; j < big[i].length ; j++) {
				System.out.print((-1*big[i][j])+" ");
			}
			System.out.println();
		}*/
		
		ArrayList<svm_node[]> out = new ArrayList<svm_node[]>();
		
		for(int i = 0 ; i < big.length ; i++) {
			svm_node[] nodearray = new svm_node[big[i].length];
			nodearray[0] = new svm_node();
			nodearray[0].index = 0;
			nodearray[0].value = input[i].lineid;
			
			for(int j = 0 ; j < input[i].indexes.length ; j++) {
				nodearray[j+1] = new svm_node();
				nodearray[j+1].index = input[i].indexes[j];
				nodearray[j+1].value = input[i].values[j];
			}
			out.add(nodearray);
		}
		
		printMemory();
		return out;
	}
	
	/**
	 * Append and and the small kernel matrix (calculated from part of A matrix and B) into the big matrix (stroing the whole kernel distance between A and B)
	 * @param big the big matrix storing the kernel distance
	 * @param small the small matrix storing the kernel distance chunk of part A and B
	 * @param startIndex
	 */
	private void addAppendToBig(float[][] big, float[][] small, int startIndex) {
		for(int i = 0 ; i < big.length ; i++) {
			for(int j = 0 ; j < small[i].length ; j++) {
				big[i][startIndex+j] += small[i][j];		
			}
		}
	}
	
	
	/**
	 * decode the result from sortAPath(String a_matrix_dir) into a set of files
	 * @param in the output of sortAPath(String a_matrix_dir)
	 * @return a set of files
	 */
	private File[] topathes(String in) {
		String[] temp = in.split("#");
		File[] result = new File[temp.length];
		for(int i = 0 ; i < temp.length ; i++) {
			result[i] = new File(temp[i]);
		}
		
		return result;
	}
	
	
	/**
	 * Convert all the files in a dir into a string and sort their names
	 * @param a_matrix_dir a dir
	 * @return a string 
	 * @throws Exception
	 */
	private String sortAPath(String a_matrix_dir) throws Exception {
		File a_dir = new File(a_matrix_dir);
		File[] status = a_dir.listFiles();
		ArrayList<String> a_feature_binary_name = new ArrayList<String>();
		for (int i = 0; i < status.length; i++) {
			if (status[i].getName().indexOf("inpart") != -1) {
				a_feature_binary_name.add(status[i].getAbsolutePath());
			}
		}
		ArrayList<Integer> a_feature_ids = new ArrayList<Integer>();

		// extract small file id from the a_feature_binary file name
		for (int i = 0; i < a_feature_binary_name.size(); i++) {
			String tfilename = a_feature_binary_name.get(i);
			int id = -1;
			try{
				id = Integer.parseInt(tfilename.substring(
						tfilename.indexOf("inpart") + "inpart".length(),
						tfilename.lastIndexOf("-")));
			} catch(NumberFormatException e) {
				throw new Exception("The input A feature file name is bad formateed:" + tfilename +"\n" + e.toString());
			}
			a_feature_ids.add(id);

		}

		// sort the a_feature_binary according to the id! index starting from 0
		String result = "";
		for (int i = 0; i < a_feature_binary_name.size(); i++) {
			boolean found = false;
			int j = 0;
			for (; j < a_feature_ids.size(); j++) {
				if (i == a_feature_ids.get(j)) {
					found = true;
					break;
				}
			}
			if (found) {
				result += a_feature_binary_name.get(j) + "#";
			} else {
				throw new Exception(
						"the input A feature is not consecutive! Missing:" + i);
			}
		}
		result = result.substring(0, result.length() - 1);
		return result;
		
	}
	
	
	public void run() throws IOException {

		float[][] big = new float[b_chunk_size][row_a];
		
		
		BufferedReader br1 = new BufferedReader(new InputStreamReader(new FileInputStream(b_matrix_sift_file)),1024*8);
		BufferedReader br2 = new BufferedReader(new InputStreamReader(new FileInputStream(b_matrix_csift_file)),1024*8);
		BufferedReader br3 = new BufferedReader(new InputStreamReader(new FileInputStream(b_matrix_mosift_file)),1024*8);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(out_prediction_file))));
		
		String line = br1.readLine();
		KernelRow[][] b_buffer = new KernelRow[3][b_chunk_size];
		int linecnt = 0;
		while(line != null) {
			int thisindex  = linecnt%b_chunk_size;
			
			
			//read b matrix sift file
			String[] temps = line.split("[ :]");
			if(temps.length/2-1 > 0)
				b_buffer[SIFT][thisindex]= new KernelRow(Integer.parseInt(temps[0]), (temps.length/2-1));	//sift
			else
				b_buffer[SIFT][thisindex]= new KernelRow(Integer.parseInt(temps[0]),0);
			
			for(int i = 2 ; i < temps.length ; i=i+2) {	//skip the second position
				int index = Integer.parseInt(temps[i])-1;
				b_buffer[SIFT][thisindex].indexes[i/2-1] = (short)index;
				b_buffer[SIFT][thisindex].values[i/2-1] = Float.parseFloat(temps[i+1]);
				
			}
			
	
			//read b matrix csift file
			line = br2.readLine();
			temps = line.split("[ :]");
			if(temps.length/2-1 > 0)
				b_buffer[CSIFT][thisindex]= new KernelRow(Integer.parseInt(temps[0]), (temps.length/2-1));	//csift
			else
				b_buffer[CSIFT][thisindex]= new KernelRow(Integer.parseInt(temps[0]),0);
			
			for(int i = 2 ; i < temps.length ; i=i+2) {	//skip the second position
				int index = Integer.parseInt(temps[i])-1;
				b_buffer[CSIFT][thisindex].indexes[i/2-1] = (short)index;
				b_buffer[CSIFT][thisindex].values[i/2-1] = Float.parseFloat(temps[i+1]);
				
			}
			
			//read b matrix mosift file
			line = br3.readLine();
			temps = line.split("[ :]");
			if(temps.length/2-1 > 0)
				b_buffer[MOSIFT][thisindex]= new KernelRow(Integer.parseInt(temps[0]), (temps.length/2-1));	//mosift
			else
				b_buffer[MOSIFT][thisindex]= new KernelRow(Integer.parseInt(temps[0]),0);
			for(int i = 2 ; i < temps.length ; i=i+2) {	//skip the second position
				int index = Integer.parseInt(temps[i])-1;
				b_buffer[MOSIFT][thisindex].indexes[i/2-1] = (short)index;
				b_buffer[MOSIFT][thisindex].values[i/2-1] = Float.parseFloat(temps[i+1]);
				
			}
			
			
			line = br1.readLine();
			linecnt++;
			
			if(linecnt%b_chunk_size ==  0) {
				//buffer is full
				System.out.println("readed No." + linecnt +" with chunk size  = " + this.b_chunk_size);
				//1) clear the big matrix
				for(int i = 0 ; i < big.length ; i++) {
					for(int j = 0 ; j < big[i].length ; j++) {
						big[i][j] = 0;
					}
				}
				printMemory();
				//2) calculate the kernel distance
				System.out.println("Calculating the kernel matrix...");
				ArrayList<svm_node[]> ns = calDistanceEarlyFusion(b_buffer,big);
				printMemory();
				
				
				//3) predict and write out the result.
				
				System.out.println("Predicting and writing the result...");
				double[][] prediction = predictor.predictHadoop(ns);
				
				for(int i = 0 ; i < prediction.length ; i++) {
					
					bw.write(((int)prediction[i][0]) + " ");	//line number is an integer
					
					for(int j = 1 ; j < prediction[i].length-1 ; j++) {
						bw.write(prediction[i][j] + " ");
					}
					bw.write(prediction[i][prediction[i].length-1] + "\n");
				}
				bw.flush();
				printMemory();
				
				//4) clear the buffer
				for(int i = 0 ; i < 3 ; i++) {
					for(int j = 0 ; j < b_chunk_size ; j++) {
						b_buffer[i][j] = null;
					}
				}
			}
		}
		
		
		//for the last chunk
		{
				//dump the buffer
				//0) count how many remaining chunk in the buffer
				int record_num = 0;
				for(int i = 0 ; i < b_buffer[SIFT].length ; i++) {
					if(b_buffer[SIFT][i] == null) {
						break;
					} else {
						record_num++;
					}
				}
				
				//replace the null value with an empty kernel 
				
				for(int i = 0 ; i < b_buffer[0].length ; i++) {
					if(b_buffer[0][i] == null) {
						b_buffer[SIFT][i] = new KernelRow(-1,0);
						b_buffer[CSIFT][i] = new KernelRow(-1,0);
						b_buffer[MOSIFT][i] = new KernelRow(-1,0);
					}
				}
			
			
				System.out.println("flush" + linecnt +" with chunk size  = " + this.b_chunk_size);
				//1) clear the big matrix
				for(int i = 0 ; i < big.length ; i++) {
					for(int j = 0 ; j < big[i].length ; j++) {
						big[i][j] = 0;
					}
				}
				
				
				printMemory();
				//2) calculate the kernel distance
				System.out.println("Calculating the kernel matrix...");
				ArrayList<svm_node[]> ns = calDistanceEarlyFusion(b_buffer,big);
				printMemory();
				
				//3) predict and write out the result.
				System.out.println("Predicting and writing the result...");
				double[][] prediction = predictor.predictHadoop(ns);
				
				
				//only write out the non-null record in the buffer
				for(int i = 0 ; i < record_num ; i++) {
					for(int j = 0 ; j < prediction[i].length-1 ; j++) {
						bw.write(prediction[i][j] + " ");
					}
					bw.write(prediction[i][prediction[i].length-1] + "\n");
				}
				bw.flush();
				printMemory();
		}
		
		
		
		br1.close();
		br2.close();
		br3.close();
		bw.close();

	}
	
	
	public void printMemory() {
		System.out.println("max-memory:"+(Runtime.getRuntime().maxMemory()/1024/1024)+
				"m:free-memory:"+(Runtime.getRuntime().freeMemory()/1024/1024)+
				"m:total"+(Runtime.getRuntime().totalMemory()/1024/1024)+"m");
	}
	
	


	public String getA_matrix_sift_dir() {
		return a_matrix_sift_dir;
	}




	public void setA_matrix_sift_dir(String a_matrix_sift_dir) throws Exception {
		this.a_matrix_sift_dir = a_matrix_sift_dir;
		String pathesString = sortAPath(a_matrix_sift_dir);
		pathes[SIFT] = this.topathes(pathesString);
	}


	public String getA_matrix_csift_dir() {
		return a_matrix_csift_dir;
	}



	public void setA_matrix_csift_dir(String a_matrix_csift_dir) throws Exception {
		this.a_matrix_csift_dir = a_matrix_csift_dir;
		String pathesString = sortAPath(a_matrix_csift_dir);
		pathes[CSIFT] = this.topathes(pathesString);
	}




	public String getA_matrix_mosift_dir() {
		return a_matrix_mosift_dir;
	}


	public void setA_matrix_mosift_dir(String a_matrix_mosift_dir) throws Exception {
		this.a_matrix_mosift_dir = a_matrix_mosift_dir;
		String pathesString = sortAPath(a_matrix_mosift_dir);
		pathes[MOSIFT] = this.topathes(pathesString);
	}

	
	public int getRow_a() {
		return row_a;
	}


	public void setRow_a(int row_a) {
		this.row_a = row_a;
	}


	public int getB_chunk_size() {
		return b_chunk_size;
	}




	public void setB_chunk_size(int b_chunk_size) {
		this.b_chunk_size = b_chunk_size;
	}




	public String getB_matrix_sift_file() {
		return b_matrix_sift_file;
	}




	public void setB_matrix_sift_file(String b_matrix_sift_file) {
		this.b_matrix_sift_file = b_matrix_sift_file;
	}




	public String getB_matrix_csift_file() {
		return b_matrix_csift_file;
	}




	public void setB_matrix_csift_file(String b_matrix_csift_file) {
		this.b_matrix_csift_file = b_matrix_csift_file;
	}




	public String getB_matrix_mosift_file() {
		return b_matrix_mosift_file;
	}




	public void setB_matrix_mosift_file(String b_matrix_mosift_file) {
		this.b_matrix_mosift_file = b_matrix_mosift_file;
	}
	

	public String getOut_prediction_file() {
		return out_prediction_file;
	}


	public void setOut_prediction_file(String out_prediction_file) {
		this.out_prediction_file = out_prediction_file;
	}

	
	public String getModel_file_location() {
		return model_file_location;
	}


	public void setModel_file_location(String model_file_location) throws IOException, ClassNotFoundException {
		
		this.model_file_location = model_file_location;
		predictor = new BoostingSVM(new File(model_file_location));
		printMemory();
	}



	public static void main(String args[]) throws Exception {
		if(args == null || args.length < 9) {
			System.out.print(
					 "Usage: SIN prediction local version. Having too many parameters. Just check the following code\n"+
					 "  worker.setRow_a(Integer.parseInt(args[0]));	//e.g. 236697\n"+	 
					 "  worker.setA_matrix_sift_dir(args[1]);	//e.g. G:\\A_feature\\sift\n"+
					 "  worker.setA_matrix_csift_dir(args[2]);	//e.g. G:\\A_feature\\csift\n"+
					 "  worker.setA_matrix_mosift_dir(args[3]);	//e.g. G:\\A_feature\\mosift\n"+
					 "  worker.setB_matrix_sift_file(args[4]);	//e.g. G:\\sift-features.peek\n"+
					 "  worker.setB_matrix_csift_file(args[5]);	//e.g. G:\\csift-features.peek\n"+
					 "  worker.setB_matrix_mosift_file(args[6]);	//e.g. G:\\mosift-features.peek\n"+
					 "  worker.setModel_file_location(args[7]);	//e.g. G:\\sin-models\\concept346.models\n"+
					 "  worker.setOut_prediction_file(args[8]);	//G:\\ground-truth-check\\out-prediction.txt\n"+
					 " if(args.length == 10)  worker.setB_chunk_size(Integer.parseInt(args[9]));\n"+
					 " else worker.setB_chunk_size(20);\n"
			);
			System.exit(1);
		}
		SVMLocal worker = new SVMLocal();
		
		worker.setRow_a(Integer.parseInt(args[0]));	//e.g. 236697
		worker.setA_matrix_sift_dir(args[1]);	//e.g. G:\\A_feature\\sift
		worker.setA_matrix_csift_dir(args[2]);	//e.g. G:\\A_feature\\csift
		worker.setA_matrix_mosift_dir(args[3]);	//e.g. G:\\A_feature\\mosift
		worker.setB_matrix_sift_file(args[4]);	//e.g. G:\\ground-truth-check\\featuers\\evl\\sift-features.peek
		worker.setB_matrix_csift_file(args[5]);	//e.g. G:\\ground-truth-check\\featuers\\evl\\csift-features.peek
		worker.setB_matrix_mosift_file(args[6]);	//e.g. G:\\ground-truth-check\\featuers\\evl\\mosift-features.peek
		worker.setModel_file_location(args[7]);	//e.g. G:\\sin-models\\concept346.models
		worker.setOut_prediction_file(args[8]);	//G:\\ground-truth-check\\out-prediction.txt
		if(args.length == 10) {
			worker.setB_chunk_size(Integer.parseInt(args[9]));
		} else {
			worker.setB_chunk_size(20);
		}

		worker.run();
	}
	
}
