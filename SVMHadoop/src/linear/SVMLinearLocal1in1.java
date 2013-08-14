package linear;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Date;


/**
 * Local version for SIN prediction.
 * @author lujiang
 *
 */
public class SVMLinearLocal1in1 {
	
	private String model_file_location;				//the file location of model file 346 concepts
	private int b_chunk_size = 1;					//the chunk size of B
	private String b_matrix_sift_file;				//the input file of B matrix sift
	private String out_prediction_file;				//the file name for the output
	private BoostingLinearSVM predictor;			//the cascade predictor
	
	
	public SVMLinearLocal1in1() throws IOException {
		super();
	}
	
	
	
	
	
	public void run() throws IOException {

		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(b_matrix_sift_file)),1024*8*1024);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(out_prediction_file))));
		String line = br.readLine();
		float[][] b_buffer = new float[b_chunk_size][];		//each row is a feature
		int[] featlineid = new int[b_chunk_size];
		int dim = predictor.dim;
		
		
		int linecnt = 0;
		
		
		while(line != null) {
			int thisindex  = linecnt%b_chunk_size;
			
			//read b matrix sift file
			String[] temps = line.trim().split("[ :]");
			b_buffer[thisindex] = new float[dim];
			featlineid[thisindex] = Integer.parseInt(temps[0]);
			
			
			if(temps.length/2 >= 1) {
				for(int i = 1 ; i < temps.length ; i=i+2) {
					int index = Integer.parseInt(temps[i])-1;
					b_buffer[thisindex][index] = Float.parseFloat(temps[i+1]);
				}
			} 
			
			line = br.readLine();
			linecnt++;
			
				
			
			if(linecnt%b_chunk_size ==  0) {
				//buffer is full
				
				System.out.println("readed No." + linecnt +" with chunk size  = " + this.b_chunk_size);
				System.out.println("---------------"+new Date(System.currentTimeMillis())+"-------------------");
				
				
				printMemory();
				//3) predict and write out the result.
				
				System.out.println("Predicting and writing the result...");
				System.out.println("---------------"+new Date(System.currentTimeMillis())+"-------------------");
				double[][] prediction = predictor.predictHadoop(featlineid, b_buffer);
				
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
				for(int j = 0 ; j < b_buffer.length ; j++) {
					b_buffer[j] = null;
				}
			}
		}
		
		
		//for the last chunk
		{
				//dump the buffer
				//0) count how many remaining chunk in the buffer
				int record_num = 0;
				for(int i = 0 ; i < b_buffer.length ; i++) {
					if(b_buffer[i] == null) {
						break;
					} else {
						record_num++;
					}
				}
				
				
				//move the last chunk to a compacted representation
				float[][] lastchunk = new float[record_num][];
				int[] lastfeatid = new int[record_num];
				for(int i = 0 ; i < record_num ; i++) {
					lastchunk[i] = b_buffer[i];
					lastfeatid[i] = featlineid[i];
				}
				
				
				
			
				System.out.println("flush " + record_num +"predictions into the out file.");
				System.out.println("---------------"+new Date(System.currentTimeMillis())+"-------------------");
				
				
				//predict and write out the result.
				System.out.println("Predicting and writing the result...");
				System.out.println("---------------"+new Date(System.currentTimeMillis())+"-------------------");
				double[][] prediction = predictor.predictHadoop(lastfeatid, lastchunk);
				//only write out the non-null record in the buffer
				for(int i = 0 ; i < prediction.length ; i++) {
					bw.write(((int)prediction[i][0]) + " ");	//line number is an integer
					
					for(int j = 1 ; j < prediction[i].length-1 ; j++) {
						bw.write(prediction[i][j] + " ");
					}
					bw.write(prediction[i][prediction[i].length-1] + "\n");
				}
				bw.flush();
				printMemory();
		}
		
		
		
		br.close();
		bw.close();

	}
	
	
	public void printMemory() {
		System.out.println("max-memory:"+(Runtime.getRuntime().maxMemory()/1024/1024)+
				"m:free-memory:"+(Runtime.getRuntime().freeMemory()/1024/1024)+
				"m:total"+(Runtime.getRuntime().totalMemory()/1024/1024)+"m");
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
		predictor = new BoostingLinearSVM(new File(model_file_location));
		printMemory();
	}



	public static void main(String args[]) throws Exception {
		if(args == null || args.length <= 2 || args.length >= 5) {
			System.out.print(
					 "Usage: SIN prediction local version. Having too many parameters. Just check the following code\n"+
					 "worker.setB_matrix_sift_file(args[0]);\n" +
					 "worker.setModel_file_location(args[1]);\n" +
					 "worker.setOut_prediction_file(args[2]);\n"+
					 "worker.setB_chunk_size(Integer.parseInt(args[3]));\n"
			);
			System.exit(1);
		}
		
		
		SVMLinearLocal1in1 worker = new SVMLinearLocal1in1();	
		worker.setB_matrix_sift_file(args[0]);	//e.g. G:\\ground-truth-check\\featuers\\evl\\sift-features.peek
		worker.setModel_file_location(args[1]);	//e.g. G:\\sin-models\\concept346.models
		worker.setOut_prediction_file(args[2]);	//G:\\ground-truth-check\\out-prediction.txt
		worker.setB_chunk_size(Integer.parseInt(args[3]));
		
		worker.run();
	}
	
}
