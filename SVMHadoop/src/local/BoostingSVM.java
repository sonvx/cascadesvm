package local;


import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.ArrayList;

import beans.svm_model;
import beans.svm_node;

public class BoostingSVM {
	public svm_model[][] models;
	public int numOfModels = 0;
	
	
	public double[] gammas = new double[] { Math.pow(2, 3), Math.pow(2, 0),
			Math.pow(2, -3), Math.pow(2, -6), 0.00195312,
			0.000244141, 3.05176e-05};
	
	//for test
	/*public double[] gammas = new double[] { Math.pow(2, 3), Math.pow(2, 0),
			Math.pow(2, -3), Math.pow(2, -6), Math.pow(2, -9),
			Math.pow(2, -12), Math.pow(2, -15),
			Math.pow(2, -4),Math.pow(2, -2)};*/
			
	
	public BoostingSVM(File indir, File indexfile) throws IOException {
		this(indir, indexfile,346);
	}
	
	
	public BoostingSVM(File indir, File indexfile, int numofmodels) throws IOException {
		models = new svm_model[numofmodels][];
		ArrayList<String> names = loadNames(indexfile);
		for(int i = 0 ; i < names.size() ; i++) {
		
			File modeldir = new File(indir, names.get(i)+File.separator);
			
			File[] models = modeldir.listFiles();
			this.models[i] = new svm_model[models.length];
			for(int j = 0 ; j < models.length ; j++) {
				if(!models[j].getName().endsWith("performance")) {
					svm_model m = SVMPrediction.svm_load_model(models[j]);
					m.modelID = i;
					m.layerID = Integer.parseInt(models[j].getName().substring(models[j].getName().lastIndexOf(".")+1,models[j].getName().length()));
					m.concept = names.get(i);
					this.models[i][m.layerID] = m;
					numOfModels++;
				}
			}
		}
	}
	
	
	

	public BoostingSVM(File inmodel) throws IOException, ClassNotFoundException {
		this(new FileInputStream(inmodel));
	}
	
	
	public BoostingSVM(InputStream inmodel) throws IOException, ClassNotFoundException {
		ObjectInput br = new ObjectInputStream(new BufferedInputStream(inmodel));
		models = new svm_model[br.readInt()][];
		for(int i = 0 ; i < models.length ; i++) {
			models[i] = new svm_model[br.readInt()];
			for(int j = 0 ; j < models[i].length ; j++) {
				models[i][j] = (svm_model) br.readObject();
				numOfModels++;
			}
		}
		br.close();
		
		System.out.println("loaded SIN models #" + numOfModels);
	}
	
	
	public double[][] predictHadoop(ArrayList<svm_node[]> in) throws IOException {
		double[][] result = new double[in.size()][];
		for(int i = 0 ; i < result.length ; i++) {
			result[i] = this.predictHadoop(in.get(i));
		}
		return result;
	}
	
	/**
	 * for Lei's distance Exp(-1*gamma*dis)
	 * @param in
	 * @param out
	 * @param gamma
	 */
	public void expMinus(svm_node[] in, svm_node[] out, double gamma) {
		for (int j = 1; j < in.length; j++) {
			out[j].value = Math.exp(-1*in[j].value * gamma);
		}
	}
	
	
	/**
	 * Exp(-1*gamma*dis)
	 * @param in
	 * @param out
	 * @param gamma
	 */
	public void exp(svm_node[] in, svm_node[] out, double gamma) {
		for (int j = 1; j < in.length; j++) {
			out[j].value = Math.exp(in[j].value * gamma);
		}
	}
	
	
	
	/**
	 * cascade average version
	 * @param in
	 * @return
	 * @throws IOException
	 */
	public double[] predictHadoop(svm_node[] in) throws IOException {
		//SVMPrediction.predictHadoop(x, output, model);
		
		svm_node[] x = new svm_node[in.length];
		for(int i = 0 ; i < x.length ; i++) {
			x[i] = new svm_node();
			x[i].index = in[i].index;
			x[i].value = in[i].value;
		}
		
		
		double[] predictions = new double[numOfModels];
		
		for(int i = 0 ; i < predictions.length ; i++) {
			predictions[i] = -1;
		}
		
		
		for(int i = 0 ; i < gammas.length ; i++) {
			
			int model_cnt = 0;
			
			//1. exp the svm_node
			exp(in, x, gammas[i]);
			
			//2. iterate all models and find the ones has the parameter gamma
			for (int j = 0; j < models.length; j++) {
				for (int k = 0; k < models[j].length; k++) {
					if(models[j][k].param.gamma == gammas[i]) {
						predictions[model_cnt] = SVMPrediction.predictHadoop(x, models[j][k]);
					}
					model_cnt++;	
				}
			}
		}
		
		
		
		
		//get final prediction 346 from the prediction 5768
		double[] finalprediction = new double[models.length];
		int model_cnt = 0;
		for (int j = 0; j < models.length; j++) {			//for each class
			finalprediction[j] = 0.0;
			for (int k = 0; k < models[j].length; k++) {	//for all layers in the class
				finalprediction[j] += (1.0d/models[j].length)*predictions[model_cnt];
				model_cnt++;
			}
			finalprediction[j] = finalprediction[j];
		}
		
		double[] fpredictionwithlineno = new double[models.length+1];
		fpredictionwithlineno[0] = in[0].value;
		for(int i = 0 ; i < finalprediction.length ; i++) {
			fpredictionwithlineno[i+1] = finalprediction[i];
		}
		

		return fpredictionwithlineno;
	}
	
	
	protected static void printMatrix(double[][] matrix) {
		for(int i = 0 ; i < matrix.length ; i++) {
			for(int j = 0 ; j < matrix[i].length ; j++) {
				System.out.print(matrix[i][j] + " ");
			}
			System.out.print("\r\n");
		}
	}
	
	
	/**
	 * write the models into a single file 
	 * @param out the location for the output file
	 * @throws IOException
	 */
	public void writeModels(File out) throws IOException {
		ObjectOutput output = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(out)));
		output.writeInt(models.length);
		for(int i = 0 ; i < models.length ; i++) {
			output.writeInt(models[i].length);
			for(int j = 0 ; j < models[i].length; j++) {
				output.writeObject(models[i][j]);
			}
		}
		output.flush();
		output.close();
	}
	
	
	private ArrayList<String> loadNames(File indexfile) throws IOException {
		ArrayList<String> names = new ArrayList<String>(346);
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(indexfile)));
		String line = br.readLine();
		while (line != null) {
			names.add(line);
			line = br.readLine();
		}
		
		br.close();
		return names;
	}
	
	public static void main(String[] args) throws Exception {
		/*
		ArrayList<svm_node[]> nodes = SVMPrediction.load_svm_node_matrix(new File("G:\\ground-truth-check\\distance\\csift-distance.peek"));
	
		BoostingSVM b = new BoostingSVM(new File("G:\\sin-models\\concept346.models"));
		double[][] re = b.predictHadoop(nodes);
		printMatrix(re);
		*/

		
		/**
		 * write models into the model file.
		 */
		//BoostingSVM b = new BoostingSVM(new File("G:\\sin-models\\concept_346\\"), new File("G:\\sin-models\\Visual_Concept_List.txt"),346);
		//b.writeModels(new File("G:\\sin-models\\concept346.models"));
		
		
		
		/*
		 *BoostingSVM b = new BoostingSVM(new File("G:\\smalltest\\value\\models\\models"), new File("G:\\smalltest\\value\\models\\names.txt"),4);
		  b.writeModels(new File("G:\\smalltest\\value\\models\\4models.models"));
		 */
	}

	
	
	
	
	
	
	
	
	
	
}
