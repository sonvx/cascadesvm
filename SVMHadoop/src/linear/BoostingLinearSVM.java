package linear;


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

import local.SVMPrediction;


public class BoostingLinearSVM {
	public svm_linear_model[][] models;
	public int numOfModels = 0;
	public int dim = 0;
	
	
	
	//for test
	/*public double[] gammas = new double[] { Math.pow(2, 3), Math.pow(2, 0),
			Math.pow(2, -3), Math.pow(2, -6), Math.pow(2, -9),
			Math.pow(2, -12), Math.pow(2, -15),
			Math.pow(2, -4),Math.pow(2, -2)};*/
			
	
	public BoostingLinearSVM(File indir, File indexfile) throws IOException {
		this(indir, indexfile,346);
	}
	
	
	public BoostingLinearSVM(File indir, File indexfile, int numofmodels) throws IOException {
		models = new svm_linear_model[numofmodels][];
		ArrayList<String> names = loadNames(indexfile);
		
		int numlayer = new File(indir, names.get(0)+File.separator).listFiles().length;
		for(int i = 0 ; i < names.size() ; i++) {
			
			File modeldir = new File(indir, names.get(i)+File.separator);
			File[] models = modeldir.listFiles();
			int modellength = 0;
			for(int j = 0 ; j < models.length ; j++) {
				if(!models[j].getName().endsWith("performance") && !models[j].getName().endsWith("output")) modellength++;
			}
			
			this.models[i] = new svm_linear_model[modellength];
			if(models.length != numlayer)	System.err.println(names.get(i));
			for(int j = 0 ; j <models.length ; j++) {
				if(!models[j].getName().endsWith("performance") && !models[j].getName().endsWith("output")) {
					svm_linear_model m = SVMPrediction.svm_load_linear_model(models[j]);
					m.modelID = i;
					m.layerID = Integer.parseInt(models[j].getName().substring(models[j].getName().lastIndexOf(".")+1,models[j].getName().length()));
					m.concept = names.get(i);
					this.models[i][m.layerID] = m;
					numOfModels++;
				}
			}
		}
		
		if(models.length > 0 && models[0].length >0 && models[0][0] != null) {
			this.dim = models[0][0].dim;
		}
	}
	
	
	

	public BoostingLinearSVM(File inmodel) throws IOException, ClassNotFoundException {
		this(new FileInputStream(inmodel));
	}
	
	
	public BoostingLinearSVM(InputStream inmodel) throws IOException, ClassNotFoundException {
		ObjectInput br = new ObjectInputStream(new BufferedInputStream(inmodel));
		models = new svm_linear_model[br.readInt()][];
		for(int i = 0 ; i < models.length ; i++) {
			models[i] = new svm_linear_model[br.readInt()];
			for(int j = 0 ; j < models[i].length ; j++) {
				models[i][j] = (svm_linear_model) br.readObject();
				numOfModels++;
			}
		}
		br.close();
		
		
		if(models.length > 0 && models[0].length >0 && models[0][0] != null) {
			this.dim = models[0][0].dim;
		}
		System.out.println("loaded SIN models #" + numOfModels);
	}
	
	
	public double[][] predictHadoop(int[] featlinids, float[][] infeaturemat) throws IOException {
		double[][] result = new double[infeaturemat.length][];
		for(int i = 0 ; i < result.length ; i++) {
			result[i] = this.linearPredictHadoop(featlinids[i], infeaturemat[i]);
		}
		return result;
	}
	
	
	
	
	/**
	 * cascade average version
	 * @param infeature feature (dense vector)
	 * @return
	 * @throws IOException
	 */
	public double[] linearPredictHadoop(int featlineid, float[] infeaturevet) throws IOException {
		//SVMPrediction.predictHadoop(x, output, model);
		
		
		
		double[] predictions = new double[numOfModels];
		
		for(int i = 0 ; i < predictions.length ; i++) {
			predictions[i] = -1;
		}
		
		
		int model_cnt = 0;
		for (int j = 0; j < models.length; j++) {
			for (int k = 0; k < models[j].length; k++) {
				predictions[model_cnt] = SVMPrediction.linearPredictHadoop(infeaturevet, models[j][k]);
				model_cnt++;	
			}
		}
		
		
		
		
		double[] finalprediction = new double[models.length];
		model_cnt = 0;
		for (int j = 0; j < models.length; j++) {			//for each class
			finalprediction[j] = 0.0;
			for (int k = 0; k < models[j].length; k++) {	//for all layers in the class
				finalprediction[j] += (1.0d/models[j].length)*predictions[model_cnt];
				model_cnt++;
			}
			finalprediction[j] = finalprediction[j];
		}
		
		double[] fpredictionwithlineno = new double[models.length+1];
		fpredictionwithlineno[0] = featlineid;
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
			names.add(line.split(" ")[0]);
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
		/*
		BoostingSVM b = new BoostingSVM(new File("\\\\mmdb\\e$\\SIN12\\model\\SIN12-SIN12\\SVM\\sin12-concept346.models"));
		
		for (int j = 0; j < b.models.length; j++) {
			for (int k = 0; k < b.models[j].length; k++) {
				if(b.models[j][k].param.gamma == b.gammas[0]) {
					
				}

			}
		}*/
		/**
		 * write models into the model file.
		 *//*
		BoostingSVM b = new BoostingSVM(new File("\\\\mmdb\\e$\\SIN12\\model\\SIN12-MED12\\SVM\\sift-csift-layer20-label11\\"), new File("\\\\mmdb\\e$\\SIN12\\model\\Visual_Concept_List.txt"),346);
		b.writeModels(new File("\\\\mmdb\\e$\\SIN12\\model\\SIN12-MED12\\SVM\\med12-2in1-20layer-11label-concept346.models"));
		*/
		
		
		
		String rawmodeldir = "G:\\a\\e\\linear\\models";
		String conceptlistfile = "G:\\a\\e\\linear\\concept.list";
		String outputmodeldir = "G:\\a\\e\\linear\\test.models";
		
		BoostingLinearSVM b = new BoostingLinearSVM(new File(rawmodeldir), new File(conceptlistfile),6);
		b.writeModels(new File(outputmodeldir));
		//BoostingSVM b = new BoostingSVM(new File(outputmodeldir));
		
	}

	
	
	
	
	
	
	
	
	
	
}
