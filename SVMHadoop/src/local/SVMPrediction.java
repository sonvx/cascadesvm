package local;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.StringTokenizer;




import beans.svm_model;
import beans.svm_node;
import beans.svm_parameter;



public class SVMPrediction {
	static final String svm_type_table[] = {"c_svc","nu_svc","one_class","epsilon_svr","nu_svr",};
	static final String kernel_type_table[]= {"linear","polynomial","rbf","sigmoid","precomputed"};
	
	
	
	public static double svm_predict_probability(svm_model model, svm_node[] x, double[] prob_estimates)
	{
		if ((model.param.svm_type == svm_parameter.C_SVC || model.param.svm_type == svm_parameter.NU_SVC) &&
		    model.probA!=null && model.probB!=null)
		{
			int i;
			int nr_class = model.nr_class;
			double[] dec_values = new double[nr_class*(nr_class-1)/2];
			svm_predict_values(model, x, dec_values);

			double min_prob=1e-7;
			double[][] pairwise_prob=new double[nr_class][nr_class];
			
			int k=0;
			for(i=0;i<nr_class;i++)
				for(int j=i+1;j<nr_class;j++)
				{
					pairwise_prob[i][j]=Math.min(Math.max(sigmoid_predict(dec_values[k],model.probA[k],model.probB[k]),min_prob),1-min_prob);
					pairwise_prob[j][i]=1-pairwise_prob[i][j];
					k++;
				}
			multiclass_probability(nr_class,pairwise_prob,prob_estimates);

			int prob_max_idx = 0;
			for(i=1;i<nr_class;i++)
				if(prob_estimates[i] > prob_estimates[prob_max_idx])
					prob_max_idx = i;
			return model.label[prob_max_idx];
		}
		return 0;
	}
	
	private static void multiclass_probability(int k, double[][] r, double[] p)
	{
		int t,j;
		int iter = 0, max_iter=Math.max(100,k);
		double[][] Q=new double[k][k];
		double[] Qp=new double[k];
		double pQp, eps=0.005/k;
	
		for (t=0;t<k;t++)
		{
			p[t]=1.0/k;  // Valid if k = 1
			Q[t][t]=0;
			for (j=0;j<t;j++)
			{
				Q[t][t]+=r[j][t]*r[j][t];
				Q[t][j]=Q[j][t];
			}
			for (j=t+1;j<k;j++)
			{
				Q[t][t]+=r[j][t]*r[j][t];
				Q[t][j]=-r[j][t]*r[t][j];
			}
		}
		for (iter=0;iter<max_iter;iter++)
		{
			// stopping condition, recalculate QP,pQP for numerical accuracy
			pQp=0;
			for (t=0;t<k;t++)
			{
				Qp[t]=0;
				for (j=0;j<k;j++)
					Qp[t]+=Q[t][j]*p[j];
				pQp+=p[t]*Qp[t];
			}
			double max_error=0;
			for (t=0;t<k;t++)
			{
				double error=Math.abs(Qp[t]-pQp);
				if (error>max_error)
					max_error=error;
			}
			if (max_error<eps) break;
		
			for (t=0;t<k;t++)
			{
				double diff=(-Qp[t]+pQp)/Q[t][t];
				p[t]+=diff;
				pQp=(pQp+diff*(diff*Q[t][t]+2*Qp[t]))/(1+diff)/(1+diff);
				for (j=0;j<k;j++)
				{
					Qp[j]=(Qp[j]+diff*Q[t][j])/(1+diff);
					p[j]/=(1+diff);
				}
			}
		}
		if (iter>=max_iter);
	}
	
	public static double svm_get_svr_probability(svm_model model)
	{
		if ((model.param.svm_type == svm_parameter.EPSILON_SVR || model.param.svm_type == svm_parameter.NU_SVR) &&
		    model.probA!=null)
		return model.probA[0];
		else
		{
			System.err.print("Model doesn't contain information for SVR probability inference\n");
			return 0;
		}
	}
	

	public static void predict(ArrayList<svm_node[]> x, DataOutputStream output, svm_model model) throws IOException
	{
		int nr_class=model.nr_class;
		int[] labels=new int[nr_class];
		svm_get_labels(model,labels);
		output.writeBytes("labels");
		for(int j=0;j<nr_class;j++)
			output.writeBytes(" "+labels[j]);
		output.writeBytes("\n");
		
		for(int i = 0 ; i < x.size() ; i++)
			predict(x.get(i), output, model);	
	}
	
	
	public static double[] predictHadoop(ArrayList<svm_node[]> x, svm_model model) throws IOException
	{
		double[] predictions = new double[x.size()];
		int nr_class=model.nr_class;
		int[] labels=new int[nr_class];
	
		svm_get_labels(model,labels);
		
		for(int i = 0 ; i < x.size() ; i++) {
			double[] prob_estimates=new double[nr_class];
			svm_predict_probability(model,x.get(i),prob_estimates);
			for(int j = 0 ; j < nr_class ; j++) {
				if(labels[j] == 1) {
					predictions[i] = prob_estimates[j];
				}
			}
		}
		
		return predictions;
	}
	
	public static double predictHadoop(svm_node[] x, svm_model model) throws IOException
	{
		int nr_class=model.nr_class;
		int[] labels=new int[nr_class];
		double prediction = 0.0;
		svm_get_labels(model,labels);
		
		double[] prob_estimates=new double[nr_class];
		svm_predict_probability(model,x,prob_estimates);
		for(int j = 0 ; j < nr_class ; j++) {
			if(labels[j] == 1) {
				prediction = prob_estimates[j];
				break;
			}
		}
		
		return prediction;
	}
	
	
	public static void predict(svm_node[] x, DataOutputStream output, svm_model model) throws IOException
	{
	
		int nr_class=model.nr_class;
		double[] prob_estimates=new double[nr_class];
		double v;
		v = svm_predict_probability(model,x,prob_estimates);
		output.writeBytes(v+" ");
		for(int j=0;j<nr_class;j++)
			output.writeBytes(prob_estimates[j]+" ");
		output.writeBytes("\n");

			
	}
	
	public static double svm_predict_values(svm_model model, svm_node[] x, double[] dec_values)
	{
		int i;
		int nr_class = model.nr_class;
		int l = model.l;

		double[] kvalue = new double[l];
		for (i = 0; i < l; i++)
			kvalue[i] = x[(int)(model.SV[i][0].value)].value;

		int[] start = new int[nr_class];
		start[0] = 0;
		for (i = 1; i < nr_class; i++)
			start[i] = start[i - 1] + model.nSV[i - 1];

		int[] vote = new int[nr_class];
		for (i = 0; i < nr_class; i++)
			vote[i] = 0;

		int p = 0;
		for (i = 0; i < nr_class; i++)
			for (int j = i + 1; j < nr_class; j++) {
				double sum = 0;
				int si = start[i];
				int sj = start[j];
				int ci = model.nSV[i];
				int cj = model.nSV[j];

				int k;
				double[] coef1 = model.sv_coef[j - 1];
				double[] coef2 = model.sv_coef[i];
				for (k = 0; k < ci; k++)
					sum += coef1[si + k] * kvalue[si + k];
				for (k = 0; k < cj; k++)
					sum += coef2[sj + k] * kvalue[sj + k];
				sum -= model.rho[p];
				dec_values[p] = sum;

				if (dec_values[p] > 0)
					++vote[i];
				else
					++vote[j];
				p++;
			}

		int vote_max_idx = 0;
		for (i = 1; i < nr_class; i++)
			if (vote[i] > vote[vote_max_idx])
				vote_max_idx = i;

		return model.label[vote_max_idx];
	}
	
	
	private static double atof(String s)
	{
		return Double.valueOf(s).doubleValue();
	}

	
	public static void svm_get_labels(svm_model model, int[] label)
	{
		if (model.label != null)
			for(int i=0;i<model.nr_class;i++)
				label[i] = model.label[i];
	}
	
	
		
	private static double sigmoid_predict(double decision_value, double A, double B)
	{
		double fApB = decision_value*A+B;
		if (fApB >= 0)
			return Math.exp(-fApB)/(1.0+Math.exp(-fApB));
		else
			return 1.0/(1+Math.exp(fApB)) ;
	}

	
	
	
	
	

	public static svm_model svm_load_model(File in) throws IOException
	{
		// read parameters
		BufferedReader fp = new BufferedReader(new FileReader(in));
		svm_model model = new svm_model();
		svm_parameter param = new svm_parameter();
		model.param = param;
		model.rho = null;
		model.probA = null;
		model.probB = null;
		model.label = null;
		model.nSV = null;

		while(true)
		{
			String cmd = fp.readLine();
			String arg = cmd.substring(cmd.indexOf(' ')+1);

			if(cmd.startsWith("svm_type"))
			{
				int i;
				for(i=0;i<svm_type_table.length;i++)
				{
					if(arg.indexOf(svm_type_table[i])!=-1)
					{
						param.svm_type=i;
						break;
					}
				}
				if(i == svm_type_table.length)
				{
					System.err.print("unknown svm type.\n");
					return null;
				}
			}
			else if(cmd.startsWith("kernel_type"))
			{
				int i;
				for(i=0;i<kernel_type_table.length;i++)
				{
					if(arg.indexOf(kernel_type_table[i])!=-1)
					{
						param.kernel_type=i;
						break;
					}
				}
				if(i == kernel_type_table.length)
				{
					System.err.print("unknown kernel function.\n");
					return null;
				}
			}
			else if(cmd.startsWith("degree"))
				param.degree = atoi(arg);
			else if(cmd.startsWith("gamma"))
				param.gamma = atof(arg);
			else if(cmd.startsWith("coef0"))
				param.coef0 = atof(arg);
			else if(cmd.startsWith("nr_class"))
				model.nr_class = atoi(arg);
			else if(cmd.startsWith("total_sv"))
				model.l = atoi(arg);
			else if(cmd.startsWith("rho"))
			{
				int n = model.nr_class * (model.nr_class-1)/2;
				model.rho = new double[n];
				StringTokenizer st = new StringTokenizer(arg);
				for(int i=0;i<n;i++)
					model.rho[i] = atof(st.nextToken());
			}
			else if(cmd.startsWith("label"))
			{
				int n = model.nr_class;
				model.label = new int[n];
				StringTokenizer st = new StringTokenizer(arg);
				for(int i=0;i<n;i++)
					model.label[i] = atoi(st.nextToken());					
			}
			else if(cmd.startsWith("probA"))
			{
				int n = model.nr_class*(model.nr_class-1)/2;
				model.probA = new double[n];
				StringTokenizer st = new StringTokenizer(arg);
				for(int i=0;i<n;i++)
					model.probA[i] = atof(st.nextToken());					
			} 
			else if(cmd.startsWith("threshold_recall"))
			{
				model.threshold_recall = atof(arg);	
			} 
			else if(cmd.startsWith("threshold_precision"))
			{
				//do nothing		
			}
			else if(cmd.startsWith("probB"))
			{
				int n = model.nr_class*(model.nr_class-1)/2;
				model.probB = new double[n];
				StringTokenizer st = new StringTokenizer(arg);
				for(int i=0;i<n;i++)
					model.probB[i] = atof(st.nextToken());					
			}
			else if(cmd.startsWith("nr_sv"))
			{
				int n = model.nr_class;
				model.nSV = new int[n];
				StringTokenizer st = new StringTokenizer(arg);
				for(int i=0;i<n;i++)
					model.nSV[i] = atoi(st.nextToken());
			}
			else if(cmd.startsWith("SV"))
			{
				break;
			}
			else
			{
				System.err.print("unknown text in model file: ["+cmd+"]\n");
				return null;
			}
		}

		// read sv_coef and SV

		int m = model.nr_class - 1;
		int l = model.l;
		model.sv_coef = new double[m][l];
		model.SV = new svm_node[l][];

		for(int i=0;i<l;i++)
		{
			String line = fp.readLine();
			StringTokenizer st = new StringTokenizer(line," \t\n\r\f:");

			for(int k=0;k<m;k++)
				model.sv_coef[k][i] = atof(st.nextToken());
			int n = st.countTokens()/2;
			model.SV[i] = new svm_node[n];
			for(int j=0;j<n;j++)
			{
				model.SV[i][j] = new svm_node();
				model.SV[i][j].index = Integer.parseInt(st.nextToken());
				model.SV[i][j].value = Double.parseDouble(st.nextToken());
			}
		}

		fp.close();
		return model;
	}
	
	private static int atoi(String s)
	{
		return Integer.parseInt(s);
	}
	
	
	
	
	
	
	/**
	 * Read LibSVM kernel file from the a text file.
	 * @param in The file following SVM standard format
	 * @return
	 * @throws IOException
	 */
	public static ArrayList<svm_node[]> load_svm_node(File in) throws IOException {
		
		ArrayList<svm_node[]> result = new ArrayList<svm_node[]>();
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(in)),1024*8*16);
		String line = br.readLine();
		while(line != null) {
			String[] temps = line.split("[ :]");
			svm_node[] this_node = new svm_node[temps.length/2];
			
			
			//skip the second position
			for(int i = 1 ; i < (this_node.length+1) ; i++) {
				this_node[i-1] = new svm_node();
				this_node[i-1].index = Integer.parseInt(temps[2*i-1]);
				this_node[i-1].value = Double.parseDouble(temps[2*i]);
				
			}
			line = br.readLine();
			
			result.add(this_node);
		}
		
		br.close();
		return result;
	}
	
	
	
	/**
	 * Read LibSVM kernel file from the a matrix
	 * @param a matrix file
	 * @return
	 * @throws IOException
	 */
	public static ArrayList<svm_node[]> load_svm_node_matrix(File in) throws IOException {
		
		ArrayList<svm_node[]> result = new ArrayList<svm_node[]>();
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(in)),1024*8*16);
		String line = br.readLine();
		int linecnt = 1;
		while(line != null) {
			String[] temps = line.split(" ");
			svm_node[] this_node = new svm_node[temps.length+1];
			
			this_node[0] = new svm_node();
			this_node[0].index = 0;
			this_node[0].value = linecnt;
			//skip the second position
			for(int i = 1 ; i < this_node.length ; i++) {
				this_node[i] = new svm_node();
				this_node[i].index = i;
				this_node[i].value = Double.parseDouble(temps[i-1]);
			}
			
			line = br.readLine();
			linecnt++;
			result.add(this_node);
		}
		
		br.close();
		return result;
	}
	
	/**
	 * It has Bug
	 * @param binary_in
	 * @param col
	 * @return
	 * @throws IOException
	 */
	public static ArrayList<svm_node[]> load_svm_node_binary(File binary_in, int col) throws IOException {
		
		ArrayList<svm_node[]> result = new ArrayList<svm_node[]>();
		
		DataInputStream br = new DataInputStream(new BufferedInputStream(new FileInputStream(binary_in),1024*8*16));
		int row = 0;
		
		while(br.available() >= 4) {
			
			svm_node[] thisrow = new svm_node[col+1];
			thisrow[0] = new svm_node();
			thisrow[0].index = 0;
			thisrow[0].value = 1;	//bug
			
			for(int j = 1 ; j < (col+1) ; j++) {
				
				thisrow[j] = new svm_node();
				thisrow[j].index = j;
				thisrow[j].value = br.readFloat();
			}
			if(row == 0 )	result.add(thisrow);
			System.out.println(++row);
		}
		
		br.close();
		return result;
	}
	
	
	public static void main(String args[]) throws Exception {
		String dir = "G:\\smalltest\\value\\test-local-prediction\\";
		svm_model m = svm_load_model(new File(dir + "4.bestmodel"));
		
		ArrayList<svm_node[]> nodes = load_svm_node(new File(dir + "kernelmatrix(prediction-in)"));	//236697
		for(int i = 0 ; i < nodes.size() ; i++) {
			for(int j = 1 ; j < nodes.get(i).length ; j++) {
				nodes.get(i)[j].value = Math.exp(nodes.get(i)[j].value * m.param.gamma);
			}
		}
		
		
		DataOutputStream out = new DataOutputStream(new FileOutputStream(new File(dir+"4.outpredict")));
		
		
		predict(nodes, out, m);
		//p.loadmodels(new File("G:\\sin-models\\concept_346"), new File("G:\\sin-models\\Visual_Concept_List.txt"));
		
		
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	

}
