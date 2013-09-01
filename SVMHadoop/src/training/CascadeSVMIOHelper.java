package training;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
//import java.util.Iterator;

import libsvm.svm_model;
import libsvm.svm_node;
import libsvm.svm_parameter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

public class CascadeSVMIOHelper {
	public static Logger logger = Logger.getLogger(CascadeSVMIOHelper.class);
	
	/*
	 * (Hadoop) Idlist
	 * Id list is stored in sequence file format:
	 * key: line id
	 * value: feature id
	 */
	public static ArrayList<Integer> readIdListHadoop(String pathString) throws IOException {
		logger.info("[START]CascadeSVMIOHelper.readIdListHadoop("+pathString+")");
		ArrayList<Integer> idlist = new ArrayList<Integer>();
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(pathString);
		SequenceFile.Reader reader = null;
		try {
			reader = new SequenceFile.Reader(fs, path, conf);
			IntWritable key = new IntWritable();
			IntWritable value = new IntWritable();
			while (reader.next(key, value)) {
				idlist.add(new Integer(value.toString()));
			}
		} finally {
			IOUtils.closeStream(reader);
		}
		logger.info("[END]CascadeSVMIOHelper.readIdListHadoop("+pathString+")");
		return idlist;
	}
	public static ArrayList<Integer> readRawIdListHadoop(String pathString) throws IOException {
		logger.info("[START]CascadeSVMIOHelper.readRawIdListHadoop("+pathString+")");
		ArrayList<Integer> idlist = new ArrayList<Integer>();
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(pathString);
		FSDataInputStream in = fs.open(path);
		int id;
		try {
			while (true) {
				try {
					id = in.readInt();
					idlist.add(new Integer(id));
				}
				catch (EOFException e) {
					break;
				}
			}
		} finally {
			IOUtils.closeStream(in);
		}
		logger.info("[END]CascadeSVMIOHelper.readRawIdListHadoop("+pathString+")");
		return idlist;
	}
	
	public static void writeIdListHadoop(String pathString, ArrayList<Integer> idlist) throws IOException {
		logger.info("[START]CascadeSVMIOHelper.writeIdListHadoop("+pathString+")");
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new Configuration());
		Path path = new Path(pathString);
		SequenceFile.Writer writer = null;
		IntWritable key = new IntWritable();
		IntWritable value = new IntWritable();
		try {
			writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass());
			for (int i = 0; i < idlist.size(); i++) {
				key.set(i);
				value.set(idlist.get(i).intValue());
				writer.append(key, value);
			}
		} finally {
			IOUtils.closeStream(writer);
		}
		logger.info("[END]CascadeSVMIOHelper.writeIdListHadoop("+pathString+")");
	}
	
//	public static void writeIdListHadoop(String pathString, Iterator<Text> idlist) throws IOException {
//		logger.info("[START]writeIdListHadoop");
//		Configuration conf = new Configuration();
//		FileSystem fs = FileSystem.get(conf);
//		Path path = new Path(pathString);
//		SequenceFile.Writer writer = null;
//		IntWritable key = new IntWritable();
//		IntWritable value = new IntWritable();
//		try {
//			writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass());
//			int i = 0;
//			while (idlist.hasNext()) {
//				key.set(i);
//				value.set(Integer.parseInt(idlist.next().toString()));
//				writer.append(key, value);
//				i++;
//			}
//		} finally {
//			IOUtils.closeStream(writer);
//		}
//		logger.info("[END]writeIdListHadoop");
//	}
	
	/*  New training instance for xi:
	 * <label> 0:i 1:K(xi,x1) ... L:K(xi,xL) 
	 * New testing instance for any x:
	 * <label> 0:? 1:K(x,x1) ... L:K(x,xL) 
	 * That is, in the training file the first column must be the "ID" of
	 * xi. In testing, ? can be any value.
	 * Shicheng: So... we can use id here?
	 * Shicheng: No, we can't. We should use sequential id start from 1
	 * So, the id of support vector should be convert into the original id when print them.
	 */
	public static void writeSVIdListHadoop(String pathString, svm_model model, ArrayList<Integer> idList) throws IOException {
		logger.info("[START]CascadeSVMIOHelper.writeSVIdListHadoop("+pathString+")");
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new Configuration());
		Path path = new Path(pathString);
		SequenceFile.Writer writer = null;
		IntWritable key = new IntWritable();
		IntWritable value = new IntWritable();
		try {
			writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass());
			for (int i = 0; i < model.l; i++) {
				key.set(i);
				value.set(idList.get((int)model.SV[i][0].value - 1));
				writer.append(key, value);
			}
		} finally {
			IOUtils.closeStream(writer);
		}
		logger.info("[END]CascadeSVMIOHelper.writeSVIdListHadoop("+pathString+")");
	} 
	
	/*
	 * (Local) Idlist
	 * One id per line.
	 */
	public static ArrayList<Integer> readIdListLocal(String path) throws IOException {
		logger.info("[START]readIdListLocal");
		ArrayList<Integer> idlist = new ArrayList<Integer>();
		BufferedReader reader = new BufferedReader(new FileReader(path));
		String line;
		while ((line = reader.readLine()) != null) {
			idlist.add(new Integer(line.trim()));
		}
		reader.close();
		logger.info("[END]readIdListLocal");
		return idlist;
	}
	
	public static void writeIdListLocal(String path, ArrayList<Integer> idlist) throws IOException {
		logger.info("[START]writeIdListLocal");
		BufferedWriter writer = new BufferedWriter(new FileWriter(path));
		for (int i = 0; i < idlist.size(); i++) {
			writer.write(idlist.get(i).toString());
			writer.newLine();
		}
		writer.close();
		logger.info("[END]writeIdListLocal");
	}
	
	/*
	 * (Hadoop) Model
	 * Single file.
	 */
	static final String svm_type_table[] =
	{
		"c_svc","nu_svc","one_class","epsilon_svr","nu_svr",
	};

	static final String kernel_type_table[]=
	{
		"linear","polynomial","rbf","sigmoid","precomputed"
	};
	
	public static void writeModelHadoop(String modelPath, svm_model model) throws IOException {
		logger.info("[START]CascadeSVMIOHelper.writeModelHadoop("+modelPath+")");
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(modelPath);
		FSDataOutputStream fp = fs.create(path);
		
		svm_parameter param = model.param;
		
		try {
			fp.writeBytes("svm_type "+svm_type_table[param.svm_type]+"\n");
			fp.writeBytes("kernel_type "+kernel_type_table[param.kernel_type]+"\n");
	
			if(param.kernel_type == svm_parameter.POLY)
				fp.writeBytes("degree "+param.degree+"\n");
	
			if(param.kernel_type == svm_parameter.POLY ||
			   param.kernel_type == svm_parameter.RBF ||
			   param.kernel_type == svm_parameter.SIGMOID)
				fp.writeBytes("gamma "+param.gamma+"\n");
	
			if(param.kernel_type == svm_parameter.POLY ||
			   param.kernel_type == svm_parameter.SIGMOID)
				fp.writeBytes("coef0 "+param.coef0+"\n");
	
			int nr_class = model.nr_class;
			int l = model.l;
			fp.writeBytes("nr_class "+nr_class+"\n");
			fp.writeBytes("total_sv "+l+"\n");
		
			{
				fp.writeBytes("rho");
				for(int i=0;i<nr_class*(nr_class-1)/2;i++)
					fp.writeBytes(" "+model.rho[i]);
				fp.writeBytes("\n");
			}
		
			if(model.label != null)
			{
				fp.writeBytes("label");
				for(int i=0;i<nr_class;i++)
					fp.writeBytes(" "+model.label[i]);
				fp.writeBytes("\n");
			}
	
			if(model.probA != null) // regression has probA only
			{
				fp.writeBytes("probA");
				for(int i=0;i<nr_class*(nr_class-1)/2;i++)
					fp.writeBytes(" "+model.probA[i]);
				fp.writeBytes("\n");
			}
			if(model.probB != null) 
			{
				fp.writeBytes("probB");
				for(int i=0;i<nr_class*(nr_class-1)/2;i++)
					fp.writeBytes(" "+model.probB[i]);
				fp.writeBytes("\n");
			}
	
			if(model.nSV != null)
			{
				fp.writeBytes("nr_sv");
				for(int i=0;i<nr_class;i++)
					fp.writeBytes(" "+model.nSV[i]);
				fp.writeBytes("\n");
			}
	
			fp.writeBytes("SV\n");
			double[][] sv_coef = model.sv_coef;
			svm_node[][] SV = model.SV;
	
			for(int i=0;i<l;i++)
			{
				for(int j=0;j<nr_class-1;j++)
					fp.writeBytes(sv_coef[j][i]+" ");
	
				svm_node[] p = SV[i];
				if(param.kernel_type == svm_parameter.PRECOMPUTED)
					fp.writeBytes("0:"+(int)(p[0].value));
				else	
					for(int j=0;j<p.length;j++)
						fp.writeBytes(p[j].index+":"+p[j].value+" ");
				fp.writeBytes("\n");
			}
		} finally {
			IOUtils.closeStream(fp);
		}
		logger.info("[END]CascadeSVMIOHelper.writeModelHadoop("+modelPath+")");
	}
	
	/* 
	 * Copied from libsvm. 
	 */
	public static void writeModelLocal(String model_file_name, svm_model model) throws IOException
	{
		logger.info("[START]writeModelLocal");
		DataOutputStream fp = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(model_file_name)));

		svm_parameter param = model.param;

		fp.writeBytes("svm_type "+svm_type_table[param.svm_type]+"\n");
		fp.writeBytes("kernel_type "+kernel_type_table[param.kernel_type]+"\n");

		if(param.kernel_type == svm_parameter.POLY)
			fp.writeBytes("degree "+param.degree+"\n");

		if(param.kernel_type == svm_parameter.POLY ||
		   param.kernel_type == svm_parameter.RBF ||
		   param.kernel_type == svm_parameter.SIGMOID)
			fp.writeBytes("gamma "+param.gamma+"\n");

		if(param.kernel_type == svm_parameter.POLY ||
		   param.kernel_type == svm_parameter.SIGMOID)
			fp.writeBytes("coef0 "+param.coef0+"\n");

		int nr_class = model.nr_class;
		int l = model.l;
		fp.writeBytes("nr_class "+nr_class+"\n");
		fp.writeBytes("total_sv "+l+"\n");
	
		{
			fp.writeBytes("rho");
			for(int i=0;i<nr_class*(nr_class-1)/2;i++)
				fp.writeBytes(" "+model.rho[i]);
			fp.writeBytes("\n");
		}
	
		if(model.label != null)
		{
			fp.writeBytes("label");
			for(int i=0;i<nr_class;i++)
				fp.writeBytes(" "+model.label[i]);
			fp.writeBytes("\n");
		}

		if(model.probA != null) // regression has probA only
		{
			fp.writeBytes("probA");
			for(int i=0;i<nr_class*(nr_class-1)/2;i++)
				fp.writeBytes(" "+model.probA[i]);
			fp.writeBytes("\n");
		}
		if(model.probB != null) 
		{
			fp.writeBytes("probB");
			for(int i=0;i<nr_class*(nr_class-1)/2;i++)
				fp.writeBytes(" "+model.probB[i]);
			fp.writeBytes("\n");
		}

		if(model.nSV != null)
		{
			fp.writeBytes("nr_sv");
			for(int i=0;i<nr_class;i++)
				fp.writeBytes(" "+model.nSV[i]);
			fp.writeBytes("\n");
		}

		fp.writeBytes("SV\n");
		double[][] sv_coef = model.sv_coef;
		svm_node[][] SV = model.SV;

		for(int i=0;i<l;i++)
		{
			for(int j=0;j<nr_class-1;j++)
				fp.writeBytes(sv_coef[j][i]+" ");

			svm_node[] p = SV[i];
			if(param.kernel_type == svm_parameter.PRECOMPUTED)
				fp.writeBytes("0:"+(int)(p[0].value));
			else	
				for(int j=0;j<p.length;j++)
					fp.writeBytes(p[j].index+":"+p[j].value+" ");
			fp.writeBytes("\n");
		}

		fp.close();
		logger.info("[END]writeModelHadoop");
	}
	
	
	/*
	 * (Hadoop) SchedulerParameter
	 * Each line is a parameter.
	 */
	public static void writeSchedulerParameterHadoop(String pathString, CascadeSVMSchedulerParameter parameter) throws IOException {
		logger.info("[START]CascadeSVMIOHelper.writeSchedulerParameterHadoop("+pathString+")");
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(pathString);
		SequenceFile.Writer writer = null;
		IntWritable key = new IntWritable();
		Text value = new Text();
		try {
			writer = new Writer(fs, conf, path, key.getClass(), value.getClass());
			key.set(0);
			value.set(parameter.toString());
			writer.append(key, value);
		} finally {
			IOUtils.closeStream(writer);
		}
		logger.info("[END]CascadeSVMIOHelper.writeSchedulerParameterHadoop("+pathString+")");
	}
	
	public static void writeSchedulerParameterHadoop(String pathString, ArrayList<CascadeSVMSchedulerParameter> parameters) throws IOException {
		logger.info("[START]CascadeSVMIOHelper.writeSchedulerParameterHadoop("+pathString+")");
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(pathString);
		SequenceFile.Writer writer = null;
		IntWritable key = new IntWritable();
		Text value = new Text();
		try {
			writer = new Writer(fs, conf, path, key.getClass(), value.getClass());
			for (int i = 0; i < parameters.size(); i++) {
				key.set(i);
				value.set(parameters.get(i).toString());
				writer.append(key, value);
			}
		} finally {
			IOUtils.closeStream(writer);
		}
		logger.info("[END]CascadeSVMIOHelper.writeSchedulerParameterHadoop("+pathString+")");
	}

	/*
	 * (Local) SchedulerParameter
	 * Each line is a parameter
	 */
	public static CascadeSVMSchedulerParameter readSchedulerParameterLocal(String pathString) 
			throws IOException, CascadeSVMParameterFormatError {
		logger.info("[START]readSchedulerParameterLocal");
		BufferedReader reader = new BufferedReader(new FileReader(pathString));
		String argLine = reader.readLine().trim();
		CascadeSVMSchedulerParameter parameter = new CascadeSVMSchedulerParameter(argLine);
		reader.close();
		logger.info("[END]readSchedulerParameterLocal");
		return parameter;
	}
	
	public static void writeSchedulerParameterLocal(String pathString, CascadeSVMSchedulerParameter parameter) 
			throws IOException, CascadeSVMParameterFormatError {
		logger.info("[START]writeSchedulerParameterLocal");
		BufferedWriter writer = new BufferedWriter(new FileWriter(pathString));
		String argLine = parameter.toString();
		writer.write(argLine);
		writer.close();
		logger.info("[END]writeSchedulerParameterLocal");
	}


	/*
	 * (Hadoop) NodeParameter
	 * Each line is a parameter.
	 */
	public static void writeNodeParameterHadoop(String pathString, CascadeSVMNodeParameter parameter) throws IOException {
		logger.info("[START]CascadeSVMIOHelper.writeNodeParameterHadoop("+pathString+")");
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(pathString);
		SequenceFile.Writer writer = null;
		IntWritable key = new IntWritable();
		Text value = new Text();
		try {
			writer = new Writer(fs, conf, path, key.getClass(), value.getClass());
			key.set(0);
			value.set(parameter.toString());
			writer.append(key, value);
		} finally {
			IOUtils.closeStream(writer);
		}
		logger.info("[END]CascadeSVMIOHelper.writeNodeParameterHadoop("+pathString+")");
	}
	
	public static void writeNodeParameterHadoop(String pathString, ArrayList<CascadeSVMNodeParameter> parameters) throws IOException {
		logger.info("[START]CascadeSVMIOHelper.writeNodeParameterHadoop("+pathString+")");
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(pathString);
		SequenceFile.Writer writer = null;
		IntWritable key = new IntWritable();
		Text value = new Text();
		try {
			writer = new Writer(fs, conf, path, key.getClass(), value.getClass());
			for (int i = 0; i < parameters.size(); i++) {
				key.set(i);
				value.set(parameters.get(i).toString());
				writer.append(key, value);
			}
		} finally {
			IOUtils.closeStream(writer);
		}
		logger.info("[END]CascadeSVMIOHelper.writeNodeParameterHadoop("+pathString+")");
	}

	/*
	 * (Local) NodeParameter
	 * Each line is a parameter
	 */
	public static CascadeSVMNodeParameter readNodeParameterLocal(String pathString) 
			throws IOException, CascadeSVMParameterFormatError {
		logger.info("[START]readNodeParameterLocal");
		BufferedReader reader = new BufferedReader(new FileReader(pathString));
		String argLine = reader.readLine().trim();
		CascadeSVMNodeParameter parameter = new CascadeSVMNodeParameter(argLine);
		reader.close();
		logger.info("[END]readNodeParameterLocal");
		return parameter;
	}
	
	public static void writeNodeParameterLocal(String pathString, CascadeSVMNodeParameter parameter) 
			throws IOException, CascadeSVMParameterFormatError {
		logger.info("[START]writeNodeParameterLocal");
		BufferedWriter writer = new BufferedWriter(new FileWriter(pathString));
		String argLine = parameter.toString();
		writer.write(argLine);
		writer.close();
		logger.info("[END]writeNodeParameterLocal");
	}
	
	/*
	 * (Hadoop) Label
	 * key: id
	 * value: label
	 */
	public static double[] readLabelHadoop(String pathString, ArrayList<Integer> idList) 
			throws IOException {
		logger.info("[START]CascadeSVMIOHelper.readLabelHadoop("+pathString+")");
		double[] labels = new double[idList.size()]; 
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(pathString);
		FSDataInputStream in = fs.open(path);
		try {
			while (true) {
				try {
					int id = in.readInt();
					double label = in.readDouble();
					if (idList.contains(id)) {
						labels[idList.indexOf(id)] = label; 
					} 
				}
				catch (EOFException e) {
					break;
				} 
			}
		}
		finally {
			IOUtils.closeStream(in);
		}
		logger.info("[END]CascadeSVMIOHelper.readLabelHadoop("+pathString+")");
		return labels;
	} 

//	public static void writeLabelHadoop(String pathString, ArrayList<Integer> idList, ArrayList<Double> labelList) throws IOException {
//		logger.info("[START]CascadeSVMIOHelper.writeLabelHadoop("+pathString+")");
//		Configuration conf = new Configuration();
//		FileSystem fs = FileSystem.get(conf);
//		Path path = new Path(pathString);
//		SequenceFile.Writer writer = null;
//		IntWritable id = new IntWritable();
//		DoubleWritable label = new DoubleWritable();
//		try {
//			writer = new SequenceFile.Writer(fs, conf, path, IntWritable.class, DoubleWritable.class);
//			for (int i = 0; i < idList.size(); i++) {
//				id.set(idList.get(i));
//				label.set(labelList.get(i));
//			}
//		} finally {
//			IOUtils.closeStream(writer);
//		}
//		logger.info("[END]CascadeSVMIOHelper.writeLabelHadoop("+pathString+")");
//	}
	
	/*
	 * (Local) Label
	 */
	public static double[] readLabelLocal(String path, ArrayList<Integer> idList)
		throws IOException {
		logger.info("[START]readLabelLocal");
		BufferedReader labelFile = new BufferedReader(new FileReader(path));
		double[] labels = new double[idList.size()];
		String line;
		while ((line = labelFile.readLine()) != null) {
			String[] valueArray = line.trim().split(" ");
			int id = Integer.parseInt(valueArray[0]);
			double label = Double.parseDouble(valueArray[1]);
			if (idList.contains(id)) {
				labels[idList.indexOf(id)] = label;
			}
		}
		labelFile.close();
		logger.info("[END]readLabelLocal");
		return labels;
	}
	
	/*
	 * (Hadoop) subset list
	 */
	public static ArrayList<String> readSubsetListHadoop(String pathString) throws IOException {
		logger.info("[START]CascadeSVMIOHelper.readSubsetListHadoop("+pathString+")");
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(pathString);
		SequenceFile.Reader reader = null;
		ArrayList<String> subsetList = new ArrayList<String>();
		IntWritable key = new IntWritable();
		Text value = new Text();
		try {
			reader = new SequenceFile.Reader(fs, path, conf);
			while (reader.next(key, value)) {
				subsetList.add(value.toString());
			}
		} finally {
			IOUtils.closeStream(reader);
		}
		logger.info("[END]CascadeSVMIOHelper.readSubsetListHadoop("+pathString+")");
		return subsetList;
	}
	
	public static void writeSubsetListHadoop(String pathString, ArrayList<String> subsetList) throws IOException {
		logger.info("[START]CascadeSVMIOHelper.writeSubsetListHadoop("+pathString+")");
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(pathString);
		SequenceFile.Writer writer = null;
		IntWritable key = new IntWritable();
		Text value = new Text();
		try {
			writer = new SequenceFile.Writer(fs, conf, path, key.getClass(), value.getClass());
			for (int i = 0; i < subsetList.size(); i++) {
				key.set(i);
				value.set(subsetList.get(i));
				writer.append(key, value);
			}
		} finally {
			IOUtils.closeStream(writer);
		}
		logger.info("[END]CascadeSVMIOHelper.writeSubsetListHadoop("+pathString+")");
	} 
	
	public static double readLDHadoop(String pathString) throws IOException {
		logger.info("[START]CascadeSVMIOHelper.readLDHadoop("+pathString+")");
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(pathString);
//		if (!fs.exists(path))
//			return -1;
		FSDataInputStream in = fs.open(path);
		double LD = -1;
		try {
			LD = in.readDouble();
		} finally {
			IOUtils.closeStream(in);
		}
		logger.info("[END]CascadeSVMIOHelper.readLDHadoop("+pathString+")");
		return LD;
	}
	
	public static void writeLDHadoop(String pathString, double LD) throws IOException {
		logger.info("[START]CascadeSVMIOHelper.writeLDHadoop("+pathString+")");
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(pathString);
		FSDataOutputStream out = fs.create(path);
		try {
			out.writeDouble(LD);
		} finally {
			IOUtils.closeStream(out);
		}
		logger.info("[END]CascadeSVMIOHelper.writeLDHadoop("+pathString+")");
	}
	
	public static double readLDLocal(String path) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(path));
		double LD = Double.parseDouble(reader.readLine().trim());
		reader.close();
		return LD;
	}
	
	public static void writeLDLocal(String path, double LD) throws IOException {
		BufferedWriter writer = new BufferedWriter(new FileWriter(path));
		writer.write(Double.toString(LD));
		writer.close();
	}
}
