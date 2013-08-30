package training;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import libsvm.svm_model;
import libsvm.svm_node;
import libsvm.svm_parameter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;


public class CascadeSVMIOHelper {
	public static Logger logger;
	static {
		logger = Logger.getLogger(CascadeSVMIOHelper.class);
	}
	
	/*
	 * (Hadoop) Idlist
	 * Id list is stored in sequence file format:
	 * key: line id
	 * value: feature id
	 */
	public static ArrayList<Integer> readIdListHadoop(String pathString) throws IOException {
		logger.info("[START]readIdListHadoop");
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
		logger.info("[END]readIdListHadoop");
		return idlist;
	}
	
	public static void writeIdListHadoop(String pathString, ArrayList<Integer> idlist) throws IOException {
		logger.info("[START]writeIdListHadoop");
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
		logger.info("[END]writeIdListHadoop");
	}
	
	public static void writeIdListHadoop(String pathString, Iterator<Text> idlist) throws IOException {
		logger.info("[START]writeIdListHadoop");
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(pathString);
		SequenceFile.Writer writer = null;
		IntWritable key = new IntWritable();
		IntWritable value = new IntWritable();
		try {
			writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass());
			int i = 0;
			while (idlist.hasNext()) {
				key.set(i);
				value.set(Integer.parseInt(idlist.next().toString()));
				writer.append(key, value);
				i++;
			}
		} finally {
			IOUtils.closeStream(writer);
		}
		logger.info("[END]writeIdListHadoop");
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
		logger.info("[START]writeModelHadoop");
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
		logger.info("[END]writeModelHadoop");
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
	public static CascadeSVMSchedulerParameter readSchedulerParameterHadoop(String pathString) 
			throws IOException, CascadeSVMParameterFormatError {
		logger.info("[START]readSchedulerParameterHadoop");
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(pathString);
		FSDataInputStream in = fs.open(path);
		String argLine;
		try {
			argLine = in.readUTF();
		} finally {
			IOUtils.closeStream(in);
		}
		CascadeSVMSchedulerParameter parameter = CascadeSVMSchedulerParameter.parseArgs(argLine);
		logger.info("[END]readSchedulerParameterHadoop");
		return parameter;
	}
	
	
	public static void writeSchedulerParameterHadoop(String pathString, CascadeSVMSchedulerParameter parameter) throws IOException {
		logger.info("[START]readSchedulerParameterHadoop");
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(pathString);
		FSDataOutputStream out = fs.create(path);
		String argLine = parameter.toString();
		try {
			out.writeUTF(argLine);
		} finally {
			IOUtils.closeStream(out);
		}
		logger.info("[END]readSchedulerParameterHadoop");
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
		CascadeSVMSchedulerParameter parameter = CascadeSVMSchedulerParameter.parseArgs(argLine);
		reader.close();
		logger.info("[END]readSchedulerParameterLocal");
		return parameter;
	}
	
	public static void writeSchedulerParameterLocal(String pathString, CascadeSVMSchedulerParameter parameter) 
			throws IOException, CascadeSVMParameterFormatError {
		BufferedWriter writer = new BufferedWriter(new FileWriter(pathString));
		String argLine = parameter.toString();
		writer.write(argLine);
		writer.close();
	}
	
	
	/*
	 * (Hadoop) NodeParameter
	 * Each line is a parameter.
	 */
	public static CascadeSVMNodeParameter readNodeParameterHadoop(String pathString) 
			throws IOException, CascadeSVMParameterFormatError {
		logger.info("[START]readNodeParameterHadoop");
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(pathString);
		FSDataInputStream in = fs.open(path);
		String argLine;
		try {
			argLine = in.readUTF();
		} finally {
			IOUtils.closeStream(in);
		}
		CascadeSVMNodeParameter parameter = CascadeSVMNodeParameter.parseArgs(argLine);
		logger.info("[END]readNodeParameterHadoop");
		return parameter;
	}
	
	
	public static void writeNodeParameterHadoop(String pathString, CascadeSVMNodeParameter parameter) throws IOException {
		logger.info("[START]readNodeParameterHadoop");
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(pathString);
		FSDataOutputStream out = fs.create(path);
		String argLine = parameter.toString();
		try {
			out.writeUTF(argLine);
		} finally {
			IOUtils.closeStream(out);
		}
		logger.info("[END]readNodeParameterHadoop");
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
		CascadeSVMNodeParameter parameter = CascadeSVMNodeParameter.parseArgs(argLine);
		reader.close();
		logger.info("[END]readNodeParameterLocal");
		return parameter;
	}
	
	
	public static void writeNodeParameterLocal(String pathString, CascadeSVMNodeParameter parameter) 
			throws IOException, CascadeSVMParameterFormatError {
		BufferedWriter writer = new BufferedWriter(new FileWriter(pathString));
		String argLine = parameter.toString();
		writer.write(argLine);
		writer.close();
	}

	/*
	 * (Hadoop) Label
	 * key: id
	 * value: label
	 */
	public static double[] readLabelHadoop(String pathString, ArrayList<Integer> idList) 
			throws IOException {
		logger.info("[START]readLabelHadoop");
		double[] labels = new double[idList.size()]; 
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(pathString);
		SequenceFile.Reader reader = null;
		try {
			reader = new SequenceFile.Reader(fs, path, conf);
			IntWritable key = new IntWritable();
			DoubleWritable label = new DoubleWritable();
			while (reader.next(key, label)) {
				if (idList.contains(key)) {
					labels[idList.indexOf(key)] = label.get(); 
				} 
			} 
		} finally {
			IOUtils.closeStream(reader);
		}
		logger.info("[END]readLabelHadoop");
		return labels;
	} 
	
	public static void writeLabelHadoop(String pathString, ArrayList<Integer> idList, ArrayList<Double> labelList) throws IOException {
		logger.info("[START]writeLabelHadoop");
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(pathString);
		SequenceFile.Writer writer = null;
		IntWritable id = new IntWritable();
		DoubleWritable label = new DoubleWritable();
		try {
			writer = new SequenceFile.Writer(fs, conf, path, IntWritable.class, DoubleWritable.class);
			for (int i = 0; i < idList.size(); i++) {
				id.set(idList.get(i));
				label.set(labelList.get(i));
			}
		} finally {
			IOUtils.closeStream(writer);
		}
		logger.info("[END]writeLabelHadoop");
	}
	
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
	 * 
	 */
	public static 
}
