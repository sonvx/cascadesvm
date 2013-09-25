package training;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;

import libsvm.svm_model;
import libsvm.svm_node;
import libsvm.svm_parameter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

public class CascadeSVMIOHelper {
	public static Logger logger = Logger.getLogger(CascadeSVMIOHelper.class);
	static final String svm_type_table[] =
	{
		"c_svc","nu_svc","one_class","epsilon_svr","nu_svr",
	};

	static final String kernel_type_table[]=
	{
		"linear","polynomial","rbf","sigmoid","precomputed"
	};
	
	/*
	 * (Hadoop) Idlist
	 * Id list is stored in sequence file format:
	 * key: line id
	 * value: feature id
	 */
	public static ArrayList<Integer> readIdListHadoop(String pathString) throws IOException {
		logger.info("[BEGIN]readIdListHadoop("+pathString+")");
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
				idlist.add(new Integer(value.get()));
				// logger.info(value.toString());
			}
		} finally {
			IOUtils.closeStream(reader);
		}
		logger.info("[END]readIdListHadoop("+pathString+")");
		return idlist;
	}
	/**
	 * rawId is stored in text format, each line is an id.
	 * @param pathString
	 * @return
	 * @throws IOException
	 */
	public static ArrayList<Integer> readRawIdListHadoop(String pathString) throws IOException {
		logger.info("[BEGIN]readRawIdListHadoop("+pathString+")");
		ArrayList<Integer> idlist = new ArrayList<Integer>();
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(pathString);
		FSDataInputStream in = fs.open(path);
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		String line;
		try {
			while ((line = reader.readLine()) != null) {
				int id = Integer.parseInt(line);
				idlist.add(new Integer(id));
				// logger.info(id);
			}
		} finally {
			reader.close();
			IOUtils.closeStream(in);
		}
		logger.info("[END]readRawIdListHadoop("+pathString+")");
		return idlist;
	}
	
	/**
	 * Write sequence file
	 * @param pathString
	 * @param idlist
	 * @throws IOException
	 */
	public static void writeIdListHadoop(String pathString, ArrayList<Integer> idlist) throws IOException {
		logger.info("[BEGIN]writeIdListHadoop("+pathString+")");
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
		logger.info("[END]writeIdListHadoop("+pathString+")");
	}
	
//	public static void writeIdListHadoop(String pathString, Iterator<Text> idlist) throws IOException {
//		logger.info("[BEGIN]writeIdListHadoop");
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
	
	/**
	 * The id of support vector should be convert into the original id when print them.
	 * The SV id is store in sequence file.
	 * @param pathString
	 * @param model
	 * @param idList
	 * @throws IOException
	 */
	public static void writeSVIdListHadoop(String pathString, svm_model model, ArrayList<Integer> idList) throws IOException {
		logger.info("[BEGIN]writeSVIdListHadoop("+pathString+")");
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
				value.set((int) model.SV[i][0].value);
				writer.append(key, value);
			}
		} finally {
			IOUtils.closeStream(writer);
		}
		logger.info("[END]writeSVIdListHadoop("+pathString+")");
	} 
	
	/**
	 * This is copied from libsvm, with only OutputStream type changed.
	 */
	public static void writeModelHadoop(String modelPath, svm_model model) throws IOException {
		logger.info("[BEGIN]writeModelHadoop("+modelPath+")");
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
		logger.info("[END]writeModelHadoop("+modelPath+")");
	}
	

	public static ArrayList<CascadeSVMSchedulerParameter> readSchedulerParameterHadoop(String pathString) throws IOException, CascadeSVMParameterFormatError {
		logger.info("[BEGIN]readSchedulerParameterHadoop("+pathString+")");
		ArrayList<CascadeSVMSchedulerParameter> schedulerParameters = new ArrayList<CascadeSVMSchedulerParameter>();
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(pathString);
		SequenceFile.Reader reader = null;
		try {
			reader = new SequenceFile.Reader(fs, path, conf);
			IntWritable key = new IntWritable();
			Text value = new Text();
			while (reader.next(key, value)) {
				schedulerParameters.add(new CascadeSVMSchedulerParameter(value.toString()));
				logger.info("key = " + key.get());
				logger.info("value = " + value.toString());
				// logger.info(value.toString());
			}
		} finally {
			IOUtils.closeStream(reader);
		}
		logger.info("[END]readSchedulerParameterHadoop("+pathString+")");
		return schedulerParameters;
	}
	/*
	 * (Hadoop) SchedulerParameter
	 * Each line is a parameter.
	 * Sequence file.
	 */
	public static void writeSchedulerParameterHadoop(String pathString, CascadeSVMSchedulerParameter parameter) throws IOException {
		logger.info("[BEGIN]writeSchedulerParameterHadoop("+pathString+")");
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
		logger.info("[END]writeSchedulerParameterHadoop("+pathString+")");
	}
	
	public static void writeSchedulerParameterHadoop(String pathString, ArrayList<CascadeSVMSchedulerParameter> parameters) throws IOException {
		logger.info("[BEGIN]writeSchedulerParameterHadoop("+pathString+")");
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
		logger.info("[END]writeSchedulerParameterHadoop("+pathString+")");
	}

//	/*
//	 * (Local) SchedulerParameter
//	 * Each line is a parameter
//	 */
//	public static CascadeSVMSchedulerParameter readSchedulerParameterLocal(String pathString) 
//			throws IOException, CascadeSVMParameterFormatError {
//		logger.info("[BEGIN]readSchedulerParameterLocal");
//		BufferedReader reader = new BufferedReader(new FileReader(pathString));
//		String argLine = reader.readLine().trim();
//		CascadeSVMSchedulerParameter parameter = new CascadeSVMSchedulerParameter(argLine);
//		reader.close();
//		logger.info("[END]readSchedulerParameterLocal");
//		return parameter;
//	}
//	
//	public static void writeSchedulerParameterLocal(String pathString, CascadeSVMSchedulerParameter parameter) 
//			throws IOException, CascadeSVMParameterFormatError {
//		logger.info("[BEGIN]writeSchedulerParameterLocal");
//		BufferedWriter writer = new BufferedWriter(new FileWriter(pathString));
//		String argLine = parameter.toString();
//		writer.write(argLine);
//		writer.close();
//		logger.info("[END]writeSchedulerParameterLocal");
//	}


	public static ArrayList<CascadeSVMNodeParameter> readNodeParameterHadoop(String pathString) throws IOException, CascadeSVMParameterFormatError {
		logger.info("[BEGIN]readNodeParameterHadoop("+pathString+")");
		ArrayList<CascadeSVMNodeParameter> nodeParameters = new ArrayList<CascadeSVMNodeParameter>();
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(pathString);
		SequenceFile.Reader reader = null;
		try {
			reader = new SequenceFile.Reader(fs, path, conf);
			IntWritable key = new IntWritable();
			Text value = new Text();
			while (reader.next(key, value)) {
				nodeParameters.add(new CascadeSVMNodeParameter(value.toString()));
				logger.info("key = " + key.get());
				logger.info("value = " + value.toString());
				// logger.info(value.toString());
			}
		} finally {
			IOUtils.closeStream(reader);
		}
		logger.info("[END]readNodeParameterHadoop("+pathString+")");
		return nodeParameters;
	}
	/*
	 * (Hadoop) NodeParameter
	 * Each line is a parameter.
	 */
	public static void writeNodeParameterHadoop(String pathString, CascadeSVMNodeParameter parameter) throws IOException {
		logger.info("[BEGIN]writeNodeParameterHadoop("+pathString+")");
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
		logger.info("[END]writeNodeParameterHadoop("+pathString+")");
	}
	
	public static void writeNodeParametersHadoop(String pathString, ArrayList<CascadeSVMNodeParameter> parameters) throws IOException {
		logger.info("[BEGIN]writeNodeParameterHadoop("+pathString+")");
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
		logger.info("[END]writeNodeParameterHadoop("+pathString+")");
	}

//	/*
//	 * (Local) NodeParameter
//	 * Each line is a parameter
//	 */
//	public static CascadeSVMNodeParameter readNodeParameterLocal(String pathString) 
//			throws IOException, CascadeSVMParameterFormatError {
//		logger.info("[BEGIN]readNodeParameterLocal");
//		BufferedReader reader = new BufferedReader(new FileReader(pathString));
//		String argLine = reader.readLine().trim();
//		CascadeSVMNodeParameter parameter = new CascadeSVMNodeParameter(argLine);
//		reader.close();
//		logger.info("[END]readNodeParameterLocal");
//		return parameter;
//	}
//	
//	public static void writeNodeParameterLocal(String pathString, CascadeSVMNodeParameter parameter) 
//			throws IOException, CascadeSVMParameterFormatError {
//		logger.info("[BEGIN]writeNodeParameterLocal");
//		BufferedWriter writer = new BufferedWriter(new FileWriter(pathString));
//		String argLine = parameter.toString();
//		writer.write(argLine);
//		writer.close();
//		logger.info("[END]writeNodeParameterLocal");
//	}
	
	/*
	 * (Hadoop) Label
	 * Label is stored in text format
	 * Each line is consist of two numbers, id and label, separated by a space.
	 */
	public static double[] readLabelHadoop(String pathString, ArrayList<Integer> idList) 
			throws IOException {
		logger.info("[BEGIN]readLabelHadoop("+pathString+")");
		double[] labels = new double[idList.size()]; 
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(pathString);
		FSDataInputStream in = fs.open(path);
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		String line;
		try {
			while ((line = reader.readLine()) != null) {
				String[] splittted_line = line.trim().split(" ");
				int id = Integer.parseInt(splittted_line[0]);
				double label = Double.parseDouble(splittted_line[1]);
				if (idList.contains(id)) {
					labels[idList.indexOf(id)] = label; 
					// logger.info(line);
				} 
			}
		}
		finally {
			reader.close();
			IOUtils.closeStream(in);
		}
		logger.info("[END]readLabelHadoop("+pathString+")");
		return labels;
	} 

//	public static void writeLabelHadoop(String pathString, ArrayList<Integer> idList, ArrayList<Double> labelList) throws IOException {
//		logger.info("[BEGIN]writeLabelHadoop("+pathString+")");
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
//		logger.info("[END]writeLabelHadoop("+pathString+")");
//	}
	
	public static double readLDHadoop(String pathString) throws IOException {
		logger.info("[BEGIN]readLDHadoop("+pathString+")");
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(pathString);
		if (!fs.exists(path))
		{
			logger.info("[END]readLDHadoop("+pathString+"), FileNotFound");
			throw(new IOException());
		}
		FSDataInputStream in = fs.open(path);
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		double LD = -1;
		try {
			String line = reader.readLine();
			LD = Double.parseDouble(line);
		} finally {
			reader.close();
			IOUtils.closeStream(in);
		}
		logger.info("[END]readLDHadoop("+pathString+"), LD = "+Double.toString(LD));
		return LD;
	}
	
	public static void writeLDHadoop(String pathString, double LD) throws IOException {
		logger.info("[BEGIN]writeLDHadoop("+pathString+")");
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(pathString);
		FSDataOutputStream out = fs.create(path);
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out));
		try {
			writer.write(Double.toString(LD));
			writer.newLine();
		} finally {
			writer.close();
			IOUtils.closeStream(out);
		}
		logger.info("[END]writeLDHadoop("+pathString+")");
	}
	
//	public static double readLDLocal(String path) throws IOException {
//		BufferedReader reader = new BufferedReader(new FileReader(path));
//		double LD = Double.parseDouble(reader.readLine().trim());
//		reader.close();
//		return LD;
//	}
//	
	
	public static void copyFileHadoop(String srcPathString, String desPathString) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path srcPath = new Path(srcPathString);
		Path desPath = new Path(desPathString);
		FileUtil.copy(fs, srcPath, fs, desPath, false, true, conf);
	}
	
	// http://stackoverflow.com/questions/17251640/how-to-see-hadoops-heap-use
	public static void printMemory() {
        logger.info("\nm:max-memory:"+(Runtime.getRuntime().maxMemory()/1024/1024)+
                    "\nm:free-memory:"+(Runtime.getRuntime().freeMemory()/1024/1024)+
                    "\nm:total:"+(Runtime.getRuntime().totalMemory()/1024/1024));
	}
	
	public static ArrayList<Integer> readIdListLocal(String path) throws IOException {
		logger.info("[BEGIN]readIdListLocal");
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
		logger.info("[BEGIN]writeIdListLocal");
		BufferedWriter writer = new BufferedWriter(new FileWriter(path));
		for (int i = 0; i < idlist.size(); i++) {
			writer.write(idlist.get(i).toString());
			writer.newLine();
		}
		writer.close();
		logger.info("[END]writeIdListLocal");
	}
	
	public static void writeSVIdListLocal(String path, svm_model model, ArrayList<Integer> idList) throws IOException {
		logger.info("[BEGIN]writeSVIdListHadoop("+path+")");
		BufferedWriter writer = new BufferedWriter(new FileWriter(path));
		for (int i = 0; i < model.l; i++) {
			writer.write(idList.get((int)model.SV[i][0].value - 1).toString());
			writer.newLine();
		}
		writer.close();
		logger.info("[END]writeSVIdListHadoop("+path+")");
	} 
	
	public static double[] readLabelLocal(String path, ArrayList<Integer> idList)
		throws IOException {
		logger.info("[BEGIN]readLabelLocal");
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
	
	public static void writeLDLocal(String path, double LD) throws IOException {
		logger.info("[BEGIN]writeLDLocal");
		BufferedWriter writer = new BufferedWriter(new FileWriter(path));
		writer.write(Double.toString(LD));
		writer.close();
		logger.info("[END]writeLDLocal");
	}
	
	public static void writeModelLocal(String model_file_name, svm_model model) throws IOException
	{
		logger.info("[BEGIN]writeModelLocal");
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
}
