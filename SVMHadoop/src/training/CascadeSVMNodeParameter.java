package training;

public class CascadeSVMNodeParameter extends CascadeSVMTrainParameter {
	public String modelPath;
	public String SVPath;
	public String LDPath;
	public static int parameterSize = 12;
	public static String helpText = "<model path> <support vector path> <LD path>" +
			"<kernel path> <label path> <idlist path> <data size> <output directory>" +
			"<subset size> <fold> <epsilon> <max iteration> \n";
	
	public CascadeSVMNodeParameter(
			String modelPath, 
			String SVPath, 
			String LDPath,
			String kernelPath, 
			String labelPath, 
			String idlistPath,
			int nData,
			String workDir,
			int nSubset, 
			int nFold,
			double epsilon,
			int max_iter) {
		super(kernelPath, labelPath, idlistPath, nData, workDir, nSubset, nFold, epsilon, max_iter);
		this.modelPath = modelPath;
		this.SVPath = SVPath;
		this.LDPath = LDPath;
	}
	
	public CascadeSVMNodeParameter(CascadeSVMNodeParameter parameter) {
		this(
				parameter.modelPath, 
				parameter.SVPath, 
				parameter.LDPath,
				parameter.kernelPath, 
				parameter.labelPath, 
				parameter.idlistPath,
				parameter.nData,
				parameter.workDir,
				parameter.nSubset, 
				parameter.nFold,
				parameter.epsilon,
				parameter.max_iter
		);
	}
	
	public CascadeSVMNodeParameter(CascadeSVMTrainParameter parameter) {
		super(parameter);
	} 
	
	public CascadeSVMNodeParameter(String[] args) 
			throws CascadeSVMParameterFormatError {
		if (args.length < parameterSize)
			throw(new CascadeSVMParameterFormatError("Too few parameters."));
		else if (args.length > parameterSize)
			throw(new CascadeSVMParameterFormatError("Too many parameters."));
		this.modelPath  = args[0]; 
		this.SVPath     = args[1];
		this.LDPath     = args[2];
		this.kernelPath = args[3]; 
		this.labelPath  = args[4]; 
		this.idlistPath = args[5];
		this.nData      = Integer.parseInt(args[6]);
		this.workDir  = args[7];
		this.nSubset    = Integer.parseInt(args[8]); 
		this.nFold      = Integer.parseInt(args[9]);
		this.epsilon    = Double.parseDouble(args[10]);
		this.max_iter   = Integer.parseInt(args[11]);
	}
	
	public CascadeSVMNodeParameter(String argLine) 
			throws CascadeSVMParameterFormatError {
		this(argLine.split(" "));
	}
	
	public String toString() {
		return  modelPath + " " + SVPath + " " + LDPath + " " + 
				kernelPath + " " + labelPath + " " + idlistPath + " " + Integer.toString(nData) + " " +
				workDir + " " + Integer.toString(nSubset) + " " + Integer.toString(nFold) + " " +
				Double.toString(epsilon) + " " + Integer.toString(max_iter);
	} 
}
