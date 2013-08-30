package training;

public class CascadeSVMSchedulerParameter extends CascadeSVMTrainParameter {
	public int iterationId;
	public double lastLD;
	public String lastSVPath;
	public String subsetListPath;
	public static int parameterSize = 10;
	public static String helpText = "<iteration id> <last LD> <last support vector Path> <subset list path>" +
			"<kernel path> <label path> <idlist path> <subset size> <fold> <dimension>\n";
	
	public CascadeSVMSchedulerParameter(int iterationId, double lastLD, String lastSVPath, String subsetListPath,
			String kernelPath, String labelPath, String idlistPath, int nSubset, int nFold, int dimension) {
		super(kernelPath, labelPath, idlistPath, nSubset, nFold, dimension);
		this.iterationId = iterationId;
		this.lastLD = lastLD;
		this.lastSVPath = lastSVPath;
		this.subsetListPath = subsetListPath;
	}
	
	public CascadeSVMSchedulerParameter() {}
	
	public CascadeSVMSchedulerParameter(CascadeSVMSchedulerParameter parameter) {
		this(parameter.iterationId, parameter.lastLD, parameter.lastSVPath, parameter.subsetListPath,
				parameter.kernelPath, parameter.labelPath, parameter.idlistPath, parameter.nSubset, parameter.nFold, parameter.dimension);
	}
	
	public CascadeSVMSchedulerParameter(CascadeSVMTrainParameter parameter) {
		super(parameter);
	} 
	
	public CascadeSVMSchedulerParameter(String[] args) 
			throws CascadeSVMParameterFormatError {
		if (args.length < parameterSize)
			throw(new CascadeSVMParameterFormatError("Too few parameters."));
		else if (args.length > parameterSize)
			throw(new CascadeSVMParameterFormatError("Too many parameters."));
		
		this.iterationId = Integer.parseInt(args[0]);
		this.lastLD = Double.parseDouble(args[1]);
		this.lastSVPath = args[2];
		this.subsetListPath = args[3];
		this.kernelPath = args[4];
		this.labelPath = args[5];
		this.idlistPath = args[6];
		this.nSubset = Integer.parseInt(args[7]);
		this.nFold = Integer.parseInt(args[8]);
		this.dimension = Integer.parseInt(args[9]);
	} 
	
	public CascadeSVMSchedulerParameter(String argLine) 
			throws CascadeSVMParameterFormatError {
		this(argLine.split(" "));
	}
	
	public static CascadeSVMSchedulerParameter parseArgs(String[] args) 
			throws CascadeSVMParameterFormatError {
		if (args.length < parameterSize)
			throw(new CascadeSVMParameterFormatError("Too few parameters."));
		else if (args.length > parameterSize)
			throw(new CascadeSVMParameterFormatError("Too many parameters."));
		
		CascadeSVMSchedulerParameter parameter = new CascadeSVMSchedulerParameter(
				Integer.parseInt(args[0]),
				Double.parseDouble(args[1]),
				args[2], args[3], args[4], args[5], args[6],
				Integer.parseInt(args[7]),
				Integer.parseInt(args[8]),
				Integer.parseInt(args[9]));
		return parameter;
	} 
	
	public static CascadeSVMSchedulerParameter parseArgs(String argLine) 
			throws CascadeSVMParameterFormatError {
		String[] args = argLine.split(" ");
		return parseArgs(args);
	}
}
