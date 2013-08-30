package training;

public class CascadeSVMNodeParameter extends CascadeSVMTrainParameter {
	public String modelPath;
	public String SVPath;
	public String LDPath;
	public static int parameterSize = 8;
	public static String helpText = "<kernel path> <label path> <idlist path> <subset size> <fold>" +
			"<model path> <support vector path> <LD path>\n";
	
	public CascadeSVMNodeParameter(String kernelPath, String labelPath, String idlistPath, int nSubset, int nFold,
			String modelPath, String SVPath, String LDPath) {
		super(kernelPath, labelPath, idlistPath, nSubset, nFold);
		this.modelPath = modelPath;
		this.SVPath = SVPath;
		this.LDPath = LDPath;
	}
	
	public CascadeSVMNodeParameter() {}
	
	public CascadeSVMNodeParameter(CascadeSVMNodeParameter parameter) {
		this(parameter.kernelPath, parameter.labelPath, parameter.idlistPath, parameter.nSubset, parameter.nFold,
				parameter.modelPath, parameter.SVPath, parameter.LDPath);
	}
	
	public static CascadeSVMNodeParameter parseArgs(String[] args) 
			throws CascadeSVMParameterFormatError {
		if (args.length < parameterSize)
			throw(new CascadeSVMParameterFormatError("Too few parameters."));
		else if (args.length > parameterSize)
			throw(new CascadeSVMParameterFormatError("Too many parameters."));
		CascadeSVMNodeParameter parameter = new CascadeSVMNodeParameter(
				args[0], args[1], args[2],
				Integer.parseInt(args[3]),
				Integer.parseInt(args[4]),
				args[5], args[6], args[7]);
		return parameter;
	}
	
	public static CascadeSVMNodeParameter parseArgs(String argLine) throws CascadeSVMParameterFormatError {
		String[] args = argLine.split(" ");
		return parseArgs(args);
	}
}
