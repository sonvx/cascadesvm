package training;

public class CascadeSVMNodeParameter extends CascadeSVMTrainParameter {
	public String modelPath;
	public String SVPath;
	public String LDPath;
	public static int parameterSize = 9;
	public static String helpText = "<model path> <support vector path> <LD path>" +
			"<kernel path> <label path> <idlist path> <subset size> <fold> <dimension>\n";
	
	public CascadeSVMNodeParameter(
			String modelPath, 
			String SVPath, 
			String LDPath,
			String kernelPath, 
			String labelPath, 
			String idlistPath, 
			int nSubset, 
			int nFold,
			int dimension) {
		super(kernelPath, labelPath, idlistPath, nSubset, nFold, dimension);
		this.modelPath = modelPath;
		this.SVPath = SVPath;
		this.LDPath = LDPath;
	}
	
	public CascadeSVMNodeParameter() {}
	
	public CascadeSVMNodeParameter(CascadeSVMNodeParameter parameter) {
		this(
				parameter.modelPath, 
				parameter.SVPath, 
				parameter.LDPath,
				parameter.kernelPath, 
				parameter.labelPath, 
				parameter.idlistPath, 
				parameter.nSubset, 
				parameter.nFold,
				parameter.dimension
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
		this.nSubset    = Integer.parseInt(args[6]);
		this.nFold 		= Integer.parseInt(args[7]);
		this.dimension  = Integer.parseInt(args[8]);
	}
	
	public CascadeSVMNodeParameter(String argLine) 
			throws CascadeSVMParameterFormatError {
		this(argLine.split(" "));
	}
	
	public static CascadeSVMNodeParameter parseArgs(String[] args) 
			throws CascadeSVMParameterFormatError {
		if (args.length < parameterSize)
			throw(new CascadeSVMParameterFormatError("Too few parameters."));
		else if (args.length > parameterSize)
			throw(new CascadeSVMParameterFormatError("Too many parameters."));
		CascadeSVMNodeParameter parameter = new CascadeSVMNodeParameter(
				args[0], args[1], args[2],
				args[3], args[4], args[5], 
				Integer.parseInt(args[6]), 
				Integer.parseInt(args[7]),
				Integer.parseInt(args[8]));
		return parameter;
	}
	
	public static CascadeSVMNodeParameter parseArgs(String argLine) throws CascadeSVMParameterFormatError {
		String[] args = argLine.split(" ");
		return parseArgs(args);
	}
}
