package training;

public class CascadeSVMTrainParameter {
	public String kernelPath;
	public String labelPath;
	public String idlistPath;
	public int nSubset;
	public int nFold;
	public static String helpText = 
			"Usage: CascadeSVMTrain " +
			"-k <kernel path> -l <label path> -i <idlist path> -ns <subset size> -nf <fold>\n";
	
	public CascadeSVMTrainParameter(String kernelPath, String labelPath, String idlistPath, int nSubset, int nFold) {
		this.kernelPath = kernelPath;
		this.labelPath  = labelPath;
		this.idlistPath = idlistPath;
		this.nSubset = nSubset;
		this.nFold = nFold;
	}
	
	public CascadeSVMTrainParameter() {}
	
	public CascadeSVMTrainParameter(CascadeSVMTrainParameter parameter) {
		parameter.kernelPath = parameter.kernelPath;
		parameter.labelPath = parameter.labelPath;
		parameter.idlistPath = parameter.idlistPath;
		parameter.nSubset = parameter.nSubset;
		parameter.nFold = parameter.nFold;
	}
	
	public void checkParameterFormat() 
			throws CascadeSVMParameterFormatError {
		if (kernelPath == "")
			throw(new CascadeSVMParameterFormatError("Kernel path is not specified."));
		if (labelPath == "")
			throw(new CascadeSVMParameterFormatError("Label path is not specified."));
		if (idlistPath == "")
			throw(new CascadeSVMParameterFormatError("Idlist path is not specified."));
		if (nSubset == 0)
			throw(new CascadeSVMParameterFormatError("The size of subset is not specified."));
		if (nFold == 0)
			throw(new CascadeSVMParameterFormatError("The size of fold is not specified."));
	}

	public static CascadeSVMTrainParameter parseArgs(String[] args) 
			throws CascadeSVMParameterFormatError {
		CascadeSVMTrainParameter parameter = new CascadeSVMTrainParameter();
		int i = 0;
		while (i < args.length) {
			if (args[i].charAt(0) != '-')
				throw(new CascadeSVMParameterFormatError("Illegal input format."));
			switch (args[i].charAt(1)) {
			case 'k': parameter.kernelPath = args[i+1]; break;
			case 'l': parameter.labelPath = args[i+1];  break;
			case 'i': parameter.idlistPath = args[i+1]; break;
			case 'n':
				if (args[i].charAt(2) == 's')
					parameter.nSubset = Integer.parseInt(args[i+1]);
				else if (args[i].charAt(3) == 'f')
					parameter.nFold = Integer.parseInt(args[i+1]);
				else
					throw(new CascadeSVMParameterFormatError("Illegal input format."));
			}
		}
		return parameter;
	}
	
	public static CascadeSVMTrainParameter parseArgs(String argsLine) 
			throws CascadeSVMParameterFormatError {
		String[] args = argsLine.split(" ");
		return parseArgs(args);
	}
}
