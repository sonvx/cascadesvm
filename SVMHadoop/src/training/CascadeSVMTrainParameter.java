package training;

public class CascadeSVMTrainParameter {
	public String kernelPath;
	public String labelPath;
	public String idlistPath;
	public int nSubset;
	public int nFold;
	public int dimension;
	public static String helpText = 
			"Usage: CascadeSVMTrain " +
			"-k <kernel path> -l <label path> -i <idlist path> -ns <subset size> -nf <fold> -d <dimension>\n";
	
	public CascadeSVMTrainParameter(
			String kernelPath, 
			String labelPath, 
			String idlistPath, 
			int nSubset, 
			int nFold,
			int dimension) {
		this.kernelPath = kernelPath;
		this.labelPath  = labelPath;
		this.idlistPath = idlistPath;
		this.nSubset = nSubset;
		this.nFold = nFold;
		this.dimension = dimension;
	}
	
	public CascadeSVMTrainParameter() {}
	
	public CascadeSVMTrainParameter(CascadeSVMTrainParameter parameter) {
		this(
				parameter.kernelPath,
				parameter.labelPath,
				parameter.idlistPath,
				parameter.nSubset,
				parameter.nFold,
				parameter.dimension
		);
	}

	public CascadeSVMTrainParameter(String[] args) 
			throws CascadeSVMParameterFormatError {
		int i = 0;
		while (i < args.length) {
			if (args[i].charAt(0) != '-')
				throw(new CascadeSVMParameterFormatError("Illegal input format."));
			switch (args[i].charAt(1)) {
			case 'k': this.kernelPath = args[i+1]; break;
			case 'l': this.labelPath = args[i+1];  break;
			case 'i': this.idlistPath = args[i+1]; break;
			case 'n':
				if (args[i].charAt(2) == 's')
					this.nSubset = Integer.parseInt(args[i+1]);
				else if (args[i].charAt(3) == 'f')
					this.nFold = Integer.parseInt(args[i+1]);
				else
					throw(new CascadeSVMParameterFormatError("Illegal input format."));
				break;
			case 'd':
				this.dimension = Integer.parseInt(args[i+1]); break;
			}
		}
	}
	
	public CascadeSVMTrainParameter(String argsLine) 
			throws CascadeSVMParameterFormatError {
		this(argsLine.split(" "));
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
		if (dimension == 0)
			throw(new CascadeSVMParameterFormatError("Dimension is not specified."));
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
				break;
			case 'd': parameter.dimension = Integer.parseInt(args[i+1]); break;
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
