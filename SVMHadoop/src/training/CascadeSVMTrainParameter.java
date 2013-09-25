package training;

public class CascadeSVMTrainParameter {
	// required
	public String kernelPath;
	public String labelPath;
	public String idlistPath;
	public String workDir;
	// optional
	public int nSubset;
	public double epsilon;
	public int max_iter;
	public double cost;
	public double gamma;
	public int nData;
	
	public static int parameterSize = 4;
	
	public static String helpText = 
			"Usage: CascadeSVMTrain [options] kernel_path label_path idlist_path work_directory\n" +
			"-n nSubset  : the size of the splitted subset (default 8)\n" +
			"-c cost : set the parameter C of C-SVC, epsilon-SVR, and nu-SVR (default 1)\n" +
			"-g gamma : set gamma in kernel function (default 1e-3)\n" +
			"-e epsilon  : stopping criteria (default 1e-3)\n" +
			"-i max_iter : max iteration (default 5)";
	
	public CascadeSVMTrainParameter(
			String kernelPath, 
			String labelPath, 
			String idlistPath,
			String workDir,
			int nSubset, 
			double cost,
			double gamma,
			double epsilon,
			int max_iter,
			int nData) {
		this.kernelPath = kernelPath;
		this.labelPath  = labelPath;
		this.idlistPath = idlistPath;
		this.workDir    = workDir;
		this.nSubset    = nSubset;
		this.cost		= cost;
		this.gamma		= gamma;
		this.epsilon	= epsilon;
		this.max_iter   = max_iter;
		this.nData		= nData;
	}
	
	public CascadeSVMTrainParameter(
			String kernelPath,
			String labelPath,
			String idlistPath,
			String workDir) {
		this();
		this.kernelPath = kernelPath;
		this.labelPath  = labelPath;
		this.idlistPath = idlistPath;
		this.workDir  = workDir;
	}
	
	public CascadeSVMTrainParameter() {
		nSubset = 8;
		cost	= 1;
		gamma	= 1e-3;
		epsilon = 1e-3;
		max_iter = 5;
	}
	
	public CascadeSVMTrainParameter(CascadeSVMTrainParameter parameter) {
		this(
				parameter.kernelPath,
				parameter.labelPath,
				parameter.idlistPath,
				parameter.workDir,
				parameter.nSubset,
				parameter.cost,
				parameter.gamma,
				parameter.epsilon,
				parameter.max_iter,
				parameter.nData
		);
	}

	public CascadeSVMTrainParameter(String[] args) 
			throws CascadeSVMParameterFormatError {
		this();
		int i = 0;
		while (i < args.length) {
			if (args[i].charAt(0) != '-')
				break;
			switch (args[i].charAt(1)) {
			case 'n': nSubset = Integer.parseInt(args[i+1]); break;
			case 'c': cost = Double.parseDouble(args[i+1]); break;
			case 'g': gamma = Double.parseDouble(args[i+1]); break;
			case 'e': epsilon = Double.parseDouble(args[i+1]); break;
			case 'i': max_iter = Integer.parseInt(args[i+1]); break;
			default:
				throw(new CascadeSVMParameterFormatError("Illegal optional parameter."));
			}
			i += 2;
		}
		if (args.length - i != parameterSize) {
			throw(new CascadeSVMParameterFormatError("Illegal parameter size."));
		}
		kernelPath = args[i];
		labelPath  = args[i+1];
		idlistPath = args[i+2];
		workDir    = args[i+3];
	}
	
	public CascadeSVMTrainParameter(String argsLine) 
			throws CascadeSVMParameterFormatError {
		this(argsLine.split(" "));
	}

	public String toString() {
		return kernelPath + " " + labelPath + " " + idlistPath + " " + workDir + " " + 
				Integer.toString(nSubset) + " " + Double.toString(cost) + " " + Double.toString(gamma) + " " + 
				Double.toString(epsilon) + " " + Integer.toString(max_iter) + " " + Integer.toString(nData);
	}
}
