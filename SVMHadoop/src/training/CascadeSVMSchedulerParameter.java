package training;

public class CascadeSVMSchedulerParameter extends CascadeSVMTrainParameter {
	public int iterationId;
	public double lastLD;
	public String lastSVPath;
	public String subsetListPath;
	public static int parameterSize = 13;
	public static String helpText = "<iteration id> <last LD> <last support vector Path> <subset list path>" +
			"<kernel path> <label path> <idlist path> <data size> <output directory>" +
			"<subset size> <fold> <epsilon> <max iteration>";
	
	public CascadeSVMSchedulerParameter(
			int iterationId,
			double lastLD,
			String lastSVPath,
			String subsetListPath,
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
		this.iterationId = iterationId;
		this.lastLD = lastLD;
		this.lastSVPath = lastSVPath;
		this.subsetListPath = subsetListPath;
	}
	
	public CascadeSVMSchedulerParameter() {
		iterationId = 0;
		lastLD = 0;
	}
	
	public CascadeSVMSchedulerParameter(CascadeSVMSchedulerParameter parameter) {
		this(
				parameter.iterationId,
				parameter.lastLD,
				parameter.lastSVPath,
				parameter.subsetListPath,
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
	
	public CascadeSVMSchedulerParameter(CascadeSVMTrainParameter parameter) {
		super(parameter);
	} 
	
	public CascadeSVMSchedulerParameter(String[] args) 
			throws CascadeSVMParameterFormatError {
		if (args.length < parameterSize)
			throw(new CascadeSVMParameterFormatError("Too few parameters."));
		else if (args.length > parameterSize)
			throw(new CascadeSVMParameterFormatError("Too many parameters."));
		
		this.iterationId    = Integer.parseInt(args[0]);
		this.lastLD         = Double.parseDouble(args[1]);
		this.lastSVPath     = args[2];
		this.subsetListPath = args[3];
		this.kernelPath     = args[4];
		this.labelPath      = args[5];
		this.idlistPath     = args[6];
		this.nData          = Integer.parseInt(args[7]);
		this.workDir		= args[8];
		this.nSubset        = Integer.parseInt(args[9]);
		this.nFold          = Integer.parseInt(args[10]);
		this.epsilon 		= Double.parseDouble(args[11]);
		this.max_iter       = Integer.parseInt(args[12]);
	} 
	
	public CascadeSVMSchedulerParameter(String argLine) 
			throws CascadeSVMParameterFormatError {
		this(argLine.split(" "));
	}

	public String toString() {
		return  Integer.toString(iterationId) + " " + Double.toString(lastLD)+ " " + lastSVPath + " " + subsetListPath + " " +
				kernelPath + " " + labelPath + " " + idlistPath + " " + Integer.toString(nData) + " " +
				workDir + " " + Integer.toString(nSubset) + " " + Integer.toString(nFold) + " " +
				Double.toString(epsilon) + " " + Integer.toString(max_iter);
	} 
}
