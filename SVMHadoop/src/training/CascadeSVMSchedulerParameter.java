package training;

public class CascadeSVMSchedulerParameter extends CascadeSVMTrainParameter {
	public int iterationId;
	public double lastLD;
	public String lastSVPath;
	public static int parameterSize = 13;
	public static String NULL_LIST_PATH = "NULL_LIST_PATH";
	public static String helpText = "<iteration id> <last LD> <last support vector Path> " +
			"<kernel path> <label path> <idlist path> <output directory>" +
			"<subset size> <cost> <gamma> <epsilon> <max iteration> <nData>";

	public CascadeSVMSchedulerParameter(
			int iterationId,
			double lastLD,
			String lastSVPath,
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
		super(kernelPath, labelPath, idlistPath, workDir, nSubset, cost, gamma, epsilon, max_iter, nData);
		this.iterationId = iterationId;
		this.lastLD = lastLD;
		this.lastSVPath = lastSVPath;
	}

	/**
	 * @param parameter
	 * This constructor is used to duplicate scheduler parameter when submit job for next iteration.
	 */
	public CascadeSVMSchedulerParameter(CascadeSVMSchedulerParameter parameter) {
		this(
				parameter.iterationId,
				parameter.lastLD,
				parameter.lastSVPath,
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
	
	/**
	 * @param parameter
	 * This constructor is used to create scheduler parameter for first iteration.
	 */
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
		this.kernelPath     = args[3];
		this.labelPath      = args[4];
		this.idlistPath     = args[5];
		this.workDir		= args[6];
		this.nSubset        = Integer.parseInt(args[7]);
		this.cost			= Double.parseDouble(args[8]);
		this.gamma			= Double.parseDouble(args[9]);
		this.epsilon 		= Double.parseDouble(args[10]);
		this.max_iter       = Integer.parseInt(args[11]);
		this.nData			= Integer.parseInt(args[12]);
	} 
	
	public CascadeSVMSchedulerParameter(String argLine) 
			throws CascadeSVMParameterFormatError {
		this(argLine.split(" "));
	}

	public String toString() {
		return  Integer.toString(iterationId) + " " + Double.toString(lastLD)+ " " + lastSVPath + " " + 
				kernelPath + " " + labelPath + " " + idlistPath + " " + workDir + " " + 
				Integer.toString(nSubset) + " " + Double.toString(cost) + " " + Double.toString(gamma) + " " + 
				Double.toString(epsilon) + " " + Integer.toString(max_iter) + " " + Integer.toString(nData);
	} 
}
