package training;

public class CascadeSVMNodeParameter extends CascadeSVMTrainParameter {
	public String modelPath;
	public String SVPath;
	public String LDPath;
	public static int parameterSize = 9;
	public static String helpText = "<model path> <support vector path> <LD path> " +
			"<kernel path> <label path> <idlist path> <output directory> <fold> <data size>\n";
	
	public CascadeSVMNodeParameter() {}
	
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
		this.workDir  = args[6];
		this.nFold      = Integer.parseInt(args[7]);
		this.nData 		= Integer.parseInt(args[8]);
	}
	
	public CascadeSVMNodeParameter(String argLine) 
			throws CascadeSVMParameterFormatError {
		this(argLine.split(" "));
	}
	
	public String toString() {
		return  modelPath + " " + SVPath + " " + LDPath + " " + 
				kernelPath + " " + labelPath + " " + idlistPath + " " +
				workDir + " " + Integer.toString(nFold) + " " + nData;
	} 
}
