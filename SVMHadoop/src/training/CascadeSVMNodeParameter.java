package training;

public class CascadeSVMNodeParameter extends CascadeSVMTrainParameter {
	public String modelPath;
	public String SVPath;
	public String LDPath;
	public static int parameterSize = 11;
	public static String helpText = "<model path> <support vector path> <LD path> " +
			"<kernel path> <label path> <idlist path> <output directory> " +
			"<cost> <gamma> <data size> <nonsense sentence>\n";
	
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
		this.workDir  	= args[6];
		this.cost		= Double.parseDouble(args[7]);
		this.gamma		= Double.parseDouble(args[8]);
		this.nData 		= Integer.parseInt(args[9]);
	}
	
	public CascadeSVMNodeParameter(String argLine) 
			throws CascadeSVMParameterFormatError {
		this(argLine.split(" "));
	}
	
	public String toString() {
		String nonsense = "aaaa";
		nonsense = nonsense + nonsense;
		nonsense = nonsense + nonsense;
		nonsense = nonsense + nonsense;
		nonsense = nonsense + nonsense;
		nonsense = nonsense + nonsense;
		nonsense = nonsense + nonsense;
		nonsense = nonsense + nonsense;
		nonsense = nonsense + nonsense;
		nonsense = nonsense + nonsense;
		nonsense = nonsense + nonsense;
		nonsense = nonsense + nonsense;
		nonsense = nonsense + nonsense;
		nonsense = nonsense + nonsense;
		nonsense = nonsense + nonsense;
		nonsense = nonsense + nonsense;
		nonsense = nonsense + nonsense;
		nonsense = nonsense + nonsense;
		nonsense = nonsense + nonsense;
		nonsense = nonsense + nonsense;
		nonsense = nonsense + nonsense;
		nonsense = nonsense + nonsense;
		return  modelPath + " " + SVPath + " " + LDPath + " " + 
				kernelPath + " " + labelPath + " " + idlistPath + " " +
				workDir + " " + Double.toString(cost) + " " + Double.toString(gamma) + " " + nData + " " + nonsense;
	}
	
	public String toShortString() {
		return  modelPath + " " + SVPath + " " + LDPath + " " + 
				kernelPath + " " + labelPath + " " + idlistPath + " " +
				workDir + " " + Double.toString(cost) + " " + Double.toString(gamma) + " " + nData;
	}
}
