package training;

/*
 * TODO: to be duplicated.
 */
public class CascadeSVMParameter {
	public int iterationId;
	// From last iteration
	public double lastLD;
	public String lastSVPath;
	// This Layer
	public int subsetSize;
	public String subsetListPath;
	public String kernelPath;
	public String labelPath;
	// SVM Parameter
	public int nrFold;
	
	public static int parameterSize = 8;
	
	public CascadeSVMParameter(String argLine) throws CascadeSVMParameterFormatError {
		try {
			parseParameter(argLine.split(" "));
		}
		catch (CascadeSVMParameterFormatError e) {
			System.err.println(e.toString());
			print_help();
			throw e;
		}
	}
	
	private void print_help() {
		System.err.println("Parameter Format:");
		System.err.println("iter LD_last SVPath subsetSize subsetListPath kernelPath labelPath nrFold");
	}
	
	private void parseParameter(String[] args)
			throws CascadeSVMParameterFormatError {
			if (args.length != parameterSize)
				throw new CascadeSVMParameterFormatError("The size of parameter (" + Integer.toString(args.length) + ") should be " + Integer.toString(parameterSize) + ".");
			try {
				iterationId = Integer.parseInt(args[0]);
				lastLD = Double.parseDouble(args[1]);
				lastSVPath = args[2];
				subsetSize = Integer.parseInt(args[3]);
				subsetListPath = args[4];
				kernelPath = args[5];
				labelPath = args[6];
				nrFold = Integer.parseInt(args[7]);
			}
			catch (NumberFormatException e) {
				throw new CascadeSVMParameterFormatError("Illegal format of number.");
			}
		}
}
