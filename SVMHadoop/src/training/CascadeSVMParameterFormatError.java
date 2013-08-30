package training;

public class CascadeSVMParameterFormatError extends Exception {
	private String errorMessage;
	private static final long serialVersionUID = 1L;
	
	public CascadeSVMParameterFormatError() {
		errorMessage = "[Error]";
	}
	
	public CascadeSVMParameterFormatError(String errorMessage) {
		this.errorMessage = "[Error]" + errorMessage;
	}
	
	public String toString() {
		return errorMessage;
	}
}
