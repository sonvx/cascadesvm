package linear;

import java.io.Serializable;

import beans.svm_parameter;


/**
 * Only for binary classification
 * @author lujiang
 *
 */
public class svm_linear_model implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public int  modelID;
	public int layerID;
	public String concept;
	public double threshold_recall;
	
	
	public svm_parameter param;		// parameter
	public int nr_class = 2;		// number of classes, = 2 in regression/one class svm
	public int dim;					// total #SV
	public double[] w;				// coefficients for SVs in decision functions (sv_coef[k-1][l])
	public double[] rho;			// constants in decision functions rho[0] = b
	public double[] probA;         // pariwise probability information
	public double[] probB;
	
	// for classification only
	public int[] label;		// label of each class (label[k])
	
	
	
	
}
