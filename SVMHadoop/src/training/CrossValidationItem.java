package training;

import java.util.ArrayList;

public class CrossValidationItem implements Comparable<CrossValidationItem> {
	public double prob;
	public int label;
	public int fold_no;
	
	
	public CrossValidationItem(double prob, int label, int fold_no) {
		super();
		this.prob = prob;
		this.label = label;
		this.fold_no = fold_no;
	}
	
	
	public CrossValidationItem(double prob, int label) {
		super();
		this.prob = prob;
		this.label = label;
	}


	
	public CrossValidationItem() {
		super();
	}

	
	public void setIndex(int index, double prob) {
		label = index;
		this.prob = prob;
	}

	@Override
	public int compareTo(CrossValidationItem o) {
		return -1*Double.compare(prob, ((CrossValidationItem)o).prob);
	}
	
	
	public String toString() {
		return prob + " "+label;
	}
	
	
	/**
	 * split CrossValidationItem according to the fold_no
	 * @param in
	 * @return
	 */
	public static ArrayList<ArrayList<CrossValidationItem>> getfolds(ArrayList<CrossValidationItem> in) {
		int maxfold = -1;
		for(int i = 0 ; i < in.size() ; i++) {
			if( in.get(i).fold_no > maxfold) {
				maxfold = in.get(i).fold_no;
			}
		}
		
		ArrayList<ArrayList<CrossValidationItem>> groups = new ArrayList<ArrayList<CrossValidationItem>>(maxfold);
		for(int i = 0 ; i < maxfold ; i++) {
			groups.add(new ArrayList<CrossValidationItem>());
		}
		
		
		for(int i = 0 ; i < in.size() ; i++) {
			groups.get(in.get(i).fold_no-1).add(in.get(i));
		}
		
		return groups;
		
	}
}
