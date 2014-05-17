package beans;

public class IDEntry {
	
	public int lineid;
	public String id;
	public String featureSuffix="";
	
	
	
	public String getFeatureFileName() {
		return(id);
		//return(id + "." + featureSuffix);
	}
	
}
