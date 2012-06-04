package beans;


public class KernelRow {
	public float[] values;
	public short[] indexes;
	public int lineid;
	
	public KernelRow(int lineid, int dim) {
		indexes = new short[dim];
		values = new float[dim];
		this.lineid = lineid;
	}
	
	public String toString(int n) {
		StringBuffer sb = new StringBuffer();
		sb.append(lineid+ " ");
		
		for(int i = 0 ; i <Math.min(values.length,n) ; i++) {
			sb.append(indexes[i] + ":" + values[i]);
		}
		sb.append("\r\n");
		return sb.toString();
	}
}
