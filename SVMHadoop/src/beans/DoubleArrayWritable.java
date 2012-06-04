package beans;



import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;


public class DoubleArrayWritable extends ArrayWritable {
	
	public DoubleArrayWritable() {
		super(DoubleWritable.class);
	}
	
	
	public String toString() {
        StringBuilder sb = new StringBuilder();
        for (String s : super.toStrings())
        {
            sb.append(s).append(" ");
        }
        return sb.toString();
    }
	
	

}
