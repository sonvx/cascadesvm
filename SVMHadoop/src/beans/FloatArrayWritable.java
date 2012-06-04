package beans;



import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.FloatWritable;


public class FloatArrayWritable extends ArrayWritable {
	
	public FloatArrayWritable() {
		super(FloatWritable.class);
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
