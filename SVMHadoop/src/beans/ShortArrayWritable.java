package beans;

import org.apache.hadoop.io.ArrayWritable;



public class ShortArrayWritable extends ArrayWritable {

	
	public ShortArrayWritable() {
		super(ShortWritable.class);
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
