package beans;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.TwoDArrayWritable;

public class FloatMatrix extends TwoDArrayWritable {

	public FloatMatrix() {
		super(FloatWritable.class);
	}
	
	

}
