package beans;

import org.apache.hadoop.io.ArrayWritable;

public class KernelRowArrayWritable extends ArrayWritable {

	public KernelRowArrayWritable() {
		super(KernelRowWritable.class);
	}
	
	

}
