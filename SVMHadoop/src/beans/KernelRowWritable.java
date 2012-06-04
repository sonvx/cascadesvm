package beans;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class KernelRowWritable  implements WritableComparable<KernelRowWritable> {
	
	public FloatArrayWritable floatarray;
	public ShortArrayWritable shortarray;
	public IntWritable lineid;
	
	public KernelRowWritable() {
		super();
		floatarray = new FloatArrayWritable();
		shortarray = new ShortArrayWritable();
		lineid = new IntWritable();
	}
	
	
	public void setFloat(FloatWritable[] values) {
		floatarray.set(values);
	}
	
	public void setShort(ShortWritable[] values) {
		shortarray.set(values);
	}
	
	public void setLineID(int in) {
		lineid = new IntWritable(in);
	}


	@Override
	public void readFields(DataInput in) throws IOException {
		lineid.readFields(in);
		shortarray.readFields(in);
		floatarray.readFields(in);
	}


	@Override
	public void write(DataOutput out) throws IOException {
		lineid.write(out);
		shortarray.write(out);
		floatarray.write(out);
		
	}
	
	public String toString(int n) {
		StringBuffer sb = new StringBuffer();
		sb.append(lineid.get() + " ");
		Writable[] s =  shortarray.get();
		Writable[] f =  floatarray.get();
		
		for(int i = 0 ; i <Math.min(f.length,n) ; i++) {
			sb.append(((ShortWritable)s[i]).get() + ":" + ((FloatWritable)f[i]).get());
		}
		sb.append("\r\n");
		return sb.toString();
	}
	
	public KernelRow toKernelRow() {
		Writable[] s =  shortarray.get();
		Writable[] f =  floatarray.get();
		KernelRow row = new KernelRow(lineid.get(), s.length);
		for(int i = 0 ; i <f.length ; i++) {
			row.indexes[i] = ((ShortWritable)s[i]).get();
			row.values[i] = ((FloatWritable)f[i]).get();
		}
		
		return row;
	}



	@Override
	public int compareTo(KernelRowWritable o) {
		// TODO Auto-generated method stub
		return (this.lineid.get() - o.lineid.get());
	}



	
}
