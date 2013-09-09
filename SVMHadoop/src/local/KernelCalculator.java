package local;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;


import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.FloatWritable;

import beans.KernelRow;


/**
 * 
 * @author lujiang
 *
 */
public class KernelCalculator {
	
	public KernelRow[] inmatrix;
	
	public KernelCalculator(File in) throws IOException {
		super();
		String filename = in.getName();
		int lineno = Integer.parseInt(filename.substring(filename.lastIndexOf("-")+1, filename.length()));
		loadKernelRow(in,lineno);
		
	}
	

	public KernelCalculator() throws IOException {
		super();
	}
	
	
	
	public float[] chi2LeiWithZeros(KernelRow row) {
		float[] result =  new float[inmatrix.length];
		for(int i = 0 ; i < inmatrix.length ; i++) {
			result[i] = -1 * dotchi2productLei_KernelZero(row, inmatrix[i])/2 ;
		}
		return result;
	}
	
	
	public float[] user_defined(KernelRow row) {
		return null;
	}
	
	public float[] rbf(KernelRow row) {
		float[] result =  new float[inmatrix.length];
		for(int i = 0 ; i < inmatrix.length ; i++) {
			result[i] = -1 * dptrbfproduct(row, inmatrix[i])/2;
		}
		return result;
	}
	
	

	public float[] chi2(KernelRow row) {
		float[] result =  new float[inmatrix.length];
		for(int i = 0 ; i < inmatrix.length ; i++) {
			result[i] = -1 * dotchi2productLei(row, inmatrix[i])/2 ;
		}
		return result;
	}
	
	public FloatWritable[] chi2Writable(KernelRow row) {
		FloatWritable[] result =  new FloatWritable[inmatrix.length];
		for(int i = 0 ; i < inmatrix.length ; i++) {
			float f = -1 * dotchi2productLei(row, inmatrix[i])/2 ;
			result[i] = new FloatWritable(f) ;
		}
		return result;
	}
	
	
	public float[] intersection(KernelRow row) {
		float[] result =  new float[inmatrix.length];
		for(int i = 0 ; i < inmatrix.length ; i++) {
			result[i] = dotmin(row, inmatrix[i])/2 ;
		}
		return result;
	}
	
	
	public float[] linear(KernelRow row) {
		float[] result =  new float[inmatrix.length];
		for(int i = 0 ; i < inmatrix.length ; i++) {
			result[i] = dotproduct(row, inmatrix[i]) ;
		}
		return result;
	}

	
	
	
	public float dotproduct(KernelRow r1, KernelRow r2) {
		double result = 0;
		int n1 = r1.indexes.length;
		int n2 = r2.indexes.length;
		for (int p1 = 0, p2 = 0; p1 < n1 && p2 < n2;) {
			int ind1 = r1.indexes[p1];
			int ind2 = r2.indexes[p2];
			if (ind1 == ind2) {
				result += (r1.values[p1]* r2.values[p2]);
				p1++;
				p2++;
			} else if (ind1 > ind2) {
				p2++;
			} else {
				p1++;
			}
		}
		return (float)result;
	}
	
	
	public float dotmin(KernelRow r1, KernelRow r2) {
		float result = 0;
		int n1 = r1.indexes.length;
		int n2 = r2.indexes.length;
		for (int p1 = 0, p2 = 0; p1 < n1 && p2 < n2;) {
			int ind1 = r1.indexes[p1];
			int ind2 = r2.indexes[p2];
			if (ind1 == ind2) {
				result += (r1.values[p1] > r2.values[p2] ? r2.values[p2] : r1.values[p1]);
				p1++;
				p2++;
			} else if (ind1 > ind2) {
				p2++;
			} else {
				p1++;
			}
		}
		return result;
	}
	
	
	/**
	 * Lei's chi square kernel
	 * @param r1 kernel row 1
	 * @param r2 kernel row 2
	 * @return
	 */
	public float dotchi2productLei(KernelRow r1, KernelRow r2) {
		float result = 0;
		int n1 = r1.indexes.length;
		int n2 = r2.indexes.length;
		int p1 = 0, p2 = 0;
		if(n1==0 || n2 ==0)	return 2;
		for (; p1 < n1 && p2 < n2;) {
			int ind1 = r1.indexes[p1];
			int ind2 = r2.indexes[p2];
			if (ind1 == ind2) {
				// (x_i*y_i)/(x_i+y_i)
				float sum = r1.values[p1] + r2.values[p2];
				if(sum == 0)
					result += 0;	//impossible to happen if all element in input matrix are positive
				else
					result += Math.pow((r1.values[p1]- r2.values[p2]),2)/sum;
				p1++;
				p2++;
			} else if (ind1 > ind2) {
				result += r2.values[p2];
				p2++;
			} else {
				result += r1.values[p1];
				p1++;	
			}
		}
		if(p1 < n1) {
			while(p1 < n1) {
				result += r1.values[p1];
				p1++;
			}
		} else if(p2 < n2) {
			while(p2 < n2) {
				result += r2.values[p2];
				p2++;
			}
		}
		return result;
	}
	
	
	public float dptrbfproduct(KernelRow r1, KernelRow r2) {
		float result = 0;
		int n1 = r1.indexes.length;
		int n2 = r2.indexes.length;
		int p1 = 0, p2 = 0;
		for (; p1 < n1 && p2 < n2;) {
			int ind1 = r1.indexes[p1];
			int ind2 = r2.indexes[p2];
			if (ind1 == ind2) {
				result += Math.pow((r1.values[p1]- r2.values[p2]),2);
				p1++;
				p2++;
			} else if (ind1 > ind2) {
				result += Math.pow(r2.values[p2],2);
				p2++;
			} else {
				result += Math.pow(r1.values[p1],2);
				p1++;	
			}
		}
		if(p1 < n1) {
			while(p1 < n1) {
				result += Math.pow(r1.values[p1],2);
				p1++;
			}
		} else if(p2 < n2) {
			while(p2 < n2) {
				result += Math.pow(r2.values[p2],2);
				p2++;
			}
		}
		return result;
	}
	
	/**
	 * Lei's chi square kernel
	 * @param r1 kernel row 1
	 * @param r2 kernel row 2
	 * @return
	 */
	public float dotchi2productLei_KernelZero(KernelRow r1, KernelRow r2) {
		float result = 0;
		int n1 = r1.indexes.length;
		int n2 = r2.indexes.length;
		int p1 = 0, p2 = 0;
		if(n1==0 && n2 !=0)	return 2;
		else if(n1 !=0 && n2 ==0)	return 2;
		
		for (; p1 < n1 && p2 < n2;) {
			int ind1 = r1.indexes[p1];
			int ind2 = r2.indexes[p2];
			if (ind1 == ind2) {
				// (x_i*y_i)/(x_i+y_i)
				float sum = r1.values[p1] + r2.values[p2];
				if(sum == 0)
					result += 0;	//impossible to happen if all element in input matrix are positive
				else
					result += Math.pow((r1.values[p1]- r2.values[p2]),2)/sum;
				p1++;
				p2++;
			} else if (ind1 > ind2) {
				result += r2.values[p2];
				p2++;
			} else {
				result += r1.values[p1];
				p1++;	
			}
		}
		if(p1 < n1) {
			while(p1 < n1) {
				result += r1.values[p1];
				p1++;
			}
		} else if(p2 < n2) {
			while(p2 < n2) {
				result += r2.values[p2];
				p2++;
			}
		}
		return result;
	}
	
	
	public float dotchi2product(KernelRow r1, KernelRow r2) {
		float result = 0;
		int n1 = r1.indexes.length;
		int n2 = r2.indexes.length;
		int p1 = 0, p2 = 0;
		for (; p1 < n1 && p2 < n2;) {
			int ind1 = r1.indexes[p1];
			int ind2 = r2.indexes[p2];
			if (ind1 == ind2) {
				// (x_i*y_i)/(x_i+y_i)
				float sum = r1.values[p1] + r2.values[p2];
				if(sum == 0)
					result += 0;	//impossible to happen if all element in input matrix are positive
				else
					result += Math.pow((r1.values[p1]- r2.values[p2]),2)/sum;
				p1++;
				p2++;
			} else if (ind1 > ind2) {
				result += r2.values[p2];
				p2++;
			} else {
				result += r1.values[p1];
				p1++;	
			}
		}
		if(p1 < n1) {
			while(p1 < n1) {
				result += r1.values[p1];
				p1++;
			}
		} else if(p2 < n2) {
			while(p2 < n2) {
				result += r2.values[p2];
				p2++;
			}
		}
		return result;
	}
	
	protected void printMatrix(float[][] matrix) {
		for(int i = 0 ; i < matrix.length ; i++) {
			for(int j = 0 ; j < matrix[i].length ; j++) {
				System.out.print(matrix[i][j] + " ");
			}
			System.out.print("\r\n");
		}
	}
	
	
	
	/**
	 * input format:
	 * lineno(int) size(int) index1(short) value1(float)...
	 * lineno(int) size(int) index1(short) value1(float)...
	 * 
	 * index is the original index-1.
	 * @param in
	 * @throws IOException
	 */
	public void loadKernelRow(FSDataInputStream br, int numofline) throws IOException {
		inmatrix = new KernelRow[numofline];
		
		for(int i = 0 ; i < numofline ; i ++) {
			int lineno = br.readInt();
			int arraysize = br.readInt();
			KernelRow thisrow = new KernelRow(lineno, arraysize);
			for(int j = 0 ; j < arraysize ; j ++) {
				thisrow.indexes[j] = br.readShort();
				thisrow.values[j] = br.readFloat();
			}
			inmatrix[i] = thisrow;
		}
	}
	

	
	
	
	/**
	 * input format:
	 * lineno(int) size(int) index1(short) value1(float)...
	 * lineno(int) size(int) index1(short) value1(float)...
	 * 
	 * index is the original index-1.
	 * @param in
	 * @throws IOException
	 */
	private void loadKernelRow(File in, int numofline) throws IOException {
		inmatrix = new KernelRow[numofline];
		DataInputStream br = new DataInputStream(new BufferedInputStream(new FileInputStream(in),1024*8*16));
		
		for(int i = 0 ; i < numofline ; i ++) {
			int lineno = br.readInt();
			int arraysize = br.readInt();
			KernelRow thisrow = new KernelRow(lineno, arraysize);
			for(int j = 0 ; j < arraysize ; j ++) {
				thisrow.indexes[j] = br.readShort();
				thisrow.values[j] = br.readFloat();
			}
			inmatrix[i] = thisrow;
			if(i%1000==0) {
				//printMemory();
			}
		}
	}
	
	public void printMemory() {
		System.out.println(":max-memory:"+(Runtime.getRuntime().maxMemory()/1024/1024)+
				"m:free-memory:"+(Runtime.getRuntime().freeMemory()/1024/1024)+
				"m:total"+(Runtime.getRuntime().totalMemory()/1024/1024)+"\r\n");
	}
	
	
	/**
	 * @deprecated
	 * @param matrix
	 * @param labels
	 * @throws Exception
	 */
	public static void reconstructKernelMatrix(File matrix, File labels) throws Exception {
		
		ArrayList<Integer> binaryLabels = new ArrayList<Integer>(1024);
		//read labels
		BufferedReader br =  new BufferedReader(new InputStreamReader(new FileInputStream(labels)));
		String line = br.readLine();
		while(line != null) {
			binaryLabels.add(Integer.parseInt(line));
			line = br.readLine();
		}
		br.close();
		
		
		try {
			String name = matrix.getName().substring(0,matrix.getName().indexOf(".kernelmatrix"))+".temp";
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(labels.getParent(),name))),1024*8*8);
			br =  new BufferedReader(new InputStreamReader(new FileInputStream(matrix)),1024*8*8);
			line = br.readLine();
			int lineno = 0;
			while(line != null) {
				line = line.substring(line.indexOf(" 0:"),line.length());
				bw.write(binaryLabels.get(lineno) +line+"\r\n");
				line = br.readLine();
				lineno++;
				
			}
			
			bw.flush();
			bw.close();
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	
	public static byte[] toIEEE754Bytes(float in) {
		int floatBits = Float.floatToIntBits(in);
		byte floatBytes[] = new byte[4];
		floatBytes[3] = (byte)(floatBits >> 24);
		floatBytes[2] = (byte)(floatBits >> 16);
		floatBytes[1] = (byte)(floatBits >> 8);
		floatBytes[0] = (byte)(floatBits);
		return floatBytes;
	}
	
	
	public static float toIEEE754Float(byte[] in) {
		int floatbits = (in[0] & 0xFF) | (in[1] & 0xFF) << 8 | (in[2] & 0xFF) << 16 | (in[3] & 0xFF) << 24;
		return Float.intBitsToFloat(floatbits);
	}
	
	
	
	
	
	
	

	
	public static void main(String args[]) throws Exception {
		/*KernelCalculator k = new KernelCalculator(new File("G:\\smalltest\\value\\test-inpart0-251"));
		float[][] result = k.chi2(k.inmatrix);
		k.printMatrix(result);*/
		
		//KernelCalculator k = new KernelCalculator();
		for(int i = 0 ; i < 10 ; i++) {
			long t1 = System.currentTimeMillis();
			//KernelCalculator k = new KernelCalculator(new File("G:\\smalltest\\preformance\\local\\test-inpart0-1500"));
			//float[][] result = k.chi2(k.inmatrix);
			//k.loadKernelRow(new File("G:\\smalltest\\preformance\\local\\test-inpart0-1500"), 1500);
			long t2 = System.currentTimeMillis();
			System.out.println("run" + i + "-" +  (t2-t1));
		}
		
		//k.printMatrix(result);
		
	}

	
	
	
	
}
