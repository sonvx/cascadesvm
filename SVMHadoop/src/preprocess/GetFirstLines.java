package preprocess;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;


public class GetFirstLines {

	public static void peek(File in) throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(in)),8*1024*1024);
		
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File("."+File.separator+in.getName()+".peek"))));
		String line = br.readLine();
		int linecnt = 0;
		while(line != null && linecnt < 10) {
			bw.write(line);
			bw.write("\r\n");
			line = br.readLine();
			linecnt++;
		}
		br.close();
		bw.flush();
		bw.close();
	}
	
	
	public static void peekBinary(File in) throws IOException {
		FileInputStream br = new FileInputStream(in);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File("."+File.separator+in.getName()+".peek"))));
		
		for(int i = 0 ; i < 10 ; i ++) {
			for(int j = 0 ; j < 236696 ; j++) {
				byte[] bs = new byte[4];
				br.read(bs);
				ByteBuffer buffer = ByteBuffer.wrap(bs);
				float f = buffer.getFloat();
				bw.write(f+" ");
				
			}
			byte[] bs = new byte[4];
			br.read(bs);
			ByteBuffer buffer = ByteBuffer.wrap(bs);
			float f = buffer.getFloat();
			bw.write(f+"\r\n");
			
		}
		br.close();
		bw.flush();
		bw.close();
		
	}
	
	public static void main(String args[]) throws IOException {
		peekBinary(new File(args[0]));
	}
}
