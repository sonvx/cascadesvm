package training;

import java.io.IOException;
import java.util.ArrayList;

public class readNodeParameter {
	public static void main(String[] args) {
		String nodeParameterPath = "C:/Users/Light/JavaJar/node.parameter";
		try {
			ArrayList<CascadeSVMNodeParameter> nodeParameters = CascadeSVMIOHelper.readNodeParameterHadoop(nodeParameterPath);
		} catch (IOException e) {
		} catch (CascadeSVMParameterFormatError e) {
		}
	}
}
