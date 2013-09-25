package training;

import java.io.IOException;
import java.util.ArrayList;

public class readSchedulerParameterTest {
	public static void main(String[] args) {
		String schedulerParameterPath = "C:\\Users\\Light\\JavaJar\\scheduler.parameter";
		try {
			ArrayList<CascadeSVMSchedulerParameter> schedulerParameters = CascadeSVMIOHelper.readSchedulerParameterHadoop(schedulerParameterPath);
		} catch (IOException e) {
		} catch (CascadeSVMParameterFormatError e) {
			System.out.println(e.toString());
		}
	}
}
