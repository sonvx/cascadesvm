package training;

import java.io.IOException;
import java.util.ArrayList;

public class rawId2SequenceFile {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length != 2) {
			System.err.println("Usage: rawId2SequenceFile <input> <output>");
		}
		String rawIdPath = args[0];
		String sequenceFilePath = args[1];
		try {
			ArrayList<Integer> idList = CascadeSVMIOHelper.readRawIdListHadoop(rawIdPath);
			CascadeSVMIOHelper.writeIdListHadoop(sequenceFilePath, idList);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
