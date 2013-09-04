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
//			ArrayList<Integer> subList = new ArrayList<Integer>();
//			for (int i = 0; i < 100; i++) {
//				subList.add(idList.get(i*100));
//			}
//			CascadeSVMIOHelper.writeIdListHadoop(sequenceFilePath, subList);
			CascadeSVMIOHelper.writeIdListHadoop(sequenceFilePath, idList);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
