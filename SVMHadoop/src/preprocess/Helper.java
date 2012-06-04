package preprocess;



public class Helper {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		if(args == null || args.length == 0) {
		String help = "The command line to run:\n"
				+ "  java -cp tools.jar com.example.MainClass [args]\n"
				+ "Functions:"
				+ "1) divide B feature file into smaller text files\n"
				+ "  class:preprocess.BMatrixPartitioner\n"
				+ "2) divide A feature file into smaller binary files\n"
				+ "  class:preprocess.AMatrixPartitioner\n"
				;
		System.out.print(help);
		} else {
			int id = Integer.parseInt(args[0]);
			String help = null;
			if(id == 1) {
				help = "1) divide B feature file into smaller ones\n"
						+ "class:preprocess.BMatrixPartitioner\n"
						+ "partition(File in, File outdir, int chunksize, String filenameprefix)\n";
			} else if(id == 2) {
				help = "2) divide A feature file into smaller ones\n"
						+ "class:preprocess.AMatrixPartitioner\n"
						+ "public static void partation(File in, File outdir, int partno, String prefix)\n";
						
			} else if(id == 3) {
				
			} else if(id == 4) {
				
			}
			
			
			System.out.print(help);
		}
		
	
	}

}
