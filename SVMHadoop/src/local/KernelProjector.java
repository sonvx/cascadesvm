package local;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class KernelProjector {
	
	public float[][] projectHadoop(FileSystem fs, String kerneldir, ArrayList<Integer> sampleIDList, int kernelDim) {
		sampleIDList = arrayMinusOne(sampleIDList);
		int sizeOfFloat = (int) (Float.SIZE/8.0d);
		//0) Sort the sample ID List
		Collections.sort(sampleIDList);
		
		//1) Calculate the skip array according to the IDList
		int[] skiparary = new int[sampleIDList.size()];
		int endLineSkip = -1;
		skiparary[0] = (sampleIDList.get(0) - 0)*sizeOfFloat;
		for(int i = 1 ; i < sampleIDList.size() ; i++) {
			skiparary[i] = (sampleIDList.get(i) - sampleIDList.get(i-1) -1)*sizeOfFloat; 
		}
		endLineSkip = (kernelDim - sampleIDList.get(sampleIDList.size()-1) - 1)*sizeOfFloat;
		int wholeLineSkip = kernelDim * sizeOfFloat;
		
		
		//2) Find out the kernel chunks that needs to access
		
		ArrayList<FileStatus> sortedKernel = null;
		int chunkSize = -1;
		try {
			sortedKernel = sortKernelPathHadoop(fs, kerneldir);
			chunkSize =  getChunkSize(sortedKernel.get(0).getPath().getName());
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		float[][] result = new float[sampleIDList.size()][sampleIDList.size()];
		
		//3) Project the kernel values
		BufferedInputStream br = null;
		
		
		int curKernelID = -1;
		byte[] floatbyte = new byte[4];
		int lastLineInKernel = 0;
		try {
			for(int i = 0 ; i < sampleIDList.size() ; ) {
				if(sampleIDList.get(i) < (curKernelID+1)*chunkSize) {
					int numLine2Skip = sampleIDList.get(i)%chunkSize - lastLineInKernel;
					safeSkip(br, numLine2Skip*wholeLineSkip);
					for(int j = 0 ; j < skiparary.length ; j++) {
						safeSkip(br, skiparary[j]);
						br.read(floatbyte);
						result[i][j] = KernelCalculator.toIEEE754Float(floatbyte);
					}
					safeSkip(br, endLineSkip);
					lastLineInKernel = sampleIDList.get(i)%chunkSize + 1;
					i++;
				} else {
					if(br != null) br.close();
					curKernelID++;		//move to next kernel chunk
					br = new BufferedInputStream(fs.open(sortedKernel.get(curKernelID).getPath()));
					lastLineInKernel = 0;
					continue;	//without increasing i
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
		
		/*for(int i = 0 ; i < result.length ; i++) {
			for(int j = 0 ; j < result[i].length ; j++) {
				System.out.print(result[i][j] + " ");
			}
			System.out.print("\n");
		}*/
		
		return result;
	}
	
	
	public float[][] project(File kerneldir, ArrayList<Integer> sampleIDList, int kernelDim) {
		sampleIDList = arrayMinusOne(sampleIDList);
		int sizeOfFloat = (int) (Float.SIZE/8.0d);
		//0) Sort the sample ID List
		Collections.sort(sampleIDList);
		
		//1) Calculate the skip array according to the IDList
		int[] skiparary = new int[sampleIDList.size()];
		int endLineSkip = -1;
		skiparary[0] = (sampleIDList.get(0) - 0)*sizeOfFloat;
		for(int i = 1 ; i < sampleIDList.size() ; i++) {
			skiparary[i] = (sampleIDList.get(i) - sampleIDList.get(i-1) -1)*sizeOfFloat; 
		}
		endLineSkip = (kernelDim - sampleIDList.get(sampleIDList.size()-1) - 1)*sizeOfFloat;
		int wholeLineSkip = kernelDim * sizeOfFloat;
		
		
		//2) Find out the kernel chunks that needs to access
		
		ArrayList<File> sortedKernel = null;
		int chunkSize = -1;
		try {
			sortedKernel = sortKernelPath(kerneldir);
			chunkSize =  getChunkSize(sortedKernel.get(0).getName());
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		float[][] result = new float[sampleIDList.size()][sampleIDList.size()];
		
		//3) Project the kernel values
		BufferedInputStream br = null;
		
		
		int curKernelID = -1;
		byte[] floatbyte = new byte[4];
		int lastLineInKernel = 0;
		try {
			for(int i = 0 ; i < sampleIDList.size() ; ) {
				if(sampleIDList.get(i) < (curKernelID+1)*chunkSize) {
					int numLine2Skip = sampleIDList.get(i)%chunkSize - lastLineInKernel;
					safeSkip(br, numLine2Skip*wholeLineSkip);
					for(int j = 0 ; j < skiparary.length ; j++) {
						safeSkip(br, skiparary[j]);
						br.read(floatbyte);
						result[i][j] = KernelCalculator.toIEEE754Float(floatbyte);
					}
					safeSkip(br, endLineSkip);
					lastLineInKernel = sampleIDList.get(i)%chunkSize + 1;
					i++;
				} else {
					if(br != null) br.close();
					curKernelID++;		//move to next kernel chunk
					br = new BufferedInputStream(new FileInputStream(sortedKernel.get(curKernelID)));
					lastLineInKernel = 0;
					continue;	//without increasing i
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
		
		/*for(int i = 0 ; i < result.length ; i++) {
			for(int j = 0 ; j < result[i].length ; j++) {
				System.out.print(result[i][j] + " ");
			}
			System.out.print("\n");
		}*/
		
		return result;
	}
	
	
	
	private void safeSkip(BufferedInputStream br, long numOfBytes) throws IOException {
		long actualSkipedByte = 0;
		while(actualSkipedByte != numOfBytes) {
			actualSkipedByte += br.skip(numOfBytes-actualSkipedByte);
		}
	}
	
	private ArrayList<FileStatus> sortKernelPathHadoop(FileSystem fs, String kerneldir) throws Exception {
		
		FileStatus[] status = fs.listStatus(new Path(kerneldir));
		ArrayList<FileStatus> sortedFiles = new ArrayList<FileStatus>();
		ArrayList<String> a_feature_binary_name = new ArrayList<String>();
		for (int i = 0; i < status.length; i++) {
			String thispath = status[i].getPath().getName();
			if (thispath.indexOf("inpart") != -1) {
				a_feature_binary_name.add(thispath.toString());
			}
		}
		
		ArrayList<Integer> kernel_ids = new ArrayList<Integer>();
		
		
		// extract small file id from the a_feature_binary file name
		for (int i = 0; i < status.length; i++) {
			String tfilename = status[i].getPath().getName();
			int id = -1;
			try{
				id = Integer.parseInt(tfilename.substring(
						tfilename.indexOf("-c") + "-c".length(),
						tfilename.lastIndexOf("-")));
			} catch(Exception e) {
				throw new Exception("The input A feature file name is in bad format:" + tfilename +"\n" + e.toString());
			}
			kernel_ids.add(id);
		}

		// sort according to the id! index starting from 0
		for (int i = 0; i < status.length; i++) {
			boolean found = false;
			int j = 0;
			for (; j < kernel_ids.size(); j++) {
				if (i == kernel_ids.get(j)) {
					found = true;
					break;
				}
			}
			if (found) {
				sortedFiles.add(status[j]);
			} else {
				throw new Exception(
						"the input A feature is not consecutive! Missing:" + i);
			}
		}
		
		
		
		//check the chunk size.
		//make sure all kernels except the last one have the same line number
		int chunkSize = -1;
		
		for (int i = 0; i < sortedFiles.size()-1; i++) {
			String tfilename = status[i].getPath().getName();
			int thisSize = getChunkSize(tfilename);
			if(chunkSize == -1) chunkSize = thisSize;
			if(thisSize != chunkSize) throw new Exception("Bad chunk size : " + status[i].getPath().getName());
		}
		
		return sortedFiles;
	}
	
	
	private ArrayList<File> sortKernelPath(File kerneldir) throws Exception {
		File[] status = kerneldir.listFiles();
		ArrayList<File> sortedFiles = new ArrayList<File>();
		ArrayList<String> a_feature_binary_name = new ArrayList<String>();
		for (int i = 0; i < status.length; i++) {
			String thispath = status[i].getName();
			if (thispath.indexOf("inpart") != -1) {
				a_feature_binary_name.add(thispath.toString());
			}
		}
		
		ArrayList<Integer> kernel_ids = new ArrayList<Integer>();
		
		
		// extract small file id from the a_feature_binary file name
		for (int i = 0; i < status.length; i++) {
			String tfilename = status[i].getName();
			int id = -1;
			try{
				id = Integer.parseInt(tfilename.substring(
						tfilename.indexOf("-c") + "-c".length(),
						tfilename.lastIndexOf("-")));
			} catch(Exception e) {
				throw new Exception("The input A feature file name is in bad format:" + tfilename +"\n" + e.toString());
			}
			kernel_ids.add(id);
		}

		// sort according to the id! index starting from 0
		for (int i = 0; i < status.length; i++) {
			boolean found = false;
			int j = 0;
			for (; j < kernel_ids.size(); j++) {
				if (i == kernel_ids.get(j)) {
					found = true;
					break;
				}
			}
			if (found) {
				sortedFiles.add(status[j]);
			} else {
				throw new Exception(
						"the input A feature is not consecutive! Missing:" + i);
			}
		}
		
		
		
		//check the chunk size.
		//make sure all kernels except the last one have the same line number
		int chunkSize = -1;
		
		for (int i = 0; i < sortedFiles.size()-1; i++) {
			String tfilename = status[i].getName();
			int thisSize = getChunkSize(tfilename);
			if(chunkSize == -1) chunkSize = thisSize;
			if(thisSize != chunkSize) throw new Exception("Bad chunk size : " + status[i].getName());
		}
		
		return sortedFiles;
	}
	
	
	
	
	private int getChunkSize(String kernelFileName) throws Exception {
		int thisSize = -1;
		try{
			thisSize = Integer.parseInt(kernelFileName.substring(
					kernelFileName.indexOf("-r") + "-r".length()));
		
		} catch(NumberFormatException e) {
			throw new Exception("The input A feature file name is in bad format:" + kernelFileName +"\n" + e.toString());
		}
		return thisSize;
	}
	
	
	/**
	 * idlist contains the ID starting from 1
	 * We need to convert the ID starting from 0
	 * @param idlist
	 */
	private ArrayList<Integer> arrayMinusOne(ArrayList<Integer> idlist) {
		ArrayList<Integer> internal_idlist = new ArrayList<Integer>();
		for(int i = 0 ; i < idlist.size() ; i++) {
			internal_idlist.add(idlist.get(i)-1);
		}
		return internal_idlist;
	}
	

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		KernelProjector worker = new KernelProjector();
		ArrayList<Integer> c = new ArrayList<Integer>();
		c.add(2);
		c.add(7);
		c.add(9);
		c.add(16);
		c.add(17);
		c.add(25);
		c.add(26);
		c.add(31);
		c.add(36);
		c.add(51);
		c.add(52);
		c.add(58);
		c.add(59);
		c.add(62);
		c.add(65);
		c.add(74);
		c.add(100);
		c.add(103);
		c.add(106);
		c.add(110);
		c.add(113);
		c.add(114);
		c.add(129);
		c.add(149);
		c.add(151);
		c.add(171);
		c.add(181);
		c.add(190);
		c.add(207);
		c.add(220);
		c.add(227);
		c.add(229);
		c.add(230);
		c.add(246);
		c.add(258);
		c.add(263);
		c.add(276);
		c.add(277);
		c.add(293);
		c.add(294);
		c.add(297);
		c.add(302);
		c.add(331);
		c.add(335);
		c.add(342);
		c.add(361);
		c.add(378);
		c.add(396);
		c.add(410);
		c.add(414);
		c.add(417);
		c.add(424);
		c.add(428);
		c.add(437);
		c.add(442);
		c.add(444);
		c.add(447);
		c.add(451);
		c.add(465);
		c.add(489);
		c.add(492);
		c.add(496);
		c.add(510);
		c.add(512);
		c.add(547);
		c.add(552);
		c.add(553);
		c.add(566);
		c.add(569);
		c.add(582);
		c.add(587);
		c.add(592);
		c.add(595);
		c.add(599);
		c.add(603);
		c.add(605);
		c.add(613);
		c.add(650);
		c.add(658);
		c.add(680);
		c.add(687);
		c.add(701);
		c.add(710);
		c.add(713);
		c.add(717);
		c.add(728);
		c.add(739);
		c.add(743);
		c.add(763);
		c.add(770);
		c.add(787);
		c.add(808);
		c.add(823);
		c.add(850);
		c.add(859);
		c.add(866);
		c.add(875);
		c.add(880);
		c.add(887);
		c.add(893);
		c.add(897);
		c.add(903);
		c.add(910);
		c.add(911);
		c.add(920);
		c.add(928);
		c.add(934);
		c.add(937);
		c.add(956);
		c.add(964);
		c.add(966);
		c.add(975);
		c.add(993);
		c.add(1002);
		c.add(1009);
		c.add(1012);
		c.add(1025);
		c.add(1037);
		c.add(1040);
		c.add(1042);
		c.add(1043);
		c.add(1047);
		c.add(1061);
		c.add(1070);
		c.add(1075);
		c.add(1076);
		c.add(1079);
		c.add(1082);
		c.add(1088);
		c.add(1115);
		c.add(1124);
		c.add(1128);
		c.add(1144);
		c.add(1154);
		c.add(1171);
		c.add(1190);
		c.add(1197);
		c.add(1205);
		c.add(1218);
		c.add(1223);
		c.add(1226);
		c.add(1235);
		c.add(1238);
		c.add(1245);
		c.add(1257);
		c.add(1264);
		c.add(1269);
		c.add(1284);
		c.add(1288);
		c.add(1296);
		c.add(1304);
		c.add(1312);
		c.add(1320);
		c.add(1340);
		c.add(1347);
		c.add(1355);
		c.add(1357);
		c.add(1360);
		c.add(1389);
		c.add(1392);
		c.add(1404);
		c.add(1408);
		c.add(1414);
		c.add(1419);
		c.add(1443);
		c.add(1449);
		c.add(1468);
		c.add(1475);
		c.add(1480);
		c.add(1484);
		c.add(1489);
		c.add(1497);
		c.add(1515);
		c.add(1523);
		c.add(1544);
		c.add(1548);
		c.add(1557);
		c.add(1563);
		c.add(1567);
		c.add(1570);
		c.add(1577);
		c.add(1581);
		c.add(1583);
		c.add(1584);
		c.add(1604);
		c.add(1609);
		c.add(1611);
		c.add(1613);
		c.add(1620);
		c.add(1622);
		c.add(1647);
		c.add(1653);
		c.add(1658);
		c.add(1663);
		c.add(1675);
		c.add(1684);
		c.add(1690);
		c.add(1692);
		c.add(1694);
		c.add(1697);
		c.add(1700);
		c.add(1704);
		c.add(1714);
		c.add(1724);
		c.add(1729);
		c.add(1742);
		c.add(1745);
		c.add(1750);
		c.add(1757);
		c.add(1767);
		c.add(1773);
		c.add(1774);
		c.add(1775);
		c.add(1776);
		c.add(1781);
		c.add(1782);
		c.add(1783);
		c.add(1787);
		c.add(1794);
		c.add(1799);
		c.add(1807);
		c.add(1812);
		c.add(1823);
		c.add(1825);
		c.add(1830);
		c.add(1892);
		c.add(1905);
		c.add(1906);
		c.add(1910);
		c.add(1911);
		c.add(1923);
		c.add(1924);
		c.add(1929);
		c.add(1931);
		c.add(1940);
		c.add(1954);
		c.add(1959);
		c.add(1963);
		c.add(1973);
		c.add(1974);
		c.add(1978);
		c.add(1985);
		c.add(1986);
		c.add(2008);
		c.add(2010);
		c.add(2012);
		c.add(2022);
		c.add(2039);
		c.add(2059);
		c.add(2062);
		c.add(2070);
		c.add(2075);
		c.add(2085);
		c.add(2087);
		c.add(2092);
		c.add(2095);
		c.add(2148);
		c.add(2154);
		c.add(2169);
		c.add(2177);
		c.add(2183);
		c.add(2198);
		c.add(2216);
		c.add(2225);
		c.add(2245);
		c.add(2253);
		c.add(2254);
		c.add(2276);
		c.add(2283);
		c.add(2286);
		c.add(2287);
		c.add(2290);
		c.add(2301);
		c.add(2308);
		c.add(2324);
		c.add(2341);
		c.add(2354);
		c.add(2359);
		c.add(2364);
		c.add(2365);
		c.add(2366);
		c.add(2389);
		c.add(2399);
		c.add(2405);
		c.add(2411);
		c.add(2416);
		c.add(2425);
		c.add(2436);
		c.add(2445);
		c.add(2447);
		c.add(2448);
		c.add(2449);
		c.add(2455);
		c.add(2457);
		c.add(2461);
		c.add(2476);
		c.add(2490);
		c.add(2493);
		c.add(2495);
		c.add(2515);
		c.add(2518);
		c.add(2521);
		c.add(2525);
		c.add(2528);
		c.add(2549);
		c.add(2560);
		c.add(2563);
		c.add(2564);
		c.add(2569);
		c.add(2583);
		c.add(2593);
		c.add(2595);
		c.add(2607);
		c.add(2623);
		c.add(2645);
		c.add(2658);
		c.add(2663);
		c.add(2664);
		c.add(2667);
		c.add(2678);
		c.add(2681);
		c.add(2688);
		c.add(2694);
		c.add(2708);
		c.add(2712);
		c.add(2716);
		c.add(2736);
		c.add(2748);
		c.add(2752);
		c.add(2755);
		c.add(2763);
		c.add(2767);
		c.add(2770);
		c.add(2775);
		c.add(2785);
		c.add(2790);
		c.add(2821);
		c.add(2824);
		c.add(2826);
		c.add(2828);
		c.add(2832);
		c.add(2839);
		c.add(2857);
		c.add(2860);
		c.add(2865);
		c.add(2866);
		c.add(2872);
		c.add(2876);
		c.add(2881);
		c.add(2887);
		c.add(2888);
		c.add(2892);
		c.add(2896);
		c.add(2914);
		c.add(2923);
		c.add(2944);
		c.add(2953);
		c.add(2972);
		c.add(2973);
		c.add(2980);
		c.add(2982);
		c.add(2983);
		c.add(2985);
		c.add(2986);
		c.add(3020);
		c.add(3032);
		c.add(3034);
		c.add(3044);
		c.add(3051);
		c.add(3052);
		c.add(3071);
		c.add(3078);
		c.add(3079);
		c.add(3087);
		c.add(3097);
		c.add(3106);
		c.add(3111);
		c.add(3127);
		c.add(3128);
		c.add(3134);
		c.add(3142);
		c.add(3144);
		c.add(3145);
		c.add(3147);
		c.add(3169);
		c.add(3178);
		c.add(3181);
		c.add(3183);
		c.add(3186);
		c.add(3196);
		c.add(3206);
		c.add(3224);
		c.add(3231);
		c.add(3237);
		c.add(3246);
		c.add(3253);
		c.add(3258);
		c.add(3271);
		c.add(3273);
		c.add(3303);
		c.add(3304);
		c.add(3314);
		c.add(3318);
		c.add(3340);
		c.add(3345);
		c.add(3355);
		c.add(3357);
		c.add(3362);
		c.add(3368);
		c.add(3399);
		c.add(3400);
		c.add(3401);
		c.add(3425);
		c.add(3426);
		c.add(3428);
		c.add(3429);
		c.add(3433);
		c.add(3453);
		c.add(3455);
		c.add(3460);
		c.add(3472);
		c.add(3479);
		c.add(3481);
		c.add(3482);
		c.add(3484);
		c.add(3489);
		c.add(3490);
		c.add(3492);
		c.add(3493);
		c.add(3497);
		c.add(3505);
		c.add(3508);
		c.add(3514);
		c.add(3532);
		c.add(3537);
		c.add(3540);
		c.add(3544);
		c.add(3545);
		c.add(3551);
		c.add(3555);
		c.add(3565);
		c.add(3589);
		c.add(3593);
		c.add(3598);
		c.add(3602);
		c.add(3616);
		c.add(3631);
		c.add(3643);
		c.add(3655);
		c.add(3657);
		c.add(3674);
		c.add(3700);
		c.add(3717);
		c.add(3725);
		c.add(3733);
		c.add(3736);
		c.add(3743);
		c.add(3759);
		c.add(3769);
		c.add(3785);
		c.add(3793);
		c.add(3798);
		c.add(3805);
		c.add(3818);
		c.add(3825);
		c.add(3841);
		c.add(3843);
		c.add(3859);
		c.add(3869);
		c.add(3872);
		c.add(3895);
		c.add(3896);
		c.add(3907);
		c.add(3922);
		c.add(3925);
		c.add(3959);
		c.add(3960);
		c.add(3964);
		c.add(3968);
		c.add(3982);
		c.add(3985);
		c.add(3999);
		c.add(4000);
		c.add(4001);
		c.add(4002);
		c.add(4009);
		c.add(4012);
		c.add(4022);
		c.add(4033);
		c.add(4044);
		c.add(4046);
		c.add(4059);
		c.add(4063);
		c.add(4067);
		c.add(4083);
		c.add(4085);
		c.add(4094);
		c.add(4095);
		c.add(4100);
		c.add(4102);
		c.add(4112);
		c.add(4129);
		c.add(4137);
		c.add(4146);
		c.add(4149);
		c.add(4163);
		c.add(4176);
		c.add(4184);
		c.add(4188);
		c.add(4202);
		c.add(4224);
		c.add(4243);
		c.add(4246);
		c.add(4256);
		c.add(4258);
		c.add(4264);
		c.add(4279);
		c.add(4282);
		c.add(4285);
		c.add(4286);
		c.add(4287);
		c.add(4290);
		c.add(4303);
		c.add(4305);
		c.add(4311);
		c.add(4316);
		c.add(4318);
		c.add(4329);
		c.add(4355);
		c.add(4360);
		c.add(4365);
		c.add(4367);
		c.add(4369);
		c.add(4415);
		c.add(4418);
		c.add(4427);
		c.add(4433);
		c.add(4437);
		c.add(4441);
		c.add(4446);
		c.add(4447);
		c.add(4452);
		c.add(4462);
		c.add(4470);
		c.add(4477);
		c.add(4510);
		c.add(4512);
		c.add(4516);
		c.add(4528);
		c.add(4540);
		c.add(4541);
		c.add(4544);
		c.add(4554);
		c.add(4557);
		c.add(4562);
		c.add(4565);
		c.add(4574);
		c.add(4584);
		c.add(4590);
		c.add(4591);
		c.add(4603);
		c.add(4620);
		c.add(4626);
		c.add(4644);
		c.add(4653);
		c.add(4679);
		c.add(4697);
		c.add(4698);
		c.add(4703);
		c.add(4708);
		c.add(4726);
		c.add(4732);
		c.add(4736);
		c.add(4745);
		c.add(4751);
		c.add(4767);
		c.add(4769);
		c.add(4780);
		c.add(4783);
		c.add(4789);
		c.add(4790);
		c.add(4799);
		c.add(4803);
		c.add(4805);
		c.add(4807);
		c.add(4815);
		c.add(4822);
		c.add(4835);
		c.add(4846);
		c.add(4852);
		c.add(4862);
		c.add(4866);
		c.add(4870);
		c.add(4871);
		c.add(4881);
		c.add(4884);
		c.add(4892);
		c.add(4897);
		c.add(4925);
		c.add(4933);
		c.add(4944);
		c.add(4946);
		c.add(4972);
		c.add(4973);
		c.add(4977);
		c.add(4982);
		c.add(4986);
		c.add(4990);
		c.add(4996);
		c.add(4999);
		c.add(5004);
		c.add(5013);
		c.add(5017);
		c.add(5018);
		c.add(5041);
		c.add(5043);
		c.add(5044);
		c.add(5073);
		c.add(5083);
		c.add(5110);
		c.add(5121);
		c.add(5124);
		c.add(5126);
		c.add(5127);
		c.add(5136);
		c.add(5147);
		c.add(5153);
		c.add(5160);
		c.add(5180);
		c.add(5181);
		c.add(5182);
		c.add(5184);
		c.add(5185);
		c.add(5197);
		c.add(5199);
		c.add(5205);
		c.add(5207);
		c.add(5210);
		c.add(5221);
		c.add(5264);
		c.add(5273);
		c.add(5275);
		c.add(5277);
		c.add(5279);
		c.add(5287);
		c.add(5292);
		c.add(5306);
		c.add(5312);
		c.add(5319);
		c.add(5322);
		c.add(5362);
		c.add(5377);
		c.add(5380);
		c.add(5384);
		c.add(5389);
		c.add(5400);
		c.add(5409);
		c.add(5421);
		c.add(5442);
		c.add(5445);
		c.add(5447);
		c.add(5450);
		c.add(5451);
		c.add(5457);
		c.add(5472);
		c.add(5482);
		c.add(5489);
		c.add(5493);
		c.add(5497);
		c.add(5526);
		c.add(5530);
		c.add(5546);
		c.add(5550);
		c.add(5569);
		c.add(5574);
		c.add(5577);
		c.add(5578);
		c.add(5586);
		c.add(5600);
		c.add(5603);
		c.add(5605);
		c.add(5607);
		c.add(5609);
		c.add(5612);
		c.add(5616);
		c.add(5624);
		c.add(5631);
		c.add(5647);
		c.add(5660);
		c.add(5661);
		c.add(5668);
		c.add(5672);
		c.add(5676);
		c.add(5700);
		c.add(5704);
		c.add(5711);
		c.add(5718);
		c.add(5726);
		c.add(5735);
		c.add(5747);
		c.add(5751);
		c.add(5754);
		c.add(5761);
		c.add(5770);
		c.add(5772);
		c.add(5775);
		c.add(5798);
		c.add(5818);
		c.add(5833);
		c.add(5835);
		c.add(5843);
		c.add(5885);
		c.add(5889);
		c.add(5934);
		c.add(5967);
		c.add(5984);
		c.add(6017);
		c.add(6028);
		c.add(6037);
		c.add(6038);
		c.add(6042);
		c.add(6048);
		c.add(6061);
		c.add(6069);
		c.add(6075);
		c.add(6076);
		c.add(6080);
		c.add(6087);
		c.add(6096);
		c.add(6109);
		c.add(6121);
		c.add(6122);
		c.add(6123);
		c.add(6125);
		c.add(6131);
		c.add(6141);
		c.add(6148);
		c.add(6149);
		c.add(6166);
		c.add(6171);
		c.add(6203);
		c.add(6206);
		c.add(6207);
		c.add(6212);
		c.add(6217);
		c.add(6224);
		c.add(6226);
		c.add(6240);
		c.add(6248);
		c.add(6249);
		c.add(6256);
		c.add(6264);
		c.add(6265);
		c.add(6269);
		c.add(6284);
		c.add(6296);
		c.add(6302);
		c.add(6314);
		c.add(6340);
		c.add(6341);
		c.add(6350);
		c.add(6351);
		c.add(6360);
		c.add(6361);
		c.add(6362);
		c.add(6367);
		c.add(6368);
		c.add(6372);
		c.add(6386);
		c.add(6396);
		c.add(6400);
		c.add(6401);
		c.add(6403);
		c.add(6408);
		c.add(6417);
		c.add(6418);
		c.add(6424);
		c.add(6466);
		c.add(6473);
		c.add(6487);
		c.add(6491);
		c.add(6493);
		c.add(6495);
		c.add(6498);
		c.add(6501);
		c.add(6513);
		c.add(6522);
		c.add(6523);
		c.add(6525);
		c.add(6538);
		c.add(6546);
		c.add(6566);
		c.add(6577);
		c.add(6586);
		c.add(6590);
		c.add(6594);
		c.add(6615);
		c.add(6617);
		c.add(6632);
		c.add(6637);
		c.add(6651);
		c.add(6663);
		c.add(6664);
		c.add(6672);
		c.add(6694);
		c.add(6708);
		c.add(6726);
		c.add(6727);
		c.add(6733);
		c.add(6734);
		c.add(6743);
		c.add(6778);
		c.add(6787);
		c.add(6793);
		c.add(6796);
		c.add(6801);
		c.add(6816);
		c.add(6838);
		c.add(6840);
		c.add(6852);
		c.add(6854);
		c.add(6863);
		c.add(6871);
		c.add(6899);
		c.add(6901);
		c.add(6910);
		c.add(6912);
		c.add(6923);
		c.add(6925);
		c.add(6927);
		c.add(6928);
		c.add(6929);
		c.add(6935);
		c.add(6941);
		c.add(6949);
		c.add(6950);
		c.add(6958);
		c.add(6959);
		c.add(6969);
		c.add(6982);
		c.add(6984);
		c.add(6986);
		c.add(7005);
		c.add(7019);
		c.add(7025);
		c.add(7029);
		c.add(7034);
		c.add(7037);
		c.add(7049);
		c.add(7052);
		c.add(7068);
		c.add(7073);
		c.add(7078);
		c.add(7079);
		c.add(7080);
		c.add(7085);
		c.add(7088);
		c.add(7102);
		c.add(7119);
		c.add(7122);
		c.add(7126);
		c.add(7127);
		c.add(7134);
		c.add(7140);
		c.add(7146);
		c.add(7150);
		c.add(7156);
		c.add(7157);
		c.add(7158);
		c.add(7182);
		c.add(7192);
		c.add(7198);
		c.add(7205);
		c.add(7219);
		c.add(7220);
		c.add(7221);
		c.add(7250);
		c.add(7251);
		c.add(7264);
		c.add(7266);
		c.add(7291);
		c.add(7310);
		c.add(7313);
		c.add(7323);
		c.add(7329);
		c.add(7334);
		c.add(7336);
		c.add(7346);
		c.add(7365);
		c.add(7378);
		c.add(7394);
		c.add(7401);
		c.add(7410);
		c.add(7414);
		c.add(7433);
		c.add(7436);
		c.add(7448);
		c.add(7451);
		c.add(7460);
		c.add(7469);
		c.add(7471);
		c.add(7495);
		c.add(7498);
		c.add(7500);
		c.add(7505);
		c.add(7507);
		c.add(7511);
		c.add(7520);
		c.add(7538);
		c.add(7554);
		c.add(7559);
		c.add(7569);
		c.add(7574);
		c.add(7577);
		c.add(7582);
		c.add(7596);
		c.add(7618);
		c.add(7624);
		c.add(7626);
		c.add(7633);
		c.add(7636);
		c.add(7642);
		c.add(7652);
		c.add(7653);
		c.add(7672);
		c.add(7678);
		c.add(7690);
		c.add(7711);
		c.add(7716);
		c.add(7717);
		c.add(7721);
		c.add(7722);
		c.add(7725);
		c.add(7730);
		c.add(7737);
		c.add(7739);
		c.add(7744);
		c.add(7751);
		c.add(7757);
		c.add(7759);
		c.add(7773);
		c.add(7779);
		c.add(7799);
		c.add(7806);
		c.add(7826);
		c.add(7835);
		c.add(7840);
		c.add(7847);
		c.add(7858);
		c.add(7872);
		c.add(7879);
		c.add(7887);
		c.add(7902);
		c.add(7908);
		c.add(7909);
		c.add(7911);
		c.add(7913);
		c.add(7924);
		c.add(7925);
		c.add(7935);
		c.add(7938);
		c.add(7941);
		c.add(7949);
		c.add(7958);
		c.add(7977);
		c.add(7978);
		c.add(7983);
		c.add(7984);
		c.add(7986);
		c.add(8001);
		c.add(8007);
		c.add(8020);
		c.add(8033);
		c.add(8035);
		c.add(8042);
		c.add(8046);
		c.add(8050);
		c.add(8052);
		c.add(8060);
		c.add(8061);
		c.add(8062);
		c.add(8068);
		c.add(8078);
		c.add(8081);
		c.add(8083);
		c.add(8084);
		c.add(8099);
		c.add(8127);
		c.add(8148);
		c.add(8149);
		c.add(8150);
		c.add(8159);
		c.add(8161);
		c.add(8162);
		c.add(8172);
		c.add(8173);
		c.add(8175);
		c.add(8182);
		c.add(8185);
		c.add(8194);
		c.add(8196);
		c.add(8207);
		c.add(8209);
		c.add(8213);
		c.add(8219);
		c.add(8230);
		c.add(8237);
		c.add(8241);
		c.add(8254);
		c.add(8268);
		c.add(8269);
		c.add(8271);
		c.add(8288);
		c.add(8290);
		c.add(8296);
		c.add(8302);
		c.add(8303);
		c.add(8315);
		c.add(8316);
		c.add(8323);
		c.add(8324);
		c.add(8327);
		c.add(8331);
		c.add(8333);
		c.add(8335);
		c.add(8346);
		c.add(8348);
		c.add(8362);
		c.add(8372);
		c.add(8373);
		c.add(8379);
		c.add(8381);
		c.add(8395);
		c.add(8417);
		c.add(8421);
		c.add(8428);
		c.add(8445);
		c.add(8453);
		c.add(8467);
		c.add(8472);
		c.add(8475);
		c.add(8477);
		c.add(8487);
		c.add(8511);
		c.add(8513);
		c.add(8525);
		c.add(8536);
		c.add(8538);
		c.add(8539);
		c.add(8547);
		c.add(8563);
		c.add(8576);
		c.add(8577);
		c.add(8583);
		c.add(8590);
		c.add(8598);
		c.add(8648);
		c.add(8649);
		c.add(8655);
		c.add(8656);
		c.add(8659);
		c.add(8668);
		c.add(8669);
		c.add(8673);
		c.add(8676);
		c.add(8697);
		c.add(8699);
		c.add(8706);
		c.add(8709);
		c.add(8715);
		c.add(8719);
		c.add(8727);
		c.add(8766);
		c.add(8773);
		c.add(8793);
		c.add(8794);
		c.add(8802);
		c.add(8808);
		c.add(8818);
		c.add(8842);
		c.add(8846);
		c.add(8868);
		c.add(8888);
		c.add(8898);
		c.add(8899);
		c.add(8910);
		c.add(8922);
		c.add(8924);
		c.add(8937);
		c.add(8951);
		c.add(8961);
		c.add(8964);
		c.add(8974);
		c.add(8977);
		c.add(8980);
		c.add(8986);
		c.add(8989);
		c.add(9018);
		c.add(9020);
		c.add(9025);
		c.add(9030);
		c.add(9033);
		c.add(9037);
		c.add(9042);
		c.add(9051);
		c.add(9055);
		c.add(9059);
		c.add(9064);
		c.add(9069);
		c.add(9079);
		c.add(9107);
		c.add(9115);
		c.add(9124);
		c.add(9129);
		c.add(9133);
		c.add(9134);
		c.add(9137);
		c.add(9140);
		c.add(9142);
		c.add(9149);
		c.add(9150);
		c.add(9156);
		c.add(9157);
		c.add(9161);
		c.add(9172);
		c.add(9208);
		c.add(9211);
		c.add(9231);
		c.add(9240);
		c.add(9248);
		c.add(9250);
		c.add(9283);
		c.add(9285);
		c.add(9317);
		c.add(9328);
		c.add(9341);
		c.add(9344);
		c.add(9349);
		c.add(9350);
		c.add(9351);
		c.add(9353);
		c.add(9358);
		c.add(9375);
		c.add(9387);
		c.add(9391);
		c.add(9405);
		c.add(9407);
		c.add(9416);
		c.add(9437);
		c.add(9441);
		c.add(9445);
		c.add(9447);
		c.add(9451);
		c.add(9455);
		c.add(9456);
		c.add(9473);
		c.add(9482);
		c.add(9491);
		c.add(9497);
		c.add(9501);
		c.add(9512);
		c.add(9520);
		c.add(9524);
		c.add(9534);
		c.add(9551);
		c.add(9555);
		c.add(9556);
		c.add(9559);
		c.add(9580);
		c.add(9582);
		c.add(9587);
		c.add(9595);
		c.add(9604);
		c.add(9607);
		c.add(9611);
		c.add(9621);
		c.add(9633);
		c.add(9634);
		c.add(9637);
		c.add(9668);
		c.add(9680);
		c.add(9689);
		c.add(9696);
		c.add(9704);
		c.add(9710);
		c.add(9742);
		c.add(9747);
		c.add(9750);
		c.add(9770);
		c.add(9776);
		c.add(9783);
		c.add(9788);
		c.add(9789);
		c.add(9792);
		c.add(9797);
		c.add(9805);
		c.add(9839);
		c.add(9845);
		c.add(9847);
		c.add(9874);
		c.add(9892);
		c.add(9914);
		c.add(9921);
		c.add(9922);
		c.add(9939);
		c.add(9940);
		c.add(9947);
		c.add(9949);
		c.add(9966);
		c.add(9978);
		c.add(9982);
		c.add(9985);
		c.add(9989);
		c.add(9991);

		worker.project(new File("C:\\Users\\lujiang\\Downloads\\kernelcomputation_test (1)\\kernel_truth"), c, 30000);
		//worker.projectHadoop(FileSystem.get(new Configuration()), "C:\\Users\\lujiang\\Downloads\\kernelcomputation_test (1)\\kernel_truth", c, 30000);

	}

}
