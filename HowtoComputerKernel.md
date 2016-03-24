### The test package for this page is [here](http://www.cs.cmu.edu/~lujiang/applications/kernelcomputation_test.zip) ###

## Kernels ##

CascadeSVM supports the following kernels:

  * Linear
  * RBF
  * Chi Square
  * Intersection
  * User Defined Kernel

To use user defined kernel, please implement the following method in [KernelCalculator](https://code.google.com/p/cascadesvm/source/browse/SVMHadoop/src/local/KernelCalculator.java)

```
public float[] user_defined(KernelRow row)
```

## Input File Format ##

The input files for the kernel calculation are

IDlist:
The first column is the relative location of the feature files and the second column is the ID number.
```
shot1_1_RKF.spbof 1
shot1_2_RKF.spbof 2
shot1_3_RKF.spbof 3
shot1_4_RKF.spbof 4
shot1_5_RKF.spbof 5
shot1_6_RKF.spbof 6
shot1_7_RKF.spbof 7
shot1_8_RKF.spbof 8
shot1_9_RKF.spbof 9
```

If the features belonging to the same video are in the same folder, the idlist file looks like
```
shot1/shot1_1_RKF.spbof 1
shot1/shot1_2_RKF.spbof 2
shot1/shot1_3_RKF.spbof 3
shot1/shot1_4_RKF.spbof 4
shot1/shot1_5_RKF.spbof 5
shot1/shot1_6_RKF.spbof 6
shot1/shot1_7_RKF.spbof 7
shot1/shot1_8_RKF.spbof 8
shot1/shot1_9_RKF.spbof 9
```


## Running Kernel Computation ##
1) Chunk the feature into a binary A matrix.

java -jar kernel-chunk-short.jar in\_feature\_dir idlist out\_A\_matrix\_dir the\_chunk\_size feature\_name\_prefix
  1. in\_feature\_dir is the dir containing the input features
  1. idlist is the given idlist.
  1. out\_A\_matrix\_dir is the dir of output A matrix
  1. the\_chunk\_size is the size of each chunked A matrix. On PSC this value is 1000
  1. eature\_name\_prefix is the prefix of the feature name
```
java -jar a-chunk-short.jar feat test.idlist A 1000 sift
```

2) Write B matrix into the sequence files.

java -jar b-sequencewriter.jar in\_feature\_dir idlist B\_out\_sequencefilename 300
  1. in\_feature\_dir is the dir containing the input features
  1. idlist is the given idlist.
  1. B\_out\_sequencefilename is the name of the output sequence file
  1. 300 is the chunk size
```
java -jar b-sequencewriter.jar feat test.idlist B/small.seq 300
```


3) Upload both A and B to Hadoop

4)Run the kernel computer
hadoop jar pdl-kernel-computer-short.jar binary\_A\_loc sequence\_B\_loc numrowinA outputdir hadoop\_mapreduce\_outdir kernelname
  1. binary\_A\_loc is the location of the uploaded binary A matrix
  1. sequence\_B\_loc is the location of the uploaded B sequence file dir
  1. numrowinA is the row number of A matrix
  1. outputdir is the output dir for the computed kernels
  1. hadoop\_mapreduce\_outdir is the dir required by Hadoop.
  1. kernelname is the name of kernel. Supported kernels: intersection, chi2


```
hadoop dfs -rmr temp
hadoop jar pdl-kernel-computer-short.jar smalltest/new/A smalltest/new/B 2038 smalltest/new/bufferout2 temp intersection
```