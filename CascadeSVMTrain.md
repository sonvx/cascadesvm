### The test package for this page is [here](http://www.cs.cmu.edu/~lujiang/applications/CascadeSVMTrain.jar) ###


TODO: The training component is still under alpha test, some instructions might be modified later.

# Preprocessing #

In order to use CascadeSVM to perform training on CascadeSVM, you should prepare kernel, label and id list before you can invoke the jar file in the console to submit job. Please see [HowtoComputeKernel](https://code.google.com/p/cascadesvm/wiki/HowtoComputerKernel) for how prepare the kernel. Here we will provide detail of how to generate the label and id list.

The input format of label is plain text file, each line has two numbers, the first integer is id, and the second float is label. The label can only be 1 or -1, this is because CascadeSVM will perform binary training for each class, then use one-versus-all strategy to predict on testing data. Here is an sample of label file:
```
1 1
2 1
3 -1
4 -1
```

The input format of label is sequence file, which is a flat file consisting of binary key/value pairs. Currently both keys and values are ids. User can first generate an text file and then use class rawId2SequenceFile to convert it to sequence file.

The text file of id list will look like this:
```
1
2
3
4
```

Then on the login node of hadoop, invoke rawId2SequenceFile:
```
hadoop -jar CascadeSVMTrain.jar rawId2SequenceFile idlist.txt idlist.sequence
```

# Run CascadeSVM Train #

Before running CascadeSVM Train, the last thing you need to upload the kernel, label and id list on the HDFS. You can use "hadoop dfs -put file" command to do it. Now you are ready to submit the job. Here is the usage of CascadeSVMTrain.

```
CascadeSVMTrain [options] kernel_path label_path idlist_path work_directory
  -n nSubset  : the size of the splitted subset (default 8)
  -v nFold    : the fold of cross validation (default 5)
  -e epsilon  : stopping criteria (default 1e-5)
  -i max_iter : max iteration (default 5)
```

Notice that you will also need to specify the work directory, which is used to store all the intermedia and final result and it is also on HDFS. For example, if the kernel, label and id list are stored in cascade/kernel, cascade/label, cascade/idlist.sequence, respectively. And cascade/workDir is created to be used as work directory. Then your command will be looked like this,

```
hadoop jar CascadeSVMTrain.jar CascadeSVMTrain cascade/kernel cascade/label cascade/idlist.sequence cascade/workDir
```

# Plan #

  1. Support transforming multiple class label file to binary class label files.
  1. Use id and feature name as key and value for id list.
  1. Organize the output of final model.