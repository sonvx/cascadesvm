# Introduction #

The matlab version code simply implements the original idea from Graf et al. The code is used to generate the demo on toy example and for verifying the correctness of the Hadoop version code. You might be interested to use it for testing on your own.

# Details #

The CascadeSVM is implemented in train\_cascade\_svm.m. The definition of the function is:
```
%  Input parameter:
%    train_label: n x 1 vector, where n is the size of instances.
%    train_matrix: n x m matrix, where m is the dimension of features.
%    subset_num: the number of splitted subsets.
%  Return values:
%    model: the final model, in libsvm format.
%    models: the intermediate models, referenced by models{pass}{layer}{subset}.
%    train_subset_ids: the ids of each subset, referenced by train_subset_ids{pass}{layer}{subset}.
%    LDs: the optimal dual form objective function value of each subset solved by libsvm, referenced by LDs{pass}{layer}{subset}.
%    total_pass: the total pass before convergence.
function [model, models, train_subset_ids, LDs, total_pass] = train_cascade_svm(train_label, train_matrix, subset_count)
```

Other code are used to run the experiment on toy examples, basically run\_experiment.m shows how to run everything. The other files are used as:
  * gen\_gaussian\_data.m: generate positive and negative data sets under gaussian distribution.
  * train\_single\_svm.m: train a single svm, used for testing the global convergence of CascadeSVM.
  * plot\_single\_svm\_model.m: plot training data, support vectors and decision boundary of single svm in one figure.
  * plot\_cascade\_svm\_model.m: plot training data, support vectors and decision boundary of cascade svm of each subset in one figure, then generate a video showing the decision boundary of each subset, see the video in home page as an example.

# Tips #

  * You might not be able to call the function svmtrain, since the svmtrain.mexa64 here might not be suitable for your platform, please refer to [LibSVM home page](http://www.csie.ntu.edu.tw/~cjlin/libsvm/) to compile the mex file.

  * If you want to see how the CascadeSVM is training when the split of data is unbalanced, please modify the following code in train\_cascade\_svm.m. Comment line 45 and uncomment line 46 should work.

```
45.         train_ids = randperm(N); % Split data set randomly
46. %         train_ids = gen_unbalanced_data(train_label, subset_count); % Split data set unbalancely
```

# Reference #

1. Graf, Hans P., et al. "Parallel support vector machines: The cascade svm." Advances in neural information processing systems. 2004.

2. LibSVM Home page: http://www.csie.ntu.edu.tw/~cjlin/libsvm/