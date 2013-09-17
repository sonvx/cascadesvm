function [model, Lp] = train_single_svm(train_label, train_matrix)
	train_N = size(train_label, 1);

	tic;
	fprintf('Computing training kernel\n');
	
	train_kernel = train_matrix * train_matrix';
	
	elasped_time = toc;
	fprintf('Computing training kernel uses %fs\n', elasped_time);

	tic;
	fprintf('Training SVM model using libsvm\n');
	% Cross Validation...-.-
	% parameter_grid = 2.^(-6:2:6);
	% max_cross_validation_accuracy = 0;
	Best_C = 1;
	% for C = parameter_grid
	%     cross_validation_accuracy = svmtrain(train_label, train_kernel, sprintf('-c %f -v 2 -h 0 -q -t 4', C));
	%     fprintf('C = %f, Accuracy = %f\n', C, cross_validation_accuracy);
	%     if (cross_validation_accuracy > max_cross_validation_accuracy)
	%         max_cross_validation_accuracy = cross_validation_accuracy;
	%         Best_C = C;
	%     end
	% end
	model = svmtrain(train_label, [(1:train_N)', train_kernel], sprintf('-c %f -q -t 4', Best_C));
	elasped_time = toc;
    Lp = svm_lagrangian(model, train_label, train_kernel);
	fprintf('Training SVM model uses %fs\n', elasped_time);
    fprintf('Lp = %f\n', Lp);
end