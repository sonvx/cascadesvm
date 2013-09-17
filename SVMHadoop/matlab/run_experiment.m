run gen_gaussian_data;
load gaussian_data;
[single_svm_model, Lp] = train_single_svm(train_label, train_matrix);
save single_svm_model single_svm_model;
plot_single_svm_model(train_matrix, train_label, single_svm_model, Lp);
[model, models, train_subset_ids, Lps, total_pass] = train_cascade_svm(train_label, train_matrix, 8);
save cascade_svm_model model models train_subset_ids;
plot_cascade_svm_model(train_matrix, train_label, models, train_subset_ids, Lps, total_pass);
