function [ Lp ] = svm_lagrangian( svm_model, svm_label, svm_kernel )
%Compute the Lagrangian value of a svm model
%   Lp = 1/2*||w||^2-\sum_{i=1}^l \alpha_i y_i (x_i \cdot w+b)+\sum_{i=1}^l
%   \alpha_i
    svm_label_sv = svm_label(svm_model.SVs);
    svm_kernel_sv = svm_kernel(svm_model.SVs, svm_model.SVs);
    Lp = -0.5 .* sum(sum((svm_model.sv_coef * svm_model.sv_coef') .* ...
        svm_kernel_sv)) + ...
        svm_model.sv_coef' * svm_label_sv;
end

