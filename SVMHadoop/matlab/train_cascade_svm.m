%%
% Function: train_cascade_svm
% Author: Shicheng Xu
% E-mail: lightxuzju@gmail.com
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
    tic;
    N = size(train_label, 1);
    subset_N = floor(N / subset_count);
    layer_size = log2(subset_count)+1;
    %% Compute size array
    %     (1:8:63)
    %     ans = 1     9    17    25    33    41    49    57
    %     (1:8:64)
    %     ans = 1     9    17    25    33    41    49    57
    %     (1:9:65)
    %     ans = 1    10    19    28    37    46    55    64
    %     (1:9:66)
    %     ans = 1    10    19    28    37    46    55    64
    size_array = (1:subset_N:N);
    size_array(subset_count + 1) = N + 1;
   
    %% Compute linear kernel
    train_kernel = train_matrix * train_matrix';
    
    %% Cascade SVM
    % models: MAX_PASSES x layer_subset_num x (subset_count, subset_count /
    % 2, ..., 1)
    MAX_PASSES = 10; % Assume the cascade svm will convergence in 10 passes.
    models = cell(1, MAX_PASSES);
    train_subset_ids = cell(1, MAX_PASSES);
    LDs = cell(1, MAX_PASSES);
    convergence = false;
    more_train_id = [];
    for pass=1:MAX_PASSES
        disp(['[PASS] ', int2str(pass)]);
        
        models{pass} = cell(1, layer_size);
        train_subset_ids{pass} = cell(1, layer_size);
        LDs{pass} = cell(1, MAX_PASSES);
        
        train_ids = randperm(N); % Split data set randomly
%         train_ids = gen_unbalanced_data(train_label, subset_count); % Split data set unbalancely
        subset_count_now = subset_count;
        for i = 1:layer_size
            fprintf('\t[LAYER] %d\n', i);
            
            models{pass}{i} = cell(1, subset_count_now);
            train_subset_ids{pass}{i} = cell(1, subset_count_now);
            LDs{pass}{i} = cell(1, subset_count_now);
            
            for j = 1:subset_count_now
                if (i == 1)
                    train_subset_ids{pass}{i}{j} = ...
                        [train_ids(size_array(j):size_array(j+1)-1) more_train_id];
                else
                    train_subset_ids{pass}{i}{j} = ...
                        [models{pass}{i-1}{2*j-1}.SVs models{pass}{i-1}{2*j}.SVs];
                end
                train_subset_ids{pass}{i}{j} = unique(train_subset_ids{pass}{i}{j});
                
                fprintf('\t\t[SUBSET] %d\n', j);
                
                [sub_train_label, sub_train_kernel] = get_subset(train_subset_ids{pass}{i}{j}, train_label, train_kernel);
                models{pass}{i}{j} = svmtrain(sub_train_label, sub_train_kernel, '-q -c 1 -t 4');
                models{pass}{i}{j}.SVs = train_subset_ids{pass}{i}{j}(models{pass}{i}{j}.SVs);
                LDs{pass}{i}{j} = svm_lagrangian(models{pass}{i}{j}, train_label, train_kernel);
                
                fprintf('\t\t\tTotal nSV = %d\n', models{pass}{i}{j}.totalSV);
                fprintf('\t\t\tLDs = %f\n', LDs{pass}{i}{j});
            end
   
            if (pass > 1 && i == 1)
                if (is_convergence(LDs{pass - 1}{layer_size}{1}, LDs{pass}{1}))
                    convergence = true;
                    model = models{pass - 1}{layer_size}{1};
                    total_pass = pass - 1;
                    break;
                end
            end
            subset_count_now = subset_count_now / 2;
        end
        if (convergence)
            break;
        end
        % feed SVs from last layer into first layer.
        more_train_id = models{pass}{layer_size}{1}.SVs;
    end
    if (~convergence)
        model = models{MAX_PASSES - 1}{subset_count}{1};
        total_pass = MAX_PASSES - 1;
    end
    elasped_time = toc;
    fprintf('elasped_time = %f\n', elasped_time);
end

function [sub_label, sub_kernel] = get_subset(ids, label, kernel)
    n = size(ids, 2);
    sub_label = label(ids);
    sub_kernel = [(1:n)', kernel(ids, ids)];
end

function convergence_flag = is_convergence(LD_last, LD_firsts)
    max_LD_first = -Inf;
    size_models = size(LD_firsts, 2);
    for i = 1:size_models
        LD_first = LD_firsts{i};
        if (LD_first > max_LD_first)
            max_LD_first = LD_first;
        end
    end
    if ((max_LD_first - LD_last) / LD_last < 1e-5)
        convergence_flag = true;
    else
        convergence_flag = false;
    end
    fprintf('LD_last = %f\n', LD_last);
    fprintf('max_LD_first = %f\n', max_LD_first);
end

function train_ids = gen_unbalanced_data(train_label, subset_count)
    N = size(train_label, 1);
    subset_N = floor(N / subset_count);
    
    train_ids = zeros(1, N);
    pos_id = find(train_label == 1)';
    neg_id = find(train_label == -1)';
    pos_subset_N = floor(subset_N * 0.9);
    neg_subset_N = subset_N - pos_subset_N;
    p = 1;
    for i = 1:floor(subset_count/2)
        train_ids(p:p+pos_subset_N - 1) = pos_id(randsample(size(pos_id, 2), pos_subset_N));
        p = p + pos_subset_N;
        train_ids(p:p+neg_subset_N - 1) = neg_id(randsample(size(neg_id ,2), neg_subset_N));
        p = p + neg_subset_N;
        pos_id = setdiff(pos_id, train_ids);
        neg_id = setdiff(neg_id, train_ids);
    end
    pos_subset_N = floor(subset_N * 0.1);
    neg_subset_N = subset_N - pos_subset_N;
    for i = floor(subset_count/2)+1 : subset_count-1
        train_ids(p:p+pos_subset_N - 1) = pos_id(randsample(size(pos_id, 2), pos_subset_N));
        p = p + pos_subset_N;
        train_ids(p:p+neg_subset_N - 1) = neg_id(randsample(size(neg_id ,2), neg_subset_N));
        p = p + neg_subset_N;
        pos_id = setdiff(pos_id, train_ids);
        neg_id = setdiff(neg_id, train_ids);
    end
    train_ids(p:p+size(pos_id, 2)-1)=pos_id;
    p = p+size(pos_id, 2);
    train_ids(p:p+size(neg_id, 2)-1)=neg_id;
end