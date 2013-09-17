function plot_cascade_svm_model(train_matrix, train_label, models, train_subset_ids, Lp, total_pass)
    aviobj = avifile('train_cascade_svm.avi', 'compression', 'None', 'videoname', 'train_cascade_svm');
    fig = figure;
    for pass = 1:total_pass
        id = 1;
        for i = 1:size(models{pass}, 2)
            for j = 1:size(models{pass}{i}, 2)
                plot_subset_svm_model(train_matrix, train_label, models{pass}{i}{j}, train_subset_ids{pass}{i}{j}, fig);
                title(['Cascade SVM: PASS ' int2str(pass) '; Subproblem ' int2str(id) '; Lp = ' num2str(Lp{pass}{i}{j})], 'FontSize', 16);
                id = id + 1;
                
                F =  getframe(fig);
                for t = 1:15
                    aviobj = addframe(aviobj, F);
                end
                print(fig, '-djpeg', ['cascade_model_' int2str(pass) '_' int2str(i) '_' int2str(j) '.jpg']);
            end
        end
    end
    close(fig);
    aviobj=close(aviobj);
end

function plot_subset_svm_model(train_matrix, train_label, model, train_subset_id, fig)
    clf(fig);
    positive_matrix = train_matrix(train_label==1, :);
    negative_matrix = train_matrix(train_label==-1, :);
    plot(positive_matrix(:, 1), positive_matrix(:, 2), 'ko', 'MarkerSize', 3);
    hold all;
    plot(negative_matrix(:, 1), negative_matrix(:, 2), 'kx', 'MarkerSize', 3);
    hold off;
    
    hold all;
    pos_subset_id = train_subset_id(train_label(train_subset_id) > 0);
    plot(train_matrix(pos_subset_id, 1), train_matrix(pos_subset_id, 2), 'go', 'MarkerSize', 10);
    hold off;
    
    hold all;
    neg_subset_id = train_subset_id(train_label(train_subset_id) < 0);
    plot(train_matrix(neg_subset_id, 1), train_matrix(neg_subset_id, 2), 'gx', 'MarkerSize', 10);
    hold off;
    
    hold all;
    pos_sv_point = train_matrix(model.SVs(model.sv_coef>0), :);
    plot(pos_sv_point(:, 1), pos_sv_point(:, 2), 'ro' ,'MarkerSize', 10);
    hold off;
    
    hold all;
    neg_sv_point = train_matrix(model.SVs(model.sv_coef<0), :);
    plot(neg_sv_point(:, 1), neg_sv_point(:, 2), 'rx' ,'MarkerSize', 10);
    hold off;
    
    w = train_matrix(model.SVs, :)' * model.sv_coef; % 2 * 1
    b = -model.rho;

    if model.Label(1) == -1
      w = -w;
      b = -b;
    end
    
    x1 = [-2 5];
    x2 = x1 .* (-w(1)/w(2)) - b/w(2);
    hold all;
    line(x1, x2);
    hold off;
    axis([-2, 5, -2, 12]);
    hold all;
    legend('original positive data', 'original negative data', 'subset positive data', 'subset negative data', 'positive support vector', 'negative support vector', 2);
    hold off;
end