function fig = plot_single_svm_model(train_matrix, train_label, model, Lp)
    fig = figure;
    positive_matrix = train_matrix(train_label==1, :);
    negative_matrix = train_matrix(train_label==-1, :);
    plot(positive_matrix(:, 1), positive_matrix(:, 2), 'ko', 'MarkerSize', 2);
    hold all;
    plot(negative_matrix(:, 1), negative_matrix(:, 2), 'kx', 'MarkerSize', 2);
    hold off;
    for i=1:model.totalSV
        hold all;
        sv_point = train_matrix(model.SVs(i),:);
        if (model.sv_coef(i)>0)
            plot(sv_point(1), sv_point(2), 'ro', 'MarkerSize', 10);
        else
            plot(sv_point(1), sv_point(2), 'rx', 'MarkerSize', 10);
        end
        hold off;
    end
    
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
    title(['Single SVM' '; Lp = ' num2str(Lp)], 'FontSize', 16);
    print(fig, '-djpeg', 'single_svm_model.jpg');
    % close(fig);
end