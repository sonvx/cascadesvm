% http://www.mathworks.com/help/stats/mvnrnd.html
% http://www.mathworks.com/help/matlab/creating_plots/using-high-level-plotting-functions.html#f6-26386
pos_N = 500;
mu1 = [2 3];
sigma1 = [2 1.5; 1.5 3];
r1 = mvnrnd(mu1, sigma1, pos_N);

neg_N = 500;
mu2 = [1 7];
sigma2 = [2 1.5; 1.5 4];
r2 = mvnrnd(mu2, sigma2, neg_N);

train_matrix = [r1; r2];
train_label = [ones(pos_N, 1); -1.*ones(neg_N, 1)];
save gaussian_data train_matrix train_label;

fig = figure;
plot(r1(:,1),r1(:,2),'ko', 'MarkerSize', 10);
hold all;
plot(r2(:,1),r2(:,2),'kx', 'MarkerSize', 10);
hold off;
axis([-2, 5, -2, 12]);
title('Original Data', 'FontSize', 20);
print(fig, '-djpeg', 'original_data.jpg');
close(fig);
