% read in data
file = 'example1.dat';
%file = 'example2.dat';
E = csvread(['../data/' file]);

%intial value
k = 4;
sigma_gen = 0.5 : 0.5 : 10; % space of 0.5 from 0.5 to 10
dim = size(E, 2);
min_sum = inf;

for sigma = sigma_gen
    % form affinity matrix A
    eleDist = squareform(pdist(E));
    A = exp(-(eleDist.^2)./(2*sigma^2));
    A = A - diag(ones(size(A, 1), 1));      % to keep Aii = 0

    D = diag(sum(A, 2));
    L = single(D^(-0.5)*A*D^(-0.5));

    %  k largest eigenvectors of L
    [v, d] = eig(L);
    [eig_val, indices] = sort(diag(d), 'descend');
    eig_vec = v(:, indices);
    X = eig_vec(:, 1 : k);

    % renormalize X
    Y = normr(X);

    % find min sum of centroids in k means
    [idx, Centroid, sumc] = kmeans(Y, k);
    
    if sum(sumc) < min_sum
        min_sum = min(min_sum, sum(sumc));
        optimalSigma = sigma;
        optIndices = indices;
        optIndex = idx;
    end
end

% assign original points according to clustering results by k-means
[~, indices] = sort(optIndices, 'descend');
clusters = optIndex(indices);

e1 = E(:, 1);
e2 = E(:, 2);
if dim == 3
    e3 = E(:, 3);
end
figure;
for i = 1 : k
    color = rand(1, 3);
    if dim == 2
        scatter(e1(clusters == i), e2(clusters == i), [], color, 'filled');
    elseif dim == 3
        scatter3(e1(clusters == i), e2(clusters == i), e3(clusters == i), [], color, 'filled');
    else
        error('too high dimension for visualisation');
    end
    hold on;
end