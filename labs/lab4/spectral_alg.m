
file = 'example1.dat';
%file = 'example2.dat';
E = csvread(['../data/' file]);

% initial values
sigma = 1.5;
dim = size(E, 2); %to find dimensions -2d or 3d based on input
k = 4;  % no. of clusters

% Affinity matrix A
eleDist = squareform(pdist(E));
A = exp(-(eleDist.^2)./(2*sigma^2));
A = A - diag(ones(size(A, 1), 1));  % to make Aii ==0

% D and matrix L
D = diag(sum(A, 2));
L = single(D^(-0.5)*A*D^(-0.5));

%  k largest eigenvectors of L
[v, d] = eig(L);
[eig_val, indices] = sort(diag(d), 'descend');
eig_vec = v(:, indices);
X = eig_vec(:, 1 : k);

% renormalize X
Y = normr(X);

% cluster rows of Y by k-means
[idx, Centroid] = kmeans(Y, k);

% assign original points according to clustering results by k-means
[~, ind] = sort(indices, 'descend');
clusters = idx(ind);

% plot results
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
