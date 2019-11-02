from gensim.models import KeyedVectors
from sklearn.cluster import MiniBatchKMeans
import numpy as np

model = KeyedVectors.load_word2vec_format('~/word2vec/GoogleNews-vectors-negative300.bin',binary=True)
K = 500
V = model.index2word
X = np.zeros((len(V), model.vector_size))

for index, word in enumerate(V):
    X[index, :] += model[word]

classifier = MiniBatchKMeans(n_clusters=K, random_state=0)
classifier.fit(X)