import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import sys
from hcluster import pdist, linkage, dendrogram
import numpy as np

data = {}
distances = []

with open(sys.argv[1], "r") as f:
    g = f.next()
    while (g):
        try:
            h = g.strip("\n").split("\t")
            x = int(h[0])
            y = int(h[1])
            z = float(h[2])

            if x < y:
                data[x][y] = z
            else:
                data[y][x] = z
            g = f.next()
        except KeyError:
            if x < y:
                data[x] = {y: z}
            else:
                data[y] = {x: z}
        except StopIteration:
            g = False

k = sorted(data.keys())
print len(k)
for i in k:
    for j in k:
        if i < j:
            distances.append(data[i][j])

link = linkage(np.array(distances), sys.argv[2])
dendrogram(link)

plt.savefig(sys.argv[3], dpi=200)
