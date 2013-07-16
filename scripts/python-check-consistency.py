import sys

total = 1000
data = {}
check = {}
for i in range(total):
    data[i] = 0
    check[i] = []
    
with open(sys.argv[1], "r") as e:
    for f in e:
        g = f.strip("\n").split("\t");
        if float(g[0]) != 2.0:
            h = g[1].strip("()").split(",")
            try:
                data[int(h[0])] += 1
                data[int(h[1])] += 1
                check[int(h[0])].append(int(h[1]))
                check[int(h[1])].append(int(h[0]))
            except KeyError:
                pass

for i, j in data.items():
    if j == 999:
        print "\t".join([str(i), str(j), str(len(set(sorted(check[i]))))])
        #print "\t".join([str(i), str(j)])
        #print sorted(check[i])
