import sys

total = []

with open(sys.argv[1], "r") as f:
    for g in f:
        h = g.strip("\n").split("|")
        i = h[1].strip().split(" ")
        total = total + map(int, i)

print len(total)
nr = set(total)
print len(nr)
print nr
