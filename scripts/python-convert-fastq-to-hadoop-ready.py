import sys
import re
import numpy

f = open(sys.argv[1], "r")
g = open(sys.argv[2], "w")
data = {}
d = []
length = 0
i = 0
pattern = "length=([0-9]+)"
prog = re.compile(pattern)
for f1, f2 in enumerate(f):
    if f1 == (1 + (i*4)):
        seq = f2.strip("\r\n")
        try:
            data[seq]
        except:
            if "N" not in f2:
                data[seq] = len(seq)
        i += 1

datav = data.values()
mean = numpy.array(datav).mean()
std_dev = numpy.array(datav).std()

mean_plus = mean + std_dev
mean_minus = mean - std_dev

print mean_plus
print mean_minus
print mean

i = 0
for k, v in data.items():
    if v < mean_plus and v > mean_minus:
        g.write("\t".join([str(i), k]) + "\n")

        i += 1

f.close()
g.close()
