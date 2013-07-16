import sys

out = open(sys.argv[2], "w")

with open(sys.argv[1], "r") as f:
    g = f.next()
    counter = 0
    lenseq = 0
    while (g):
        try:
            h = g.strip("\n")
            if counter % 4 == 0:
                old = id
                id = h
            elif counter % 4 == 1:
                seq = h
                lenseq = len(h)
            elif counter % 4 == 2:
                plus = h
            elif counter % 4 == 3:
                qual = h

            if counter % 4 == 0 and len(id) > 0:
                if lenseq > int(sys.argv[3]):
                    out.write(old + "\n" + seq + "\n" +
                              plus + "\n" + qual + "\n");
                old = ""
                seq = ""
                plus = ""
                qual = ""

            counter += 1
            g = f.next()
        except:
            g = False
            if counter % 4 == 0 and len(id) > 0 and lenseq > int(sys.argv[3]):
                out.write(id + "\n" + seq + "\n" +
                          plus + "\n" + qual + "\n");
            
            
