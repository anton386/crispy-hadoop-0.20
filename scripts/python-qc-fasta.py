import sys

out = open(sys.argv[2], "w")

with open(sys.argv[1], "r") as f:
    seq = ""
    id = ""
    g = f.next()
    while (g):
        try:
            string = g.strip("\n")
            if string[0] == ">":
                check = True
                old = id
                id = string
            else:
                check = False
                seq += string

            if check == True and len(seq) > 0:
                out.write(old + "\n" + seq + "\n")
                old = ""
                seq = ""
                
            g = f.next()
        except:
            g = False
            out.write(id + "\n" + seq + "\n")
            out.close()
