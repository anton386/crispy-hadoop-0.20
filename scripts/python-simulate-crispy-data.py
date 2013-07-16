import sys
import getopt
import scipy.stats as spstats
import random

'''
Generate Hadoop Ready Reads with the defined parameters
python27 python-simulate-crispy-data.py --length <l> --size <n> --similarity <s>
'''

opts, args = getopt.getopt(sys.argv[1:], "", ["length=", "size=", "similarity="])

for o, a in opts:
    if o == '--length':
        length = int(a)
    elif o == '--size':
        size = int(a)
    elif o == '--similarity':
        similarity = float(a)
    else:
        assert False, "unhandled option"

probability_base = [0.25, 0.25, 0.25, 0.25]
mutation_rate = 1 - similarity

def generate_seed_sequence():
    sequence = []
    for i in range(length):
        index = int(spstats.uniform.rvs()*4)
        if index == 0:
            sequence.append("A")
        elif index == 1:
            sequence.append("T")
        elif index == 2:
            sequence.append("G")
        elif index == 3:
            sequence.append("C")

    return "".join(sequence)

def generate_mutated_sequence(sequence):
    mutated = []
    for i in range(len(sequence)):
        base = sequence[i]

        if random.random() < mutation_rate:
            index = int(spstats.uniform.rvs()*4)
            if index == 0:
                mutated.append("A")
            elif index == 1:
                mutated.append("T")
            elif index == 2:
                mutated.append("G")
            elif index == 3:
                mutated.append("C")
        else:
            mutated.append(base)

    return "".join(mutated)

seed = generate_seed_sequence()
sys.stdout.write("\t".join(["0", seed]) + "\n")
for i in range(1,size):
    sys.stdout.write("\t".join([str(i), generate_mutated_sequence(seed)]) + "\n")
