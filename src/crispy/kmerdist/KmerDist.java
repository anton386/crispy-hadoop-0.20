package crispy.kmerdist;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * <Usage> java KmerDist <input> <--write|--no-write> <output> 
 **/
public class KmerDist {

    static int k_mer = 6;
    Integer[] read1;
    Integer[] read2;

    /** Call the program to start running this serially
     *  And calculates the distances for all pairwise combinations
     * @param args
     */
    public static void main(String[] args) {
        BufferedReader f = null;
	BufferedWriter out = null;
        String g = null;
        String[] h = null;
        ArrayList<String> index = new ArrayList<String>();
	ArrayList<Integer[]> kmer_index = new ArrayList<Integer[]>();
        int counter = 0;

        try {
            f = new BufferedReader(new FileReader(args[0]));
	    /*
	    if (file_flag == "--write") {
		out = new BufferedWriter(new FileWriter(file_output));
	    }
	    */
	    out = new BufferedWriter(new FileWriter(args[1]));
            g = f.readLine();
            while (g != null) {
                h = g.replaceAll("\n", "").split("\t");
                index.add(h[1]);
                counter = counter + 1;
                g = f.readLine();
            }

	    for (Integer i = 0; i < counter; i++) {
		// enumerate reads here
		Integer[] k1 = KmerDist.enumerateKmers(index.get(i), k_mer);
		// sort them here
		KmerDist.sortKmers(k1);
		kmer_index.add(k1);
	    }

            for (Integer i = 0; i < counter; i++) {
                for (Integer j = i; j < counter; j++) {
                    if (i != j) {
			KmerDist kd = new KmerDist(kmer_index.get(i), kmer_index.get(j)); // compare them
			/*
			if (file_flag == "--write") {
			    out.write("(" + i.toString() + "," + j.toString() + ")" + "\n");
			    out.write("kmerDist: " + kd.execute() + "\n");
			} else {
			    kd.execute();
			}
			*/
			out.write("(" + i.toString() + "," + j.toString() + ")" + "\t" + kd.execute() + "\n");
		    }
		}
	    }
	} catch (FileNotFoundException e) {
	    e.printStackTrace();
	} catch (IOException e) {
	    e.printStackTrace();
	} catch (Exception e) {
	    e.printStackTrace();
	} finally {
	    try {
		f.close();
		/*
		if (file_flag == "--write") {
		    out.close();
		}
		*/
		out.close();
	    } catch (IOException e){
		e.printStackTrace();
	    }
	}
    }

    public KmerDist(Integer[] r1, Integer[] r2) {
	this.read1 = r1;
	this.read2 = r2;
    }

    public double execute() {
	double distance = 0.00;
	try {
	    distance = calculateKmerDist(read1, read2);
	} catch (Exception e) {
	    e.printStackTrace();
	    System.out.println(read1);
	    System.out.println(read2);
	    System.exit(1);
	}
	return distance;
    }

    public static Integer[] enumerateKmers(String s, int k) throws Exception {
	int length = s.length();
	ArrayList<Integer> kmers = new ArrayList<Integer>();
	if (k > length) {
	    throw new Exception("Length of read is shorter than k-mer threshold");
	}
	int limit = length - k + 1;
	for (int i = 0; i < limit; i++) {
	    kmers.add(s.substring(i, i + k).hashCode());
	}
	
	Integer[] j = new Integer[limit];
	return kmers.toArray(j);
    }

    public static void sortKmers(Integer[] kmers) throws Exception {
	Arrays.sort(kmers);
    }

    public double calculateKmerDist(Integer[] i, Integer[] j)
	throws Exception {
	double counter = 0.00;
	int k = 0;
	int l = 0;
	int isize = i.length;
	int jsize = j.length;
	int diff = isize - jsize;
	int min = isize;
	if (diff > 0) {
	    min = jsize;
	}
	while (k < min && l < min) {
	    try {
		if (i[k] > j[l]) {
		    l++;
		} else if (i[k] < j[l]) {
		    k++;
		} else {
		    l++;
		    k++;
		    counter++;
		}
	    } catch (ArrayIndexOutOfBoundsException e) {
		System.out.println(k);
		System.out.println(l);
		System.out.println(counter);
		System.out.println(isize);
		System.out.println(jsize);
		System.out.println(min);
		e.printStackTrace();
		System.exit(1);
	    }
	}
	return 1.00 - counter / (min - k_mer + 1.00);
    }

    /*
    // Use short for kmer = 6 and below for optimization
    // Use int for kmer = 6 and above
    public Short hashSequence(String s, int length) {
        int hash = 0;
        int i = 0;
        int multiplier = 2;
        int j = 0;
        for (int k = 0; k < length; k++) {
            char base = s.charAt(k);
            if (base == 'A') {
                j = 0;
            } else if (base == 'T') {
                j = 1;
            } else if (base == 'G') {
                j = 2;
            } else if (base == 'C') {
                j = 3;
            }
            // Get the bit location and add later
            int location = (j << 2*i);
            // Use short value
            hash = (hash | location);
            i++;
        }
        return (short) hash;
    }
    */

}
