package crispy;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.Vector;

public class KmerDist {

	int k_mer = 6;
	String read1;
	String read2;

	/** Call the program to start running this serially
	 *  And calculates the distances for all pairwise combinations
	 * @param args
	 */
	public static void main(String[] args) {
		BufferedReader f = null;
		String g = null;
		String[] h = null;
		Vector<String> index = new Vector<String>();
		int counter = 0;
		try {
			f = new BufferedReader(new FileReader(args[0]));
			g = f.readLine();
			while (g != null) {
				h = g.replaceAll("\n", "").split("\t");
				index.add(h[1]);
				counter = counter + 1;
				g = f.readLine();
			}

			for (Integer i = 0; i < counter; i++) {
				for (Integer j = 0; j < counter; j++) {
					if (i != j) {
					    System.out.println(index.get(i));
					    System.out.println(index.get(j));
					    KmerDist kd = new KmerDist(index.get(i), index.get(j)); // compare
																				// them
					    System.out.println("(" + i.toString() + "," + j.toString() + ")");
					    System.out.println("kmerDist: " + kd.execute());
					}
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		// KmerDist kd = new KmerDist("ATCGGCGTACGGCTT", "ATCGCGGTAAGGCTT");
	}

	public KmerDist(String r1, String r2) {
		this.read1 = r1;
		this.read2 = r2;
	}

	public KmerDist(String r1, String r2, int k) {
		this.read1 = r1;
		this.read2 = r2;
		this.k_mer = k;
	}

	public double execute() {
		// System.out.println("KmerDist algorithm initializing");
		double distance = 0.00;
		try {
			Vector<String> k1 = enumerateKmers(read1, k_mer);
			Vector<String> k2 = enumerateKmers(read2, k_mer);

			sortKmers(k1);
			sortKmers(k2);

			// System.out.println("kmerDist: " + calculateKmerDist(k1, k2));
			distance = calculateKmerDist(k1, k2);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return distance;
	}

	public Vector<String> enumerateKmers(String s, int k) throws Exception {
		int length = s.length();
		Vector<String> kmers = new Vector<String>();
		if (k > length) {
			throw new Exception(
					"Length of read is shorter than k-mer threshold");
		}
		int limit = length - k + 1;
		for (int i = 0; i < limit; i++) {
			kmers.add(s.substring(i, i + k));
		}
		return kmers;
	}

	public void sortKmers(Vector<String> kmers) throws Exception {
		Collections.sort(kmers);
	}

	public double calculateKmerDist(Vector<String> i, Vector<String> j)
			throws Exception {
		int counter = 0;
		int k = 0;
		int l = 0;
		int q = 0;
		int total = i.size() + j.size();
		int diff = i.size() - j.size();
		int min = i.size();
		if (diff > 0) {
			min = j.size();
		} else if (diff > 0) {
			min = i.size();
		}
		while (k < min && l < min) {
			if (i.get(k).hashCode() > j.get(l).hashCode()) {
				l++;
				q++;
			} else if (i.get(k).hashCode() < j.get(l).hashCode()) {
				k++;
				q++;
			} else {
				l++;
				k++;
				counter++;
				q += 2;
			}
		}
		return 1.00 - (double) counter / ((double) min - (double) k_mer + 1.00);
	}

}
