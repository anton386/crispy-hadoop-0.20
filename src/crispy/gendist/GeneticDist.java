package crispy.gendist;

import java.util.ArrayList;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FileNotFoundException;
import java.io.IOException;

import crispy.gendist.GlobalAlignment;
import crispy.gendist.GlobalAlignmentAffine;
import crispy.gendist.SemiGlobalAlignment;
import crispy.gendist.SemiGlobalAlignmentAffine;
import crispy.gendist.BandedSemiGlobalAlignmentAffine;

public class GeneticDist {

    public static void main(String[] args) {
	BufferedReader f = null;
	BufferedWriter out = null;
	String g = null;
	String[] h = null;
	ArrayList<String> index = new ArrayList<String>();
	int counter = 0;

	try {
	    // Read in read 1, Read in read 2
	    f = new BufferedReader(new FileReader(args[0]));
	    out = new BufferedWriter(new FileWriter(args[1]));
	    g = f.readLine();
	    while (g != null) {
		// TODO
		h = g.replaceAll("\n", "").split("\t");
		index.add(h[1]);
		counter = counter + 1;
		g = f.readLine();
	    }
	    for (Integer i = 0; i < counter; i++) {
		for (Integer j = i; j < counter; j++) {
		    if (i != j) {
			GeneticDist gd = new GeneticDist();
			out.write("(" + i.toString() + "," + j.toString() + ")" + "\t" + gd.execute(index.get(i), index.get(j), Double.parseDouble(args[2])) + "\n");
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
		out.close();
	    } catch (IOException e) {
		e.printStackTrace();
	    }
	}
    }

    public double execute(String r1, String r2, Double width) {
	BandedSemiGlobalAlignmentAffine bsgaa = new BandedSemiGlobalAlignmentAffine(r1, r2, 
										    width);
	return bsgaa.execute();
    }

    public double execute(String r1, String r2, Double width, 
			  Double match, Double mismatch, 
			  Double gapOpen, Double gapE) {
	
	BandedSemiGlobalAlignmentAffine bsgaa = new BandedSemiGlobalAlignmentAffine(r1, r2,
										    width,
										    match,
										    mismatch,
										    gapOpen,
										    gapE);
	return bsgaa.execute();
    }
}