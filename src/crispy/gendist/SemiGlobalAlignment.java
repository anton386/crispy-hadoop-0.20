package crispy.gendist;

import java.util.Arrays;
import java.util.Vector;

public class SemiGlobalAlignment {
    public String readOne = "";
    public String readTwo = "";
    public Double mismatch = -4.00;
    public Double match = 5.00;
    public Double gap = -10.00;
    public Double[] scoringMatrixPrevRow;
    public Double[] scoringMatrixCurrRow;
    public Integer[] lengthMatrixPrevRow;
    public Integer[] lengthMatrixCurrRow;
    public Integer[] mismatchesMatrixPrevRow;
    public Integer[] mismatchesMatrixCurrRow;
    
    public static void main(String[] args) {
	int n = 1000;
	double[] data_init = new double[n];
	double[] data_alig = new double[n];
	long elapsed_time = System.nanoTime();
	for (int i = 0; i < n; i++) {
	    SemiGlobalAlignment sga = new SemiGlobalAlignment(args[0], args[1]);
	    sga.initializeScoringMatrix();
	    sga.alignmentScore();
	}
	elapsed_time = System.nanoTime() - elapsed_time;
	System.out.println("Total: " + (elapsed_time / (double) n) / (1000000000));
    }

    public SemiGlobalAlignment(String read1, String read2) {
	if (read1.length() > read2.length()) {
	    this.readOne = read1;
	    this.readTwo = read2;
	} else {
	    this.readOne = read2;
	    this.readTwo = read1;
	}
    }

    public SemiGlobalAlignment(String read1, String read2,
			       Double match, Double mismatch,
			       Double gapOpen) {
	if (read1.length() > read2.length()) {
	    this.readOne = read1;
	    this.readTwo = read2;
	} else {
	    this.readOne = read2;
	    this.readTwo = read1;
	}
	this.match = match;
	this.mismatch = mismatch;
	this.gap = gapOpen;
    }

    public Double execute() {
	this.initializeScoringMatrix();
	return this.alignmentScore();
    }

    public void initializeScoringMatrix() {
	int size = this.readOne.length() + 1;
	this.scoringMatrixPrevRow = new Double[size];
	this.lengthMatrixPrevRow = new Integer[size];
	this.mismatchesMatrixPrevRow = new Integer[size];

	for (int i = 0; i < size; i++) {
	    this.scoringMatrixPrevRow[i] = 0.00;
	    this.lengthMatrixPrevRow[i] = 0;
	    this.mismatchesMatrixPrevRow[i] = 0;
	}
    }

    public Double alignmentScore() {

	Double score = Double.NEGATIVE_INFINITY;
	Integer length = 0;
	Integer mismatches = 0;
	Integer lengthTwo = this.readTwo.length();
	Integer lengthOne = this.readOne.length();

	for (int j = 1; j < (lengthTwo+1); j++) {
	    
	    this.scoringMatrixCurrRow = new Double[lengthOne+1];
	    this.lengthMatrixCurrRow = new Integer[lengthOne+1];
	    this.mismatchesMatrixCurrRow = new Integer[lengthOne+1];
	    
	    this.scoringMatrixCurrRow[0] = 0.00;
	    this.lengthMatrixCurrRow[0] = 0;
	    this.mismatchesMatrixCurrRow[0] = 0;

            for (int i = 1; i < (lengthOne+1); i++) {

                Double x = this.scoringMatrixPrevRow[i] + this.gap;  // Upper
                Double y = this.scoringMatrixCurrRow[i-1] + this.gap;  // Lower
                Double z = 0.00;
                if (this.readOne.charAt(i-1) == this.readTwo.charAt(j-1)) {
                    z = this.scoringMatrixPrevRow[i-1] + this.match;
                } else {
                    z = this.scoringMatrixPrevRow[i-1] + this.mismatch;
                }
		this.scoringMatrixCurrRow[i] = Math.max(Math.max(x,y),z);


		Integer a = 0;
		Integer b = 0;
		if (this.scoringMatrixCurrRow[i] == x) {		
		    a = this.lengthMatrixPrevRow[i] + 1;
		    b = this.mismatchesMatrixPrevRow[i] + 1;
		} else if (this.scoringMatrixCurrRow[i] == y) {
		    a = this.lengthMatrixCurrRow[i-1] + 1;
		    b = this.mismatchesMatrixCurrRow[i-1] + 1;
		} else {
		    if (this.readOne.charAt(i-1) == this.readTwo.charAt(j-1)) {
			a = this.lengthMatrixPrevRow[i-1] + 1;
			b = this.mismatchesMatrixPrevRow[i-1];
		    } else {
			a = this.lengthMatrixPrevRow[i-1] + 1;
			b = this.mismatchesMatrixPrevRow[i-1] + 1;
		    }
		}
		
		this.lengthMatrixCurrRow[i] = a;
		this.mismatchesMatrixCurrRow[i] = b;

		
		if (j == lengthTwo && this.scoringMatrixCurrRow[i] > score) {
		    score = this.scoringMatrixCurrRow[i];
		    length = this.lengthMatrixCurrRow[i];
		    mismatches = this.mismatchesMatrixCurrRow[i];
		}
		if (i == lengthOne && this.scoringMatrixCurrRow[i] > score) {
		    score = this.scoringMatrixCurrRow[i];
		    length = this.lengthMatrixCurrRow[i];
		    mismatches = this.mismatchesMatrixCurrRow[i];
		}
            }
	    
	    this.scoringMatrixPrevRow = this.scoringMatrixCurrRow;
	    this.lengthMatrixPrevRow = this.lengthMatrixCurrRow;
	    this.mismatchesMatrixPrevRow = this.mismatchesMatrixCurrRow;

        }

	Double m = (double) mismatches;
	Double l = (double) length;
	Double distance = m/l;
	
	return distance;
    }


}