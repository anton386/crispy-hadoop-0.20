package crispy.gendist;

import java.util.Arrays;
import java.util.Vector;

public class GlobalAlignmentAffine extends GlobalAlignment {
    
    public Double gapExtension = -5.00;
    public Double[] upperLevelPrevRow;
    public Double[] upperLevelCurrRow;
    public Double[] lowerLevelPrevRow;
    public Double[] lowerLevelCurrRow;

    public static void main(String[] args) {
	int n = 1000;
	double[] data_init = new double[n];
	double[] data_alig = new double[n];
	long elapsed_time = System.nanoTime();
	for (int i = 0; i < n; i++) {
	    GlobalAlignmentAffine gaa = new GlobalAlignmentAffine(args[0], args[1]);
	    gaa.initializeScoringMatrix();
	    gaa.alignmentScore();
	}
	elapsed_time = System.nanoTime() - elapsed_time;
	System.out.println("Total: " + (elapsed_time / (double) n) / (1000000000));
    }

    public GlobalAlignmentAffine(String read1, String read2) {
	super(read1, read2);
    }

    public Double execute() {
	this.initializeScoringMatrix();
	return this.alignmentScore();
    }

    public void initializeScoringMatrix() {
	this.initializeDiagonalMatrix();
	this.initializeUpperMatrix();
	this.initializeLowerMatrix();
    }

    public void initializeDiagonalMatrix() {
	int size = this.readOne.length() + 1;
	this.scoringMatrixPrevRow = new Double[size];
	this.lengthMatrixPrevRow = new Integer[size];
	this.mismatchesMatrixPrevRow = new Integer[size];

	for (int i = 0; i < size; i++) {
	    this.scoringMatrixPrevRow[i] = this.gap + (((double)i)*this.gapExtension);
	    this.lengthMatrixPrevRow[i] = i;
	    this.mismatchesMatrixPrevRow[i] = i;
	}	
    }

    public void initializeUpperMatrix() {

	int size = this.readOne.length() + 1;
	this.upperLevelPrevRow = new Double[size];

	for (int i = 0; i < size; i++) {
	    this.upperLevelPrevRow[i] = Double.NEGATIVE_INFINITY;
	}
	// add increasing gap down the row
	
    }

    public void initializeLowerMatrix() {
	
	int size = this.readOne.length() + 1;
	this.lowerLevelPrevRow = new Double[size];
	
	this.lowerLevelPrevRow[0] = Double.NEGATIVE_INFINITY;
	for (int i = 1; i < size; i++) {
	    this.lowerLevelPrevRow[i] = (this.gap + ((double)i)*this.gapExtension);
	}

	// add NEGATIVE_INFINITY down the row
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
	    this.upperLevelCurrRow = new Double[lengthOne+1];
	    this.lowerLevelCurrRow = new Double[lengthOne+1];
	    
	    this.scoringMatrixCurrRow[0] = (this.gap + ((double)j)*this.gapExtension);
	    this.lengthMatrixCurrRow[0] = j;
	    this.mismatchesMatrixCurrRow[0] = j;
	    this.upperLevelCurrRow[0] = (this.gap + ((double)j)*this.gapExtension);
	    this.lowerLevelCurrRow[0] = Double.NEGATIVE_INFINITY;

	    for (int i = 1; i < (lengthOne+1); i++) {

		Double x1 = this.upperLevelPrevRow[i] + this.gapExtension; // Upper Open
		Double x2 = this.scoringMatrixPrevRow[i] + this.gap + this.gapExtension;

		Double y1 = this.lowerLevelCurrRow[i-1] + this.gapExtension; // Left Open
		Double y2 = this.scoringMatrixCurrRow[i-1] + this.gap + this.gapExtension;

		Double z = 0.00;
		// match or mismatch
		if (this.readOne.charAt(i-1) == this.readTwo.charAt(j-1)) {
		    z = this.scoringMatrixPrevRow[i-1] + this.match;
		} else {
		    z = this.scoringMatrixPrevRow[i-1] + this.mismatch;
		}
		
		Double x = Math.max(x1, x2);
		Double y = Math.max(y1, y2);

		this.scoringMatrixCurrRow[i] = Math.max(Math.max(x,y), z);

                Integer a = 0;
                Integer b = 0;
                if (this.scoringMatrixCurrRow[i] == x) {
                    a = this.lengthMatrixPrevRow[i] + 1;
                    b = this.mismatchesMatrixPrevRow[i] + 1;
                } else if (this.scoringMatrixCurrRow[i] == 1.00) {
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
		this.upperLevelCurrRow[i] = x;
		this.lowerLevelCurrRow[i] = y;
		
	    }
	    
	    this.scoringMatrixPrevRow = this.scoringMatrixCurrRow;
	    this.lengthMatrixPrevRow = this.lengthMatrixCurrRow;
	    this.mismatchesMatrixPrevRow = this.mismatchesMatrixCurrRow;
	    this.upperLevelPrevRow = this.upperLevelCurrRow;
	    this.lowerLevelPrevRow = this.lowerLevelCurrRow;
	}

	Double m = (double) this.mismatchesMatrixPrevRow[lengthOne];
	Double l = (double) this.lengthMatrixPrevRow[lengthOne];
	Double distance = m/l;

	return distance;
    }

    /*
    public static void main(String[] args) {
        GlobalAlignmentAffine gaa = new GlobalAlignmentAffine(args[0], args[1]);
        gaa.initializeScoringMatrix();
	gaa.alignmentScore();
        for (Vector<Double> a : gaa.scoringMatrix) {
            System.out.println(Arrays.toString(a.toArray()));
        }
	for (Vector<Double> b : gaa.upperLevel) {
            System.out.println(Arrays.toString(b.toArray()));
        }
	for (Vector<Double> c : gaa.lowerLevel) {
            System.out.println(Arrays.toString(c.toArray()));
        }
	for (Vector<Integer> d : gaa.lengthMatrix) {
            System.out.println(Arrays.toString(d.toArray()));
        }
        for (Vector<Integer> e : gaa.mismatchesMatrix) {
            System.out.println(Arrays.toString(e.toArray()));
        }
    }
    */
}