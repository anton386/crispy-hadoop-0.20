package crispy.gendist;

import java.util.Arrays;
import java.util.Vector;

public class BandedSemiGlobalAlignmentAffine extends SemiGlobalAlignment {
    
    public Double gapExtension = -5.00;
    public Integer bandedK = this.readTwo.length() / 5;
    public Double[] upperLevelPrevRow;
    public Double[] upperLevelCurrRow;
    public Double[] lowerLevelPrevRow;
    public Double[] lowerLevelCurrRow;

    
    public static void main (String[] args) {
	int n = 1000;
	double[] data_init = new double[n];
	double[] data_alig = new double[n];
	long elapsed_time = System.nanoTime();
	for (int i = 0; i < n; i++) {
	    BandedSemiGlobalAlignmentAffine bsgaa = new BandedSemiGlobalAlignmentAffine(args[0], args[1], Double.parseDouble(args[2]));
	    bsgaa.initializeScoringMatrix();
	    bsgaa.alignmentScore();
	}
	elapsed_time = System.nanoTime() - elapsed_time;
	System.out.println("Total: " + (elapsed_time / (double) n) / (1000000000));
    }
    

    public BandedSemiGlobalAlignmentAffine(String read1, String read2) {
	super(read1, read2);
    }

    public BandedSemiGlobalAlignmentAffine(String read1, String read2, Double width) {
	super(read1, read2);
	Double temp = (double) this.readTwo.length() * width;
	this.bandedK = temp.intValue();
    }

    public BandedSemiGlobalAlignmentAffine(String read1, String read2, Double width,
					   Double match, Double mismatch,
					   Double gapOpen, Double gapE) {
	super(read1, read2, match, mismatch, gapOpen);
	Double temp = (double) this.readTwo.length() * width;
	this.bandedK = temp.intValue();
	this.gapExtension = gapE;
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
	    this.scoringMatrixPrevRow[i] = 0.00;
	    this.lengthMatrixPrevRow[i] = 0;
	    this.mismatchesMatrixPrevRow[i] = 0;
	}
    }

    public void initializeUpperMatrix() {
	
	int size = this.readOne.length() + 1;
	this.upperLevelPrevRow = new Double[size];

	for (int i = 0; i < size; i++) {
	    this.upperLevelPrevRow[i] = Double.NEGATIVE_INFINITY;
	}
        
    }

    public void initializeLowerMatrix() {
	
	int size = this.readOne.length() + 1;
	this.lowerLevelPrevRow = new Double[size];
	
	this.lowerLevelPrevRow[0] = Double.NEGATIVE_INFINITY;
        
    }

    public Double[] initializeScoringMatrixCurrRow() {
	
	int size = this.readOne.length()+1;
	Double[] scoringMatrixCurrRow = new Double[size];
	scoringMatrixCurrRow[0] = 0.00;
	for (int i = 1; i < size; i++) {
	    scoringMatrixCurrRow[i] = Double.NEGATIVE_INFINITY;
	}
	return scoringMatrixCurrRow;
    }

    public Double[] initializeUpperLevelCurrRow() {
	
	int size = this.readOne.length()+1;
	Double[] upperLevelCurrRow = new Double[size];
	upperLevelCurrRow[0] = 0.00;
	for (int i = 1; i < size; i++) {
	    upperLevelCurrRow[i] = Double.NEGATIVE_INFINITY;
	}
	return upperLevelCurrRow;
    }

    public Double[] initializeLowerLevelCurrRow() {

	int size = this.readOne.length()+1;
	Double[] lowerLevelCurrRow = new Double[size];
	for (int i = 0; i < size; i++) {
	    lowerLevelCurrRow[i] = Double.NEGATIVE_INFINITY;
	}
	return lowerLevelCurrRow;
    }

    public Double alignmentScore() {

	Double score = Double.NEGATIVE_INFINITY;
	Integer length = 0;
	Integer mismatches = 0;
	Integer lengthTwo = this.readTwo.length();
	Integer lengthOne = this.readOne.length();

        for (int j = 1; j < (lengthTwo+1); j++) {

	    this.scoringMatrixCurrRow = initializeScoringMatrixCurrRow();
	    this.lengthMatrixCurrRow = new Integer[lengthOne+1];
	    this.mismatchesMatrixCurrRow = new Integer[lengthOne+1];
	    this.upperLevelCurrRow = initializeUpperLevelCurrRow();
	    this.lowerLevelCurrRow = initializeLowerLevelCurrRow();
	    
	    this.lengthMatrixCurrRow[0] = 0;
	    this.mismatchesMatrixCurrRow[0] = 0;

            for (int i = Math.max(1, j-this.bandedK+1); i < Math.min((lengthOne+1), j+this.bandedK+1); i++) {
		//if (j > (i-this.bandedK+1)) {
		Double x1 = this.upperLevelPrevRow[i] + this.gapExtension;
		Double x2 = this.scoringMatrixPrevRow[i] + this.gap + this.gapExtension;
		    //} else 
		
		//if (j < (i+this.bandedK+1)) {
		Double y1 = this.lowerLevelCurrRow[i-1] + this.gapExtension;
		Double y2 = this.scoringMatrixCurrRow[i-1] + this.gap + this.gapExtension;
		    //}

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
                this.upperLevelCurrRow[i] = x;
                this.lowerLevelCurrRow[i] = y;
                
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

	    //DEBUG System.out.println(Arrays.toString(this.scoringMatrixCurrRow));
	    //DEBUG System.out.println(Arrays.toString(this.lengthMatrixCurrRow));
	    //DEBUG System.out.println(Arrays.toString(this.mismatchesMatrixCurrRow));
	    //DEBUG System.out.println(Arrays.toString(this.upperLevelCurrRow));
	    //DEBUG System.out.println(Arrays.toString(this.lowerLevelCurrRow));

	    this.scoringMatrixPrevRow = this.scoringMatrixCurrRow;
	    this.lengthMatrixPrevRow = this.lengthMatrixCurrRow;
	    this.mismatchesMatrixPrevRow = this.mismatchesMatrixCurrRow;
	    this.upperLevelPrevRow = this.upperLevelCurrRow;
	    this.lowerLevelPrevRow = this.lowerLevelCurrRow;
        }

        Double m = (double) mismatches;
        Double l = (double) length;
        Double distance = m/l;

        return distance;
    }

    /*
    public static void main(String[] args) {
	BandedSemiGlobalAlignmentAffine bsgaa = new BandedSemiGlobalAlignmentAffine(args[0], args[1]);
	bsgaa.initializeScoringMatrix();
	System.out.println(bsgaa.alignmentScore());
	System.out.println(bsgaa.bandedK);
	for (Vector<Double> a : bsgaa.scoringMatrix) {
	    System.out.println(Arrays.toString(a.toArray()));
	}
	for (Vector<Double> b : bsgaa.upperLevel) {
	    System.out.println(Arrays.toString(b.toArray()));
	}
	for (Vector<Double> c : bsgaa.lowerLevel) {
	    System.out.println(Arrays.toString(c.toArray()));
	}
	for (Vector<Integer> d : bsgaa.lengthMatrix) {
            System.out.println(Arrays.toString(d.toArray()));
        }
        for (Vector<Integer> e : bsgaa.mismatchesMatrix) {
            System.out.println(Arrays.toString(e.toArray()));
        }
    }
    */
    
}