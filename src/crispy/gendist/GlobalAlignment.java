package crispy.gendist;

import java.util.Arrays;
import java.util.Vector;

public class GlobalAlignment {
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
    

    public GlobalAlignment(String read1, String read2) {
	if (read1.length() > read2.length()) {
	    this.readOne = read1;
	    this.readTwo = read2;
	} else {
	    this.readOne = read2;
	    this.readTwo = read1;
	}
    }

    public Double execute() {
	this.initializeScoringMatrix();
	return this.alignmentScore();
    }
    
    public static void main(String[] args) {
	int n = 1000;
	double[] data_init = new double[n];
	double[] data_alig = new double[n];
	long elapsed_time = System.nanoTime();
	for (int i = 0; i < n; i++) {
	    GlobalAlignment ga = new GlobalAlignment(args[0], args[1]);
	    ga.initializeScoringMatrix();
	    ga.alignmentScore();
	}
	elapsed_time = System.nanoTime() - elapsed_time;
	System.out.println("Total: " + (elapsed_time / (double) n) / (1000000000));
    }
    
    public void initializeScoringMatrix() {
	int size = this.readOne.length() + 1;
	this.scoringMatrixPrevRow     = new Double[size];
	this.lengthMatrixPrevRow      = new Integer[size];
	this.mismatchesMatrixPrevRow  = new Integer[size];
	
	for (int i = 0; i < size; i++) {
	    this.scoringMatrixPrevRow[i] = this.gap;
	    this.lengthMatrixPrevRow[i] = i;
	    this.mismatchesMatrixPrevRow[i] = i;
	}
	
    }
    
    public Double alignmentScore() {
	
	Integer lengthTwo = this.readTwo.length();
	Integer lengthOne = this.readOne.length();
	
	for (int j = 1; j < (lengthTwo+1); j++) {

	    this.scoringMatrixCurrRow = new Double[lengthOne+1];
	    this.lengthMatrixCurrRow = new Integer[lengthOne+1];
	    this.mismatchesMatrixCurrRow = new Integer[lengthOne+1];

	    this.scoringMatrixCurrRow[0] = this.gap;
	    this.lengthMatrixCurrRow[0] = j;
	    this.mismatchesMatrixCurrRow[0] = j;
	    
	    for (int i = 1; i < (lengthOne+1); i++) {
		
		Double x = this.scoringMatrixPrevRow[i] + this.gap;
		Double y = this.scoringMatrixCurrRow[i-1] + this.gap;
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

	    }

	    this.scoringMatrixPrevRow = this.scoringMatrixCurrRow;
	    this.lengthMatrixPrevRow = this.lengthMatrixCurrRow;
	    this.mismatchesMatrixPrevRow = this.mismatchesMatrixCurrRow;

	}
	
	Double m = (double) this.mismatchesMatrixPrevRow[lengthOne];
	Double l = (double) this.lengthMatrixPrevRow[lengthOne];

	Double distance = m/l;

	return distance;
	
    }
    

    /*
    public static void main(String[] args) {
	GlobalAlignment ga = new GlobalAlignment(args[0], args[1]);
	ga.initializeScoringMatrix();
	System.out.println(ga.alignmentScore());
	
	for (int i = 0; i < ga.scoringMatrix.length; i++) {
	    System.out.println(Arrays.toString(ga.scoringMatrix[i]));
	}
	for (int i = 0; i < ga.lengthMatrix.length; i++) {
            System.out.println(Arrays.toString(ga.lengthMatrix[i]));
        }
        for (int i = 0; i < ga.mismatchesMatrix.length; i++) {
            System.out.println(Arrays.toString(ga.mismatchesMatrix[i]));
        }
    }
    
    public void initializeScoringMatrix() {
	this.scoringMatrix = new Double[this.readTwo.length()+1][this.readOne.length()+1];
	this.lengthMatrix = new Integer[this.readTwo.length()+1][this.readOne.length()+1];
	this.mismatchesMatrix = new Integer[this.readTwo.length()+1][this.readOne.length()+1];
	
	for (int j = 0; j < (this.readTwo.length()+1); j++) {
	    this.scoringMatrix[j][0] = this.gap;
	    this.lengthMatrix[j][0] = j;
	    this.mismatchesMatrix[j][0] = j;
	}

	for (int i = 1; i < (this.readOne.length()+1); i++) {
	    this.scoringMatrix[0][i] = this.gap;
	    this.lengthMatrix[0][i] = i;
	    this.mismatchesMatrix[0][i] = i;
	}
    }
    
    public Double alignmentScore() {

	Integer lengthTwo = this.readTwo.length();
	Integer lengthOne = this.readOne.length();
	for (int j = 1; j < (lengthTwo+1); j++) {
	    for (int i = 1; i < (lengthOne+1); i++) {
		Double x = this.scoringMatrix[j-1][i] + this.gap; // Upper
		Double y = this.scoringMatrix[j][i-1] + this.gap; // Lower
		Double z = 0.00;
		if (this.readOne.charAt(i-1) == this.readTwo.charAt(j-1)) {
		    z = this.scoringMatrix[j-1][i-1] + this.match;
		} else {
		    z = this.scoringMatrix[j-1][i-1] + this.mismatch;
		}

		Integer a = 0;
		Integer b = 0;
                if (x >= y && x >= z) {
                    a = this.lengthMatrix[j-1][i] + 1;
                    b = this.mismatchesMatrix[j-1][i] + 1;
		    this.scoringMatrix[j][i] = x;
                } else if (y >= x && y >= z) {
                    a = this.lengthMatrix[j][i-1] + 1;
                    b = this.mismatchesMatrix[j][i-1] + 1;
		    this.scoringMatrix[j][i] = y;
                } else {
                    if (this.readOne.charAt(i-1) == this.readTwo.charAt(j-1)) {
                        a = this.lengthMatrix[j-1][i-1] + 1;
                        b = this.mismatchesMatrix[j-1][i-1];
                    } else {
                        a = this.lengthMatrix[j-1][i-1] + 1;
                        b = this.mismatchesMatrix[j-1][i-1] + 1;
                    }
		    this.scoringMatrix[j][i] = z;
                }
		
		this.lengthMatrix[j][i] = a;
                this.mismatchesMatrix[j][i] = b;
	    }
	}
	Double m = (double) this.mismatchesMatrix[lengthTwo][lengthOne];
	Double l = (double) this.lengthMatrix[lengthTwo][lengthOne];
	Double distance = m/l;

	return distance;
    }
    
    public static Double[] maxScore(Double[] score) {
	Double max = score[0];
	Double index = 0.00;
	for (int i = 0; i < score.length; i++) {
	    if (score[i] > max) {
		max = score[i];
		index = (double) i;
	    }
	}
	Double[] data = {max,index};
	return data;
    }
    */
}