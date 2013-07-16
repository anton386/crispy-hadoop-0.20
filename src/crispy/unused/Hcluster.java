// package crispy.hcluster.Hcluster
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class Hcluster {

    int nodes = 10;
    int nnodes = nodes;
    int tnodes = (nodes * 2) - 1;
    Integer read1 = 0;
    Integer read2 = 0;
    Double smallest = 1.00;
    Double[][] data = new Double[nodes][nodes];
    ArrayList<Integer> index = new ArrayList<Integer>();
    HashMap<Integer, HashSet<Integer>> resultsTree = new HashMap<Integer, HashSet<Integer>>();

    public static void main(String[] args) {
	Hcluster h = new Hcluster(Integer.parseInt(args[0]));
	h.test();
    }

    public Hcluster(int nodes) {
	this.nodes = nodes;
	this.nnodes = nodes;
	this.tnodes = (nodes * 2) - 1;
	this.data = new Double[this.nodes][this.nodes];
    }

    public void test() {
	this.createFullMatrix();
	this.createIndex();
	this.createResultsTree();
	while (this.nodes > 1) {
	    System.out.println("Nodes: " + this.nodes);
	    this.printDataMatrix();
	    this.findSmallestValue();
	    this.updateResultsTree();
	    this.updateDataMatrix();
	}
	System.out.println(this.resultsTree);
    }

    public void updateResultsTree() {
	HashSet<Integer> hs = this.resultsTree.get(this.nnodes-1);
	hs.add(this.read1);
	hs.add(this.read2);
    }

    public void updateDataMatrix() {
	Double[][] newDataMatrix = new Double[this.nodes][this.nodes];
	ArrayList<Integer> newIndex = new ArrayList<Integer>();
	for (Integer item : this.index) {
	    newIndex.add(item);
	}

	// remove read1 and read2 from index
	newIndex.remove(this.read1);
	newIndex.remove(this.read2);

	// add new node to index
	newIndex.add(this.nnodes-1);

	int y = 0;
	int x = 0;
	int z = 0;
	int m = 0;
	int n = 0;
	for (int j = 0; j < newIndex.size(); j++) {
	    for (int i = 0; i < j; i++) {
		if (j == newIndex.indexOf(this.nnodes-1)) {
		    y = this.index.indexOf(this.read2);
		    x = this.index.indexOf(this.read1);
		    z = this.index.indexOf(newIndex.get(i));
		    newDataMatrix[j][i] = newDataMatrix[i][j] = ((this.data[y][z] + 
								 this.data[x][z]) / 
								 (double) 2);
		} else {
		    y = this.index.indexOf(this.read2);
		    x = this.index.indexOf(this.read1);
		    newDataMatrix[j][i] = newDataMatrix[i][j] = this.data[x][y];
		}
	    }
	}

	this.data = newDataMatrix;
	this.index = newIndex;
    }
    
    public void findSmallestValue() {
	this.smallest = 1.00;
	for (int j = 0; j < this.nodes; j++) {
	    for (int i = 0; i < j; i++) {
		if (this.data[j][i] < this.smallest) {
		    this.smallest = this.data[j][i];
		    this.read2 = this.index.get(j);
		    this.read1 = this.index.get(i);
		}
	    }
	}
	this.nodes -= 1;
	this.nnodes += 1;

	//System.out.println(this.read1.toString() + "," + this.read2.toString() 
	//		   + "," + this.smallest.toString());
    }
		    
    public void createResultsTree() {
	for (int i = 0; i < this.tnodes; i++) {
	    this.resultsTree.put(i, new HashSet<Integer>());
	}
    }

    public void createIndex() {
	for (int j = 0; j < this.nodes; j++) {
	    this.index.add(j);
	}
    }

    public void createFullMatrix() {
	for (int j = 0; j < this.nodes; j++) {
	    for (int i = 0; i < j; i++) {
		this.data[j][i] = this.data[i][j] = Math.random();
	    }
	}
    }

    public void printDataMatrix() {
	for (int i = 0; i < this.data.length; i++) {
	    System.out.println(Arrays.toString(this.data[i]));
	}
    }
}