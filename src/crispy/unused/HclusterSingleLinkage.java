import java.util.Arrays;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.Set;
import java.util.Map;

public class HclusterSingleLinkage {

    Double[][] fullMatrix;
    ArrayList<Point> points;
    TreeMap<Double, ArrayList<ArrayList<Integer>>> data;
    TreeMap<Integer, Node> cluster;
    

    public static void main(String[] args) {
	Integer size = Integer.parseInt(args[0]);
	HclusterSingleLinkage hcsl = new HclusterSingleLinkage();
	hcsl.createPoints(size);
	hcsl.createFullMatrix();
	for (Map.Entry<Double, ArrayList<ArrayList<Integer>>> d : hcsl.data.entrySet()) {
	    Double e = d.getKey();
	    ArrayList<ArrayList<Integer>> f = d.getValue();
	    
	    System.out.println(e.toString() + ", " + f.toString());
	}
	hcsl.clusterFullMatrix();
	for (Map.Entry<Integer, Node> d : hcsl.cluster.entrySet()) {
	    Integer e = d.getKey();
	    Node f = d.getValue();
	    
	    System.out.println(e.toString() + ", " + f.index.toString() + ", " + f.distance.toString());
	}
    }

    public void createPoints(int size) {
	this.points = new ArrayList<Point>();
	for (int i = 0; i < size; i++) {
	    this.points.add(new Point());
	}
    }

    public void createFullMatrix() {
	Integer size = this.points.size();
	this.fullMatrix = new Double[size][size];
	this.data = new TreeMap<Double, ArrayList<ArrayList<Integer>>>();
	for (int j = 0; j < size; j++) {
	    for (int i = 0; i < j; i++) {
		if (j != i) {
		    Double distance = Point.euclidean(this.points.get(i), this.points.get(j));
		    this.fullMatrix[j][i] = distance;

		    ArrayList<ArrayList<Integer>> value = this.data.get(distance);
		    if (value == null) {
			ArrayList<Integer> setOfPoints = new ArrayList<Integer>();
			setOfPoints.add(i);
			setOfPoints.add(j);
			ArrayList<ArrayList<Integer>> setsOfPoints = new ArrayList<ArrayList<Integer>>();
			setsOfPoints.add(setOfPoints);
			this.data.put(distance, setsOfPoints);
		    } else {
			ArrayList<Integer> setOfPoints = new ArrayList<Integer>();
			setOfPoints.add(i);
			setOfPoints.add(j);
			value.add(setOfPoints);
		    }
		}
	    }
	}
    }

    public void clusterFullMatrix() {
	Integer size = this.points.size();
	Integer newKeyStart = size;
	Integer newKeyEnd = size + size - 2;
	this.cluster = new TreeMap<Integer, Node>();
	for (Map.Entry<Double, ArrayList<ArrayList<Integer>>> d : this.data.entrySet()) {
	    Double e = d.getKey();
	    ArrayList<ArrayList<Integer>> f = d.getValue();

	    for (ArrayList<Integer> g : f) {
		Node newNode = new Node(newKeyStart, e);
		// check for value
		// recursively check for node till its absent and return
		// if both nodes are different, add newNode
		// otherwise, continue with the next nearest value
		Integer node1 = this.recursiveCheckNode(g.get(0));
		Integer node2 = this.recursiveCheckNode(g.get(1));
		if (node1 != node2) {
		    this.cluster.put(node1, newNode);
		    this.cluster.put(node2, newNode);

		    // End Cluster condition
		    if (newKeyStart == newKeyEnd) {
			Node LastNode = new Node(-1, e);
			this.cluster.put(newKeyStart, LastNode);
			break;
		    }

		    newKeyStart += 1;
		}
	    }
	}
    }

    public Integer recursiveCheckNode(Integer nodeIndex) {
	Node node = this.cluster.get(nodeIndex);
	if (node == null) {
	    // if absent, return that nodeIndex for comparison before Update
	    return nodeIndex;
	} else {
	    // if present, get node
	    return this.recursiveCheckNode(node.index);
	}
    }

}

class Node {
    Integer index;
    Double distance;
    public Node(Integer index, Double distance) {
	this.index = index;
	this.distance = distance;
    }
}

class Point {

    Double x;
    Double y;

    public Point() {
	this.x = Math.random();
	this.y = Math.random();
    }

    public static Double euclidean(Point p1, Point p2) {
	Double hypotheneus = Math.pow((p1.x - p2.x), 2.00) + Math.pow((p1.y - p2.y), 2.00);
	return Math.sqrt(hypotheneus);
    }
}

