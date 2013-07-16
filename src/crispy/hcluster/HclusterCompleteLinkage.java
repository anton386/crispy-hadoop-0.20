package crispy.hcluster;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.DoubleWritable;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.IOException;

import crispy.io.Point;
import crispy.io.PointWritable;
import crispy.io.Node;
import crispy.io.ParentNode;
import crispy.io.ChildNode;

public class HclusterCompleteLinkage {

    Node rootNode;
    //ToRemove HashMap<Node, Node> index = new HashMap<Node, Node>();
    HashMap<Integer, Node> index = new HashMap<Integer, Node>();
    HashMap<Node, ArrayList<Integer>> clusters = new HashMap<Node, ArrayList<Integer>>();

    Integer size = 0;
    Integer parentNodeKeyStart = 0;

    Point point;
    Double distance;
    Configuration conf;
    FileSystem fs;
    LocalFileSystem fslocal;
    Node currentNode1;
    Node currentNode2;
    Node currentPNode;
    Integer counter = 100;
    HashMap<Integer, Double> storedValues = new HashMap<Integer, Double>();

    public static void main(String[] args) throws IOException {
	HclusterCompleteLinkage hccl = new HclusterCompleteLinkage(Integer.parseInt(args[3]));
	hccl.constructTree(args[0]);
	hccl.search(Double.parseDouble(args[1]));
	hccl.writeClustersToFile(args[2]);
    }

    public HclusterCompleteLinkage(Integer size) {
	this.size = size;
	this.parentNodeKeyStart = size;
    }

    /**
     * Searches the ParentNodes in the rootNode Tree
     * that meets the threshold distance value. Next,
     * it recursively searches for all ChildNodes and
     * places them in the "clusters" field.
     *
     * @params Double threshold
     *
     */
    public void search(Double threshold) {
	this.recursiveSearchBelowThreshold(this.rootNode, threshold);
	for (Node key : this.clusters.keySet()) {
	    if (key instanceof ParentNode) {
		this.recursiveSearch(key, key);
	    }
	}
    }

    /**
     * Writes the clusters to file.
     *
     */
    public void writeClustersToFile(String filename) throws IOException {
	BufferedWriter out = new BufferedWriter(new FileWriter(filename));
	for (Map.Entry<Node, ArrayList<Integer>> m : this.clusters.entrySet()) {
	    String output = "";
	    if (m.getKey() instanceof ParentNode) {
		ParentNode pNode = (ParentNode) m.getKey();
		
		Integer cluster = m.getKey().hashCode();
		Double distance = pNode.distance;
		String placeholder = "%d | %f | ";
		output = output + String.format(placeholder, cluster, distance);
		for (Integer n : m.getValue()) {
		    output = output + String.format("%d ", n);
		}
		out.write(output);
		out.newLine();
	    }
	    else if (m.getKey() instanceof ChildNode) {
		Integer cluster = m.getKey().hashCode();
		String placeholder = "%d | * | %d";
		output = String.format(placeholder, cluster, cluster);
		out.write(output);
		out.newLine();
	    }
	}
	out.close();
    }

    /**
     * Recursively searches for the ParentNodes that
     * meet the threshold. These are the clusters at
     * the threshold value. The Nodes are placed in
     * the "clusters" field, where the recursiveSearch()
     * subroutine is used to find the ChildNodes that
     * belong to this ParentNode.
     *
     * @params Node node
     * @params Double threshold
     */
    public void recursiveSearchBelowThreshold(Node node, Double threshold) {
	if (node instanceof ParentNode) {
	    ParentNode pNode = (ParentNode) node;
	    if (pNode.distance > threshold) {
		// recurse
		this.recursiveSearchBelowThreshold(pNode.left, threshold);
		this.recursiveSearchBelowThreshold(pNode.right, threshold);
	    } else {
		// store the node
		this.clusters.put(node, new ArrayList<Integer>());
		return;
	    }
	}
	else if (node instanceof ChildNode) {
	    this.clusters.put(node, new ArrayList<Integer>());
	    return;
	}
    }

    
    /**
     * Recursively searches for the ChildNode in the
     * rootNode Tree. Adds the ChildNode id to the 
     * "clusters" field. This subroutine is used after
     * finding the ParentNodes (or clusters) that meet
     * the threshold cutoff.
     *
     * @params Node node
     * @params Node parent
     *
     */
    public void recursiveSearch(Node node, Node parent) {
	if (node instanceof ChildNode) {
	    // append child read no. to results
	    this.clusters.get(parent).add(node.hashCode());
	    return;
	} else {
	    ParentNode pNode = (ParentNode) node;
	    this.recursiveSearch(pNode.left, parent);
	    this.recursiveSearch(pNode.right, parent);
	}
    }

    /**
     * Constructs a complete linkage tree.
     * Naive method. No shortcuts.
     * 
     * @param String sortedFile
     */
    public void constructTree(String sortedFile) throws IOException {
	
	DoubleWritable key = new DoubleWritable();
	PointWritable value = new PointWritable(new Point(0,0));
	
	this.conf = new Configuration();
	this.fs = FileSystem.get(this.conf);
	this.fslocal = FileSystem.getLocal(this.conf);
	Path in = new Path(sortedFile);
	Path out;
	int i = 0;
	while (this.counter > 1) {
	    System.out.println(this.counter.toString());
	    out = new Path("temp-" + this.counter.toString() + ".bin");
	    this.searchSmallestValue(in);
	    this.addToTree();
	    this.updateValues(in, out);
	    // remove the temporary in files
	    // except the original sort file
	    if (i > 0) {
		this.fslocal.delete(in, false);
	    }
	    in = out;
	    i++;
	}
	
	this.searchSmallestValue(in);
	this.addToTree();
	this.rootNode = this.currentPNode;
    }

    public void searchSmallestValue(Path path) throws IOException {
	
	SequenceFile.Reader sfr = new SequenceFile.Reader(this.fslocal, 
							  path, this.conf);

	Point p1 = new Point(0,0);
	Double d1 = 20.0;
	DoubleWritable key = new DoubleWritable();
	PointWritable value = new PointWritable(new Point(0,0));

	// Search through file from top to bottom,
	// the smallest distance
	while (sfr.next(key, value)) {
	    Double d2 = key.get();
	    Point p2 = value.get();
	    if (d2 < d1) {
		d1 = d2;
		p1 = p2;
	    }
	}

	this.distance = d1;
	this.point = p1;
    }
    
    public void addToTree() {

	// Instantiate a new Parent Node with the distance
	ParentNode pNode = new ParentNode(this.parentNodeKeyStart,
					  this.distance);
	Boolean bothChild = true;

	// Append the left node to the Parent
	// Remove the node if its a ParentNode
	Node n1 = this.index.get(this.point.getRead1());
	Node node1;
	if ((n1 != null) && (n1 instanceof ParentNode)) {
	    node1 = n1;
	    this.index.remove(this.point.getRead1());
	} 
	else {
	    node1 = new ChildNode(this.point.getRead1());
	}

	// Append the right node to the Parent
	// Remove the node if its a ParentNode
	Node n2 = this.index.get(this.point.getRead2());
	Node node2;
	if ((n2 != null) && (n2 instanceof ParentNode)) {
	    node2 = n2;
	    this.index.remove(this.point.getRead2());
	} 
	else {
	    node2 = new ChildNode(this.point.getRead2());
	}

	// Append left and right node to parent and store in index
	// for safe keeping. Not to be squirreled away by the garbage
	// collector.
	pNode.left = node1;
	pNode.right = node2;
	this.index.put(this.parentNodeKeyStart, pNode);

	// Use this for storeValues()
	this.currentNode1 = node1;
	this.currentNode2 = node2;
	this.currentPNode = pNode;
	
	this.parentNodeKeyStart += 1;

    }

    public void updateValues(Path in, Path out) throws IOException {
	this.counter = 0;
	SequenceFile.Reader sfr = new SequenceFile.Reader(this.fslocal, 
							  in, this.conf);

	SequenceFile.Writer sfw = new SequenceFile.Writer(this.fslocal,
							  this.conf, out,
							  DoubleWritable.class,
							  PointWritable.class);

	DoubleWritable key = new DoubleWritable();
	PointWritable value = new PointWritable(new Point(0,0));
	while (sfr.next(key, value)) {
	    Double d = key.get();
	    Point p = value.get();
	    Boolean check = true;

	    // Complete Linkage is applied in the following
	    // lines of code
	    //
	    // Total match e.g. (1,3) == (1,3)
	    if (p.getRead1() == this.currentNode1.hashCode() &&
		p.getRead2() == this.currentNode2.hashCode()) {
		check = false;
	    } // Total match e.g. (1,3) == (3,1)
	    else if (p.getRead1() == this.currentNode2.hashCode() &&
		     p.getRead2() == this.currentNode1.hashCode()) {
		check = false;
	    }
	    else {

		// Partial match e.g. (1,3) == (1,6) == (6,1)
		//                    (1,3) == (3,6) == (6,3)
		if (p.getRead1() == this.currentNode1.hashCode() ||
		    p.getRead1() == this.currentNode2.hashCode()) {
		    Double dist = this.storedValues.get(p.getRead2());
		    if (dist != null) {
			if (d > dist) {
			    this.storedValues.put(p.getRead2(), d);
			    // write out
			    sfw.append(new DoubleWritable(d), 
				       new PointWritable(new Point(this.currentPNode.hashCode(),
								   p.getRead2())));
			    this.counter += 1;
			} else {
			    // write out
			    sfw.append(new DoubleWritable(dist), 
				       new PointWritable(new Point(this.currentPNode.hashCode(),
								   p.getRead2())));
			    this.counter += 1;
			}
		    } else {
			this.storedValues.put(p.getRead2(), d);
			// nothing to write out
		    }
		    check = false;
		}

		// Partial match e.g. (1,3) == (1,6) == (6,1)
		//                    (1,3) == (3,6) == (6,3)
		if (p.getRead2() == this.currentNode1.hashCode() ||
		    p.getRead2() == this.currentNode2.hashCode()) {
		    Double dist = this.storedValues.get(p.getRead1());
		    if (dist != null) {
			if (d > dist) {
			    this.storedValues.put(p.getRead1(), d);
			    // write out
			    sfw.append(new DoubleWritable(d),
				       new PointWritable(new Point(this.currentPNode.hashCode(),
								   p.getRead1())));
			    this.counter += 1;
			} else {
			    // write out
			    sfw.append(new DoubleWritable(dist),
				       new PointWritable(new Point(this.currentPNode.hashCode(),
								   p.getRead1())));
			    this.counter += 1;
			}
		    } else {
			this.storedValues.put(p.getRead1(), d);
			// nothing to write out
		    }
		    check = false;
		}
	    
	    }

	    // No match and therefore no need to update
	    if (check) {
		// if both condition fails, check == True and write out
		sfw.append(key, value);
		this.counter += 1;
	    }
	}
	this.storedValues = new HashMap<Integer, Double>();

	sfr.close();
	sfw.close();
    }

}