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
import java.util.HashSet;
import java.util.Iterator;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.IOException;

import crispy.io.Point;
import crispy.io.PointWritable;
import crispy.io.Node;
import crispy.io.ParentNode;
import crispy.io.ChildNode;

public class HclusterSingleLinkage {

    Node rootNode = null;
    HashMap<Node, Node> index = new HashMap<Node, Node>();
    HashMap<Node, ArrayList<Integer>> clusters = new HashMap<Node, ArrayList<Integer>>();
    HashSet<Node> parents = new HashSet<Node>();

    Integer size = 0;
    Integer parentNodeKeyStart = 0;
    Integer endConditionSize = 0;
    Integer counter = 0;
    Integer minClusterSize = 4;

    public static void main(String[] args) throws IOException {
	HclusterSingleLinkage hcsl = new HclusterSingleLinkage(Integer.parseInt(args[3]));
	hcsl.constructTree(args[0]);
	hcsl.search(Double.parseDouble(args[1]));
	hcsl.writeClustersToFile(args[2]);
    }

    public HclusterSingleLinkage(Integer size) {
	this.size = size;
	this.parentNodeKeyStart = size;
	this.endConditionSize = (size * 2) - 2;
    }

    public HclusterSingleLinkage(Integer size, Integer minClusterSize) {
	this.size = size;
	this.parentNodeKeyStart = size;
	this.endConditionSize = (size * 2) - 2;
	this.minClusterSize = minClusterSize;
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
	Iterator<Node> parent = this.parents.iterator();
	Node n = null;
	while (parent.hasNext()) {
	    n = parent.next();
	    this.recursiveSearchBelowThreshold(n, threshold);
	}

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
		if (m.getValue().size() >= this.minClusterSize) {
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
	    }
	    else if (m.getKey() instanceof ChildNode) {
		if (m.getValue().size() >= this.minClusterSize) {
		    Integer cluster = m.getKey().hashCode();
		    String placeholder = "%d | * | %d";
		    output = String.format(placeholder, cluster, cluster);
		    out.write(output);
		    out.newLine();
		}
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
     * Constructs a single linkage tree. 
     * First recursively searches for the latest parent node 
     * that it belongs to in a cluster.
     * Secondly, if both parent nodes are different, it adds a
     * new parent node. Otherwise, it continues with the next 
     * nearest value
     * 
     * @param String sortedFile
     */
    public void constructTree(String sortedFile) throws IOException {
	Configuration conf = new Configuration();
	FileSystem fs = FileSystem.get(conf);
	LocalFileSystem fslocal = FileSystem.getLocal(conf);
	Path path = new Path(sortedFile);
	SequenceFile.Reader sfr = new SequenceFile.Reader(fslocal, path, conf);
	
	DoubleWritable key = new DoubleWritable();
	PointWritable value = new PointWritable(new Point(0,0));
	
	while (sfr.next(key, value)) {
	    Double distance = key.get();
	    Point p = value.get();
	    
	    // iterate through every value
	    this.addParentNode(distance, p);
	    //Sif (!this.addParentNode(distance, p)) {
	    //S	break;
	    //S}
	}

	// construct list of parent nodes by searching index
	this.searchIndex();

    }

    public void searchIndex() {
	// use recursiveCheckNode() to find
	for (Map.Entry<Node, Node> m : this.index.entrySet()) {
	    Node key = m.getKey();
	    Node value = m.getValue();
	    if (key instanceof ChildNode) {
		Node parent = this.recursiveCheckNode(key);
		if (!this.parents.contains(parent)) {
		    this.parents.add(parent);
		}
	    }
	}
    }


    /**
     * Adds the point currently with the smallest distance
     * as a new parent node into the Tree.
     *
     * @params Double distance
     * @params Point p
     *
     */
    public boolean addParentNode(Double distance, Point p) {
	
	ChildNode cNode1 = new ChildNode(p.getRead1());
	ChildNode cNode2 = new ChildNode(p.getRead2());
	Node node1 = this.recursiveCheckNode(cNode1);
	Node node2 = this.recursiveCheckNode(cNode2);

	if (!node1.equals(node2)) {

	    ParentNode pNode = new ParentNode(this.parentNodeKeyStart, distance);
	    pNode.left = node1;
	    pNode.right = node2;
	    
	    this.index.put(node1, pNode);
	    this.index.put(node2, pNode);

	    this.counter += 1;

	    //Sif (this.index.size() == this.endConditionSize) { // End Cluster condition
	    //S	this.rootNode = pNode;
	    //S	return false;
	    //S}
	    
	    //DEBUG System.out.println(node1.hashCode());
	    //DEBUG System.out.println(node2.hashCode());
	    //DEBUG System.out.println(this.parentNodeKeyStart + ", " + distance.toString());
	    //DEBUG System.out.println();

	    this.parentNodeKeyStart += 1;
	}
	return true;
    }


    /**
     * Recursively searches for the ParentNode with 
     * the largest distance. This subroutine is used
     * to construct the Hcluster Tree. Input is either
     * a ParentNode or a ChildNode. The return value
     * is a ParentNode.
     *
     * @params Node node
     * @return Node node
     *
     */
    public Node recursiveCheckNode(Node node) {
	Node check = this.index.get(node);
	if (check == null) {
	    // if absent, return that node for comparison before updating
	    return node;
	} else {
	    // if present, recursively try again
	    return this.recursiveCheckNode(check);
	}
    }

}