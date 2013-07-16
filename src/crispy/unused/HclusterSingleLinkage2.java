package crispy.hcluster;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.DoubleWritable;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.Set;
import java.util.Map;
import java.io.IOException;

import crispy.io.Point;
import crispy.io.PointWritable;

public class HclusterSingleLinkage {

    Integer size = 1000;
    Integer newKeyStart = size;
    Integer newKeyEnd = size + size - 2;
    TreeMap<Integer, Node> cluster = new TreeMap<Integer, Node>();

    public static void main(String[] args) throws IOException {
	HclusterSingleLinkage hcsl = new HclusterSingleLinkage();
	hcsl.clusterFullMatrix();
	//for (Map.Entry<Integer, Node> d : hcsl.cluster.entrySet()) {
	//    Integer key = d.getKey();
	//    Node value = d.getValue();
	//    System.out.println(key.toString() + ", " + 
	//		       value.index.toString() + ", " + 
	//		       value.distance.toString()); 
	//}
    }

    public void clusterFullMatrix() throws IOException {
	
	Configuration conf = new Configuration();
	FileSystem fs = FileSystem.get(conf);
	Path path = new Path("/data/chenga/projects/crispy-hadoop-0.20/test/sorted.bin");
	SequenceFile.Reader sfr = new SequenceFile.Reader(fs, path, conf);

	DoubleWritable key = new DoubleWritable();
	PointWritable value = new PointWritable(new Point(0, 0));

	while (sfr.next(key, value)) {
	    Point p = value.get();
	    Node newNode = new Node(newKeyStart, key.get());
	    // check for value
	    // recursively check for node till its absent and return
	    // if both nodes are different, add newNode
	    // otherwise, continue with the next nearest value
	    Integer node1 = this.recursiveCheckNode(p.getRead1());
	    Integer node2 = this.recursiveCheckNode(p.getRead2());
	    if (node1 != node2) {
		this.cluster.put(node1, newNode);
		this.cluster.put(node2, newNode);
		
		// End Cluster condition
		if (newKeyStart == newKeyEnd) {
		    Node lastNode = new Node(-1, key.get());
		    this.cluster.put(newKeyStart, lastNode);
		    break;
		}

		newKeyStart += 1;
		System.out.println(newKeyStart.toString());
	    }
	
	}

    }

    public Integer recursiveCheckNode(Integer nodeIndex) {
	Node node = this.cluster.get(nodeIndex);
	if (node == null) {
	    // if absent, return that nodeIndex for comparison before update
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