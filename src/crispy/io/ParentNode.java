package crispy.io;

import crispy.io.Node;

public class ParentNode extends Node {
    
    public Node left;
    public Node right;
    public Double distance;

    public ParentNode(Integer id, Double distance) {
	super(id); // inherit from Node
	this.distance = distance;
    }

}