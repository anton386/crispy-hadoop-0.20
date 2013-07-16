package crispy.io;

public class Node {

    Integer id;

    public Node(Integer id) {
	this.id = id;
    }
    
    /*
     * Gives an id to the parent node object.
     * Parent id is zero-based and starts after
     * this Child id.
     *
     * Gives an id to the child node object.
     * Child id is zero-based and is assigned by its
     * original index id.
     * 
     * @Override
     * @return int
     *
     */
    public int hashCode() {
	return this.id;
    }

    public boolean equals(Object node) {
	boolean test = false;
	if (this.hashCode() == node.hashCode()) {
	    test = true;
	} else {
	    test = false;
	}
	return test;
    }

}