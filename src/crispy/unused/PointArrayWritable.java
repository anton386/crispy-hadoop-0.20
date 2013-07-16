package crispy.hcluster;

import org.apache.hadoop.io.ArrayWritable;

import crispy.hcluster.PointWritable;

public class PointArrayWritable extends ArrayWritable {
    
    public PointArrayWritable() {
	super(PointWritable.class);
    }

}