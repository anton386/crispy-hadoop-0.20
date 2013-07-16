package crispy.io;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.MapWritable;

public class MapArrayWritable extends ArrayWritable {

    public MapArrayWritable() {
	super(MapWritable.class);
    }

}