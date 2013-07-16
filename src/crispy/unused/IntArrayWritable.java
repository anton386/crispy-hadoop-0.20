package crispy;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Writable;

public class IntArrayWritable extends ArrayWritable implements WritableComparable {
    
    public IntArrayWritable() {
        super(IntWritable.class);
    }
    public IntArrayWritable(IntWritable[] values) {
        super(IntWritable.class, values);
    }

    public int compareTo(Object o) {
	IntArrayWritable new_o = (IntArrayWritable) o;
	Writable[] thisValue = ((IntArrayWritable)this).get();
	Writable[] thatValue = ((IntArrayWritable)new_o).get();
	int a = 0;
	for (int i = 0; i < thisValue.length; i++) {
	    Integer counter = 0;
	    IntWritable j = (IntWritable) thisValue[i];
	    IntWritable k = (IntWritable) thatValue[i];
	    Integer l = j.get();
	    Integer m = k.get();
	    if (l < m) {
		a = -1;
		break;
	    } else if (l > m) {
		a = 1;
		break;
	    } else {
		if (counter == (thisValue.length-1)) {
		    a = 0;
		}
	    } 
	    counter++;
	}
	return a;
    }
}