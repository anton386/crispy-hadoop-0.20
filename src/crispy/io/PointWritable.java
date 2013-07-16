package crispy.io;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import crispy.io.Point;

public class PointWritable implements Writable {

    Integer read1;
    Integer read2;
    
    public PointWritable(Point p) {
	this.read1 = p.read1;
	this.read2 = p.read2;
    }

    public void write(DataOutput out) throws IOException {
	out.writeInt(this.read1);
	out.writeInt(this.read2);
    }

    public void readFields(DataInput in) throws IOException {
	this.read1 = in.readInt();
	this.read2 = in.readInt();
    }

    public Point get() {
	Point p = new Point(this.read1, this.read2);
	return p;
    }
}