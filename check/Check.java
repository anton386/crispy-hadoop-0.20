import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import crispy.io.PointWritable;
import crispy.io.Point;

public class Check {
    public static void main(String[] args) throws IOException {
	Configuration conf = new Configuration();
	SequenceFile.Reader sfr = new SequenceFile.Reader(FileSystem.get(conf), new Path(args[0]), conf);
	
	BufferedWriter out = new BufferedWriter(new FileWriter(args[1]));
	DoubleWritable key = new DoubleWritable();
	PointWritable value = new PointWritable(new Point(0,0));
	while (sfr.next(key, value)) {
	    out.write(value.get().getRead1().toString() + "\t" +  
		      value.get().getRead2().toString() + "\t" +
		      key.toString());
	    out.newLine();
	}
	out.close();
    }

}