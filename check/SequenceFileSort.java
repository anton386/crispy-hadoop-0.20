import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;

import crispy.io.PointWritable;

public class SequenceFileSort {
    public static void main (String[] args) throws IOException {
	Configuration conf = new Configuration();
	FileSystem fs = FileSystem.get(conf);
	LocalFileSystem fslocal = FileSystem.getLocal(conf);
	SequenceFile.Sorter sfsort = new SequenceFile.Sorter(fslocal,DoubleWritable.class,
							     PointWritable.class, conf);
	
	Path outputSort = new Path(args[0]);
	Path[] outputCopy = new Path[Integer.parseInt(args[1])];
	for (int i = 0; i < Integer.parseInt(args[1]); i++) {
	    outputCopy[i] = new Path(args[2+i]);
	}
	sfsort.sort(outputCopy, outputSort, false);
    }
}
