import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import crispy.io.MapArrayWritable;

public class CheckHDFSSequenceFile {
    public static void main(String[] args) throws IOException {
	Configuration conf = new Configuration();
	FileSystem fs = FileSystem.get(conf);
	Path path = new Path(args[0]);
	SequenceFile.Reader sfr = new SequenceFile.Reader(fs, path, conf);

	BufferedWriter out = new BufferedWriter(new FileWriter(args[1]));
	IntWritable key = new IntWritable();
	MapArrayWritable value = new MapArrayWritable();
	while (sfr.next(key, value)) {
	    for (Writable v : value.get()) {
		MapWritable mw = (MapWritable) v;
		mw.remove(new Text("type"));
		for (Map.Entry<Writable, Writable> m : mw.entrySet()) {
		    IntWritable iwKey = (IntWritable) m.getKey();
		    Text tValue = (Text) m.getValue();
		    out.write(iwKey.toString() + "\t" + tValue.toString());
		    out.newLine();
		}
	    }
	}
	out.close();
	
    }
}