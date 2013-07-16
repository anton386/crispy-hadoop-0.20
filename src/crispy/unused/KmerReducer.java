package crispy;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import crispy.IntArrayWritable;

public class KmerReducer extends Reducer<IntArrayWritable, Text, Text, Text> {

    public void reduce (IntArrayWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException{
	
	Writable[] l = key.get();
	IntWritable m = (IntWritable) l[0];
	IntWritable n = (IntWritable) l[1];
	String k = "(" + m.toString() + "," + n.toString() + ")";

	String s = "";
	for (Text t: value) {
	    s = t.toString();
	}
	
	context.write(new Text(k), new Text(s));   
    }

}