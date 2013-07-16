package crispy.test;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import crispy.IntArrayWritable;

public class TestMapper extends Mapper<Text, IntArrayWritable, LongWritable, IntWritable> {
    public void map (Text key, IntArrayWritable value, Context context) throws IOException, InterruptedException {
	long counter = 0;
	Writable[] k = value.get();
	for (Writable l: k) {
	    context.write(new LongWritable(counter), (IntWritable) l);
	    counter++;
	}
    }
}