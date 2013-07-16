package crispy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import crispy.IntArrayWritable;

public class EnumerateSortKmerMapper extends Mapper<LongWritable, Text, Text, ArrayWritable> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	// parse input-line
	String line = value.toString();
	String[] h = line.replaceAll("\n","").split("\t");
	try {
	    Integer[] kmers = EnumerateSortKmerMapper.enumerateKmers(h[1], 6);
	    EnumerateSortKmerMapper.sortKmers(kmers);

	    IntWritable[] kmersIntWritable = new IntWritable[kmers.length];

	    for (int i = 0; i < kmers.length; i++) {
		kmersIntWritable[i] = new IntWritable(kmers[i]);
	    }

	    IntArrayWritable out = new IntArrayWritable(kmersIntWritable);

	    context.write(new Text(h[0]), out);
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }

    public static Integer[] enumerateKmers(String s, int k) throws Exception {
	int length = s.length();
	ArrayList<Integer> kmers = new ArrayList<Integer>();
	if (k > length) {
	    throw new Exception("Length of read is shorter than k-mer threshold");
	}
	int limit = length - k + 1;
	for (int i = 0; i < limit; i++) {
	    kmers.add(s.substring(i, i + k).hashCode());
	}

	Integer[] j = new Integer[limit];
	return kmers.toArray(j);
    }

    public static void sortKmers(Integer[] kmers) throws Exception {
	Arrays.sort(kmers);
    }

}