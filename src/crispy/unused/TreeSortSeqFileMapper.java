package crispy.hcluster;

import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import java.util.Map;
import java.util.TreeMap;
import java.util.ArrayList;
import java.util.Collections;
import java.io.IOException;

import crispy.hcluster.Point;
import crispy.hcluster.PointWritable;
import crispy.hcluster.PointArrayWritable;

public class TreeSortSeqFileMapper
    extends MapReduceBase implements Mapper<IntWritable, MapWritable,
				     IntWritable, PointArrayWritable> {

    ArrayList<Point> tree = new ArrayList<Point>();
    PointArrayWritable paw = new PointArrayWritable();
    Integer counter = 0;

    public void map(IntWritable key, MapWritable value,
		    OutputCollector<IntWritable, PointArrayWritable> output,
		    Reporter context) throws IOException {

	for (Map.Entry<Writable, Writable> m1 : value.entrySet()) {
	    IntWritable r1 = (IntWritable) m1.getKey();
	    MapWritable v1 = (MapWritable) m1.getValue();
	    MapWritable d1 = (MapWritable) v1.get(new Text("distances"));
	    for (Map.Entry<Writable, Writable> m2 : d1.entrySet()) {
		IntWritable r2 = (IntWritable) m2.getKey();
		DoubleWritable v2 = (DoubleWritable) m2.getValue();
		Point p = new Point(v2.get(), r1.get(), r2.get());
		this.tree.add(p);
	    }
	}

	Collections.sort(this.tree);
	Integer size = this.tree.size();
	PointWritable[] temp = new PointWritable[size];

	for (Integer i = 0; i < size; i++) {
	    temp[i] = new PointWritable(this.tree.get(i));
	}

	this.paw.set(temp);

	output.collect(new IntWritable(this.counter), this.paw);
	
	this.tree = new ArrayList<Point>();
	this.paw = new PointArrayWritable();
	this.counter += 1;
    }
}
