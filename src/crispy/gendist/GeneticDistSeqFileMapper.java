package crispy.gendist;

import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.util.Set;
import java.util.Map;

import crispy.gendist.GeneticDist;
import crispy.io.Point;
import crispy.io.PointWritable;
import crispy.io.MapArrayWritable;

public class GeneticDistSeqFileMapper 
    extends MapReduceBase implements Mapper<DoubleWritable, MapWritable, 
				     DoubleWritable, PointWritable> {
    
    Double threshold = 0.50;
    Double width = 0.25;
    Double match = 5.00;
    Double mismatch = -4.00;
    Double gapOpen = -10.00;
    Double gapE = -5.00;

    public void configure(JobConf job) {
	this.threshold = Double.parseDouble(job.get("kmerDistThreshold"));
	this.width = Double.parseDouble(job.get("bandedK"));
	this.match = Double.parseDouble(job.get("match"));
	this.mismatch = Double.parseDouble(job.get("mismatch"));
	this.gapOpen = Double.parseDouble(job.get("gapOpen"));
	this.gapE = Double.parseDouble(job.get("gapExtension"));
    }

    public void map(DoubleWritable key, MapWritable value, 
		    OutputCollector<DoubleWritable, PointWritable> output, 
		    Reporter context) throws IOException {

	// Outer Loop - for each Read
	//for (Map.Entry<Writable, Writable> m1 : value.entrySet()) {
	//    IntWritable r1 = (IntWritable) m1.getKey();
	//    MapWritable v1 = (MapWritable) m1.getValue();
	//    MapWritable d1 = (MapWritable) v1.get(new Text("distances"));
	    
	    // Inner Loop - loop through every other Read
	//    for (Map.Entry<Writable, Writable> m2 : d1.entrySet()) {
	//	IntWritable r2 = (IntWritable) m2.getKey();
	//	DoubleWritable v2 = (DoubleWritable) m2.getValue();

		// If the pairwise KmerDist is less than the threshold,
		// refine the metric by using GeneticDist.
	//	if (v2.get() < this.threshold) {
	//	    String read1 = v1.get(new Text("seq")).toString();

	//	    MapWritable m3 = (MapWritable) value.get(r2);
	//	    String read2 = m3.get(new Text("seq")).toString();
	
		    // calculate GeneticDist
	//	    GeneticDist gd = new GeneticDist();
	//	    Double distance = gd.execute(read1, read2);
		    
		    //m2.setValue(new DoubleWritable(distance));
	//	    output.collect(new DoubleWritable(distance), 
	//			   new PointWritable(new Point(r1.get(), r2.get())));
	//	} else { // should create as null and treat a sparse matrix
		    //m2.setValue(new DoubleWritable(1.00));
	//	    output.collect(new DoubleWritable(1.00), 
	//			   new PointWritable(new Point(r1.get(), r2.get())));
	//	}
	//    }
	//}

	String read1 = value.get(new Text("read1")).toString();
	String read2 = value.get(new Text("read2")).toString();
	IntWritable r1 = (IntWritable) value.get(new Text("read1id"));
	IntWritable r2 = (IntWritable) value.get(new Text("read2id"));
	if (key.get() < this.threshold) {
	    GeneticDist gd = new GeneticDist();
	    Double distance = gd.execute(read1, read2, this.width,
					 this.match, this.mismatch,
					 this.gapOpen, this.gapE);

	    output.collect(new DoubleWritable(distance),
			   new PointWritable(new Point(r1.get(), r2.get())));
	} 
	//else {
	//    output.collect(new DoubleWritable(1.00),
	//		   new PointWritable(new Point(r1.get(), r2.get())));
	//}

    }
}