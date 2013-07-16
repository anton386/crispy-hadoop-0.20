package crispy.kmerdist;

import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import crispy.kmerdist.KmerDist;
import crispy.io.MapArrayWritable;

public class KmerDistSeqFileMapper 
    extends MapReduceBase implements Mapper<IntWritable, MapArrayWritable, 
				     DoubleWritable, MapWritable> {

    HashMap<Integer, Integer[]> data = new HashMap<Integer, Integer[]>();
    HashMap<Integer, String> seqData = new HashMap<Integer, String>();
    ArrayList<Integer> readIndex = new ArrayList<Integer>();
    //TRMapWritable out = new MapWritable();
    Integer blockSize;
    Integer kmer;
    
    public void configure(JobConf job) {
	this.blockSize = Integer.parseInt(job.get("blockSize"));
	this.kmer = Integer.parseInt(job.get("kmer"));
    }

    /**
     * The KmerDistSeqFileMapper maps a block of reads
     * that are either a Square (e.g. 200 x 200) or
     * a Triangle (e.g. 2 x (200 x 199 / 2)).
     * It takes in an input with the following data structure
     * IntWritable key
     * MapArrayWritable value
     * [0]: {
     *   "type": "triangle"
     *   1: "ATCGTCGTAGATC"
     *   2: "GTCATAACTAGAT"
     * }
     * [1]: {
     *   "type": "triangle"
     *   201: "TCAGTATACTATAT"
     *   202: "ATACATAGAGTAAC"
     * }
     */
    public void map(IntWritable key, MapArrayWritable value, 
		    OutputCollector<DoubleWritable, MapWritable> output, 
		    Reporter reporter) throws IOException {
	
	for (Writable v : value.get()) {
	    // check for type
	    MapWritable m = (MapWritable) v;

	    if (this.isSquare(m)) {
		this.generateKmerIndex(m); // Generate KmerIndexData for fast comparison
		int size = this.readIndex.size();		

		for (int i = 0; i < this.blockSize; i++) {
		    Integer k = this.readIndex.get(i);
		    Integer[] read1 = this.data.get(k);
		    //TRMapWritable batch = new MapWritable();
		    //TRMapWritable dists = new MapWritable();
		    for (int j = this.blockSize; j < size; j++) {
			Integer l = this.readIndex.get(j);
			Integer[] read2 = this.data.get(l);
			
			// calculate KmerDist
			KmerDist kd = new KmerDist(read1, read2);
			Double distance = kd.execute();
			
			// NEW
			MapWritable info = new MapWritable();
			info.put(new Text("read1"), new Text(this.seqData.get(k)));
			info.put(new Text("read2"), new Text(this.seqData.get(l)));
			info.put(new Text("read1id"), new IntWritable(k));
			info.put(new Text("read2id"), new IntWritable(l));
			output.collect(new DoubleWritable(distance),
				       info);
			//TRdists.put(new IntWritable(l), new DoubleWritable(distance));
		    }
		    //TRbatch.put(new Text("seq"), new Text(this.seqData.get(k)));
		    //TRbatch.put(new Text("distances"), dists);
		    //TRthis.out.put(new IntWritable(k), batch);
		}

		//TRfor (int j = this.blockSize; j < size; j++) {
		//TR    Integer l = this.readIndex.get(j);
		//TR    MapWritable batch = new MapWritable();
		//TR    MapWritable dists = new MapWritable();
		//TR    batch.put(new Text("seq"), new Text(this.seqData.get(l)));
		//TR    batch.put(new Text("distances"), dists);
		//TR    this.out.put(new IntWritable(l), batch);
		//TR}

		//TRoutput.collect(key, this.out);

		this.data = new HashMap<Integer, Integer[]>();
		this.seqData = new HashMap<Integer, String>();
		this.readIndex = new ArrayList<Integer>();
		//TRthis.out = new MapWritable();
	    }
	    else if (this.isTriangle(m)) {
		this.generateKmerIndex(m); // Generate KmerIndexData for fast comparison
		int size = this.readIndex.size();
		
		for (int i = 0; i < size; i++) {
		    Integer k = this.readIndex.get(i);
		    Integer[] read1 = this.data.get(k);
		    //TRMapWritable batch = new MapWritable();
		    //TRMapWritable dists = new MapWritable();
		    for (int j = 0; j < i; j++) {
			if (i != j) {
			    Integer l = this.readIndex.get(j);
			    Integer[] read2 = this.data.get(l);
			
			    // calculate KmerDist
			    KmerDist kd = new KmerDist(read1, read2);
			    Double distance = kd.execute();

			    // NEW
			    MapWritable info = new MapWritable();
			    info.put(new Text("read1"), new Text(this.seqData.get(k)));
			    info.put(new Text("read2"), new Text(this.seqData.get(l)));
			    info.put(new Text("read1id"), new IntWritable(k));
			    info.put(new Text("read2id"), new IntWritable(l));
			    output.collect(new DoubleWritable(distance),
					   info);
			    //TRdists.put(new IntWritable(l), new DoubleWritable(distance));
			}
		    }
		    //TRbatch.put(new Text("seq"), new Text(this.seqData.get(k)));
		    //TRbatch.put(new Text("distances"), dists);
		    //TRthis.out.put(new IntWritable(k), batch);
		}

		//TRoutput.collect(key, this.out);

		this.data = new HashMap<Integer, Integer[]>();
		this.seqData = new HashMap<Integer, String>();
		this.readIndex = new ArrayList<Integer>();
		//TRthis.out = new MapWritable();
	    }

	}
    }

    public void generateKmerIndex(MapWritable m) {
	try {
	    Set<Map.Entry<Writable, Writable>> entries;
	    m.remove(new Text("type"));
	    entries = m.entrySet();
	    for (Map.Entry<Writable, Writable> e : entries) {
	    
		// Covert to the appropriate Writable types
		IntWritable nk = (IntWritable) e.getKey();
		Text nv = (Text) e.getValue();

		// Calculate Kmer and Sort
		Integer[] kmerData = enumerateKmers(nv.toString(), this.kmer);
		sortKmers(kmerData);
		// Store
		this.data.put(nk.get(), kmerData);
		this.seqData.put(nk.get(), nv.toString());
		this.readIndex.add(nk.get());
		Collections.sort(this.readIndex);
	    }
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }

    public boolean isSquare(MapWritable m) {
	if (m.get(new Text("type")).equals(new Text("square"))) {
	    return true;
	} else {
	    return false;
	}
    }

    public boolean isTriangle(MapWritable m) {
	if (m.get(new Text("type")).equals(new Text("triangle"))) {
	    return true;
	} else {
	    return false;
	}
    }

    public static Integer[] enumerateKmers(String s, Integer k) throws Exception {
	Integer length = s.length();
	ArrayList<Integer> kmers = new ArrayList<Integer>();
	if (k > length) {
	    throw new Exception("Length of read is shorter than k-mer threshold");
	}
	Integer limit = length - k + 1;
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