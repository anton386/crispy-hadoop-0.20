package crispy.partition;

import java.io.RandomAccessFile;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;

import crispy.io.MapArrayWritable;

public class SequenceFileBuilder {

    Read r;
    Integer lineCount = 0;
    Integer readCount = 0;
    Integer iteration = 0;
    Integer readCountPerBlock = 0;
    Integer readsPerBlock = 100000;
    Integer currentBlockStart = 0;
    String inputFastq = "";
    String outputIndex = "";
    String outputSequence = "";
    RandomAccessFile raf;
    BufferedWriter index;
    SequenceFile.Writer sequencefile;
    ArrayList<Read> block = new ArrayList<Read>();
    ArrayList<Read> refBlock = new ArrayList<Read>();
    ArrayList<ArrayList<Read>> triangle = new ArrayList<ArrayList<Read>>();

    Integer squaresCount = 1;
    public Integer totalSize = 0;

    public static void main(String[] args) {
	SequenceFileBuilder sfb = new SequenceFileBuilder(args[0], args[1], 
							  args[2], Integer.parseInt(args[3]));
    }

    /**
     * Constructor for SequenceFileBuilder
     * Will construct the object properties,
     * examine the file line by line
     * and write out the Sequence
     *
     * @param inputFastq    Filename in the local file system
     * @param readsPerBlock The maximum number of reads per logical block.
     *                      May or may not exceed the Hadoop Block of 64MB
     */
    public SequenceFileBuilder(String inputFastq, String outputIndexToReadID,
			       String outputSequenceFile, Integer readsPerBlock) {
	this.readsPerBlock = readsPerBlock;
	this.inputFastq = inputFastq;
	this.outputIndex = outputIndexToReadID;
	this.outputSequence = outputSequenceFile;
	this.execute();
    }

    /**
     * Execute. Goes to the block's starting line
     * before executing executePerBlock().
     *
     */
    public void execute() {
	try {
	    this.openFastqFile();
	    this.openIndexFile();
	    this.openSequenceFile();

	    String in = this.raf.readLine();
	    while (in != null) {
		    
		if (this.lineCount % 4 == 0 
		    && this.readCount.equals(this.currentBlockStart)) {
		    
		    this.executePerBlock(in);
		    this.currentBlockStart += this.readsPerBlock;

		    this.lineCount = 0; // zero-based, resets values
		    this.readCount = 0; // zero-based, resets values
		    this.iteration += 1; // increment num of iteration
		    in = this.raf.readLine();
		} else {  // runs through the lines already read
		    if (this.lineCount % 4 == 1) {
			this.readCount += 1;
		    }

		    this.lineCount += 1;
		    in = this.raf.readLine();
		}
	    }

	    this.raf.close();
	    this.index.close();
	    this.sequencefile.close();
	} catch (FileNotFoundException e) {
	    e.printStackTrace();
	} catch (IOException e) {
	    e.printStackTrace();
	}
	

    }

    /**
     * Iterates through the block line by line.
     * Ends when the end of file is reached.
     * Most of the heavy lifting is done here.
     *
     * @return void
     */
    public void executePerBlock(String in) throws FileNotFoundException, IOException{
	boolean pass = false;

	while (in != null) {

	    // check each line in file whether its an ID 
	    if (this.isReadID(this.lineCount)) {
		this.r = new Read();
		this.r.readID = this.getReadID(in);
		this.r.id = this.readCount;
		this.writeToIndexToReadID(this.r.id, this.r.readID);
	    } // or if its a Sequence
	    else if (this.isReadSequence(this.lineCount)) {
		this.r.readSequence = this.getReadSequence(in);
		this.block.add(r);
		this.readCount += 1;
		this.readCountPerBlock += 1;
		this.countTotalSize();
	    }
	    else if (this.lineCount % 4 == 3) {
		// when readCount reaches threshold, 
		// start appending to the next data structure
		if ((this.readCount % this.readsPerBlock) == 0) {
		    this.readCountPerBlock = 0; // reset this value

		    if (this.atEndOfCurrentWorkingBlock()) {
			// mark
			this.refBlock = this.block;
			this.createTriangle(false);
			this.block = new ArrayList<Read>();
			pass = true;
		    } else {
			this.createSquare();
			this.block = new ArrayList<Read>();
		    }

		    //DEBUG System.out.println(this.iteration.toString() + " " +
		    //DEBUG		       this.readCountPerBlock.toString());

		}
	    }

	    // increment the number of lines that has been read
	    this.lineCount += 1;
	    in = this.raf.readLine();

	}

	
	//DEBUG System.out.println(this.iteration.toString() + " " +
	//DEBUG 		   this.readCountPerBlock.toString());

	// write out the whatever that didn't come out
	if (in == null && pass == true && this.readCountPerBlock > 0) {
	    this.createSquare();
	    this.block = new ArrayList<Read>();
	    this.readCountPerBlock = 0; // Fix
	}
	
	if (in == null && pass == false && this.readCountPerBlock < this.readsPerBlock) {
	    this.createTriangle(true);
	}

	this.raf.seek(0);
    }

    /**
     * Writes the file that consists of the index
     * to the read id. Uses the first iteration
     * to obtain the records.
     *
     * @return void
     */
    public void writeToIndexToReadID(Integer id, String readID) throws IOException {
	if (this.iteration == 0) {
	    this.index.write(id.toString() + "\t" + readID);
	    this.index.newLine();
	}
    }

    /**
     * Counts the total number of reads in the
     * fastq file. Uses the first iteration
     * to obtain the records.
     *
     * @return void
     */
    public void countTotalSize() {
	if (this.iteration == 0) {
	    this.totalSize += 1;
	}
    }


    /**
     * Stores the first n reads in the block.
     * When the block slots in the triangle
     * is full, it will enumerate a pairwise
     * combination of the reads and write to
     * the sequence file.
     *
     * @return void
     */
    public void createTriangle(boolean last) throws FileNotFoundException, IOException{
	this.triangle.add(this.block);

	if (this.triangle.size() == 1 && last == true) {
	    IntWritable key = new IntWritable();
	    MapArrayWritable value = new MapArrayWritable();
	    MapWritable[] mapArray = new MapWritable[1];

	    for (int i = 0; i < 1; i++) {
		MapWritable map = new MapWritable();
		// place type into MapWritable first
		map.put(new Text("type"), new Text("triangle"));

		Iterator<Read> iter = this.triangle.get(i).iterator();

		while (iter.hasNext()) {
		    Read read = iter.next();
		    map.put(new IntWritable(read.id), new Text(read.readSequence));
		}
		mapArray[i] = map;
	    }

	    key.set(this.squaresCount);
	    value.set(mapArray);
	    this.sequencefile.append(key, value);
	    this.triangle = new ArrayList<ArrayList<Read>>();
	    this.squaresCount += 1;
	}
	else if (this.triangle.size() == 2 || last == true) {
	    IntWritable key = new IntWritable();
	    MapArrayWritable value = new MapArrayWritable();
	    MapWritable[] mapArray = new MapWritable[2];

	    for (int i = 0; i < 2; i++) {
		MapWritable map = new MapWritable();
		// place type into MapWritable first
		map.put(new Text("type"), new Text("triangle"));

		Iterator<Read> iter = this.triangle.get(i).iterator();

		while (iter.hasNext()) {
		    Read read = iter.next();
		    map.put(new IntWritable(read.id), new Text(read.readSequence));
		}
		mapArray[i] = map;
	    }

	    key.set(this.squaresCount);
	    value.set(mapArray);
	    this.sequencefile.append(key, value);
	    this.triangle = new ArrayList<ArrayList<Read>>();
	    this.squaresCount += 1;
	}
	
    }

    /**
     * Simply enumerates the pairwise combinations
     * in the refBlock and the current block. And
     * writes it to the sequence file.
     *
     * @return void
     */
    public void createSquare() throws FileNotFoundException, IOException {
	IntWritable key = new IntWritable();
	MapArrayWritable value = new MapArrayWritable();
	MapWritable[] mapArray = new MapWritable[1];

	for (int i = 0; i < 1; i++) {
	    MapWritable map = new MapWritable();
	    // place type into MapWritable first
	    map.put(new Text("type"), new Text("square"));

	    Iterator<Read> i1 = this.refBlock.iterator();
	    Iterator<Read> i2 = this.block.iterator();

	    while (i1.hasNext()) {
		Read read = i1.next();
		map.put(new IntWritable(read.id), new Text(read.readSequence));
	    }

	    while (i2.hasNext()) {
		Read read = i2.next();
		map.put(new IntWritable(read.id), new Text(read.readSequence));
	    }
	    mapArray[i] = map;
	}
	
	key.set(this.squaresCount);
	value.set(mapArray);
	this.sequencefile.append(key, value);
	this.squaresCount += 1;
    }


    /**
     * Are we at the end of the current working block?
     * Current working block is defined as the cumulative
     * count of the readsPerBlock or any partial end
     *
     * @return boolean atEndOfCurrentWorkingBlock
     */
    public boolean atEndOfCurrentWorkingBlock() {

	if (this.readCount.equals(this.currentBlockStart
				  + this.readsPerBlock)) {
	    return true;
	} else {
	    return false;
	}
    }

    /**
     * Opens File and assigns to object this.raf
     *
     * @return void
     */
    public void openFastqFile() throws FileNotFoundException {
	this.raf = new RandomAccessFile(this.inputFastq, "r");
    }


    /**
     * Opens File and assigns to object this.index
     *
     * @return void
     */
    public void openIndexFile() throws FileNotFoundException, IOException {
	this.index = new BufferedWriter(new FileWriter(this.outputIndex));
    }

    public void openSequenceFile() throws IOException {
	Configuration conf = new Configuration();
	FileSystem fs = FileSystem.get(conf);
	Path path = new Path(this.outputSequence);
	IntWritable key = new IntWritable();
	MapArrayWritable value = new MapArrayWritable();
	
	this.sequencefile = SequenceFile.createWriter(fs, conf, path, 
						      key.getClass(),
						      value.getClass());
    }

    /**
     * Determines whether the line is a FASTQ read id
     *
     * @param int lineNumber
     * @return boolean isReadID
     */
    public boolean isReadID(int lineNumber) {
	if (lineNumber % 4 == 0) {
	    return true;
	} else {
	    return false;
	}
    }

    /**
     * Determines whether the line is a read sequence
     *
     * @param int lineNumber
     * @return boolean isReadSequence
     */
    public boolean isReadSequence(int lineNumber) {
	if (lineNumber % 4 == 1) {
	    return true;
	} else {
	    return false;
	}
    }

    /**
     * Parses the line for the read id
     *
     * @param String line
     * @return String readID
     */
    public String getReadID(String line) {
	return line.replace("\n", "").substring(1);
    }

    /**
     * Parses the line for the read sequence
     *
     * @param String line
     * @return String readSequence
     */
    public String getReadSequence(String line) {
	return line.replace("\n", "").toUpperCase();
    }
}

class Read {
    Integer id;
    String readID;
    String readSequence;
}