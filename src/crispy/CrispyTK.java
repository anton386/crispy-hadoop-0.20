package crispy;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.FileOutputStream;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FileStatus;

import crispy.kmerdist.KmerDistSeqFileMapper;
import crispy.gendist.GeneticDistSeqFileMapper;
import crispy.partition.SequenceFileBuilder;
import crispy.hcluster.HclusterSingleLinkage;
import crispy.io.MapArrayWritable;
import crispy.io.PointWritable;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

//AWS import com.amazonaws.services.s3.AmazonS3;
//AWS import com.amazonaws.services.s3.AmazonS3Client;
//AWS import com.amazonaws.services.s3.model.S3Object;
//AWS import com.amazonaws.services.s3.model.GetObjectRequest;
//AWS import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;

public class CrispyTK extends Configured implements Tool {

    // Parameters
    String hdfsOutputDir = "";
    String localOutputDir = "";
    String localFastq = "";
    Integer blockSize = 0;
    Integer numMapTasks = 0;

    // Folders
    String hdfsOutputInDir = "";
    String hdfsMROutput = "";
    String localOutputOutDir = "";
    String localTempDir = "";

    // Output Files
    String hdfsSequenceFile = "";
    String localSorted = "";
    String localTree = "";
    String localClusters = "";
    String localIndexFile = "";
    Integer totalSize = 0;

    Integer kmer = 6;
    Double kmerDistThreshold = 0.5;
    Double hclusterThreshold = 0.6;
    Double bandedK = 0.25;
    Double match = 5.00;
    Double mismatch = -4.00;
    Double gapOpen = -10.00;
    Double gapExtension = -5.00;
    Integer minClusterSize = 4;

    public void parseCLIArguments(String[] args) throws ParseException {
	CommandLine cli;
	Options options = new Options();
	CommandLineParser parser = new GnuParser();

	// [0] hdfsOutputDir (required)
	OptionBuilder obHDFSOutputDir = OptionBuilder.withArgName("hdfsOutputDir");
	obHDFSOutputDir.isRequired();
	obHDFSOutputDir.hasArgs();
	obHDFSOutputDir.withDescription("The HDFS output directory written to (Required)");
	obHDFSOutputDir.withLongOpt("hdfsOutputDir");
	Option oHDFSOutputDir = obHDFSOutputDir.create("hdfsOutputDir");

	// [1] localOutputDir (required)
	OptionBuilder obLocalOutputDir = OptionBuilder.withArgName("localOutputDir");
	obLocalOutputDir.isRequired();
	obLocalOutputDir.hasArgs();
	obLocalOutputDir.withDescription("The local output directory written to (Required)");
	obLocalOutputDir.withLongOpt("localOutputDir");
	Option oLocalOutputDir = obLocalOutputDir.create("localOutputDir");

	// [2] localFastq (required)
	OptionBuilder obLocalFastq = OptionBuilder.withArgName("localFastq");
	obLocalFastq.isRequired();
	obLocalFastq.hasArgs();
	obLocalFastq.withDescription("The input fastq file to be processed");
	obLocalFastq.withLongOpt("input");
	Option oLocalFastq = obLocalFastq.create("input");

	// [3] blockSize
	OptionBuilder obBlockSize = OptionBuilder.withArgName("blockSize");
	obBlockSize.hasArgs();
	obBlockSize.withDescription("The number of reads being processed per block");
	obBlockSize.withLongOpt("blockSize");
	Option oBlockSize = obBlockSize.create("blockSize");
	
	// [4] numMapTasks
	OptionBuilder obNumMapTasks = OptionBuilder.withArgName("numMapTasks");
	obNumMapTasks.hasArgs();
	obNumMapTasks.withDescription("The number of map tasks to "
				      + "split the contiguous block into");
	obNumMapTasks.withLongOpt("numMapTasks");
	Option oNumMapTasks = obNumMapTasks.create("numMapTasks");

	// [5] kmer
	OptionBuilder obKmer = OptionBuilder.withArgName("kmer");
	obKmer.hasArgs();
	obKmer.withDescription("The number of contiguous bases to be compared "
			       + "to the other read");
	obKmer.withLongOpt("kmer");
	Option oKmer = obKmer.create("kmer");
	
	// [6] kmerDistThreshold
	OptionBuilder obKmerDistThreshold = OptionBuilder.withArgName("kmerDistThreshold");
	obKmerDistThreshold.hasArgs();
	obKmerDistThreshold.withDescription("Performs GeneticDistance if "
					    + "kmerDist is smaller than threshold");
	obKmerDistThreshold.withLongOpt("kmerDistThreshold");
	Option oKmerDistThreshold = obKmerDistThreshold.create("kmerDistThreshold");

	// [7] hclusterThreshold
	OptionBuilder obHclusterThreshold = OptionBuilder.withArgName("hclusterThreshold");
	obHclusterThreshold.hasArgs();
	obHclusterThreshold.withDescription("Performs clustering at this threshold");
	obHclusterThreshold.withLongOpt("hclusterThreshold");
	Option oHclusterThreshold = obHclusterThreshold.create("hclusterThreshold");

	// [8] bandedK
	OptionBuilder obBandedK = OptionBuilder.withArgName("bandedK");
	obBandedK.hasArgs();
	obBandedK.withDescription("Width of the DP matrix that will be computed. In percent");
	obBandedK.withLongOpt("bandedK");
	Option oBandedK = obBandedK.create("bandedK");
	
	// [9] match
	OptionBuilder obMatch = OptionBuilder.withArgName("match");
	obMatch.hasArgs();
	obMatch.withDescription("Nucleotide match score");
	obMatch.withLongOpt("match");
	Option oMatch = obMatch.create("match");

	// [10] mismatch
	OptionBuilder obMismatch = OptionBuilder.withArgName("mismatch");
	obMismatch.hasArgs();
	obMismatch.withDescription("Nucleotide mismatch score");
	obMismatch.withLongOpt("mismatch");
	Option oMismatch = obMismatch.create("mismatch");

	// [11] gapOpen
	OptionBuilder obGapOpen = OptionBuilder.withArgName("gapOpen");
	obGapOpen.hasArgs();
	obGapOpen.withDescription("Gap opening score");
	obGapOpen.withLongOpt("gapOpen");
	Option oGapOpen = obGapOpen.create("gapOpen");

	// [12] gapExtension
	OptionBuilder obGapExtension = OptionBuilder.withArgName("gapExtension");
	obGapExtension.hasArgs();
	obGapExtension.withDescription("Gap extension score");
	obGapExtension.withLongOpt("gapExtension");
	Option oGapExtension = obGapExtension.create("gapExtension");

	// [13] minClusterSize
	OptionBuilder obMinClusterSize = OptionBuilder.withArgName("minClusterSize");
	obMinClusterSize.hasArgs();
	obMinClusterSize.withDescription("minimum cluster size before it is written out");
	obMinClusterSize.withLongOpt("minClusterSize");
	Option oMinClusterSize = obMinClusterSize.create("minClusterSize");

	// Options
	options.addOption(oHDFSOutputDir);
	options.addOption(oLocalOutputDir);
	options.addOption(oLocalFastq);
	options.addOption(oBlockSize);
	options.addOption(oNumMapTasks);
	options.addOption(oKmer);
	options.addOption(oKmerDistThreshold);
	options.addOption(oHclusterThreshold);
	options.addOption(oBandedK);
	options.addOption(oMatch);
	options.addOption(oMismatch);
	options.addOption(oGapOpen);
	options.addOption(oGapExtension);
	options.addOption(oMinClusterSize);

	cli = parser.parse(options, args);

	if (cli.hasOption("hdfsOutputDir")) {
	    this.hdfsOutputDir = parseTrailingForwardSlash(
				     cli.getOptionValue("hdfsOutputDir"));
	}
	if (cli.hasOption("localOutputDir")) {
	    this.localOutputDir = parseTrailingForwardSlash(
				      cli.getOptionValue("localOutputDir"));
	}
	if (cli.hasOption("input")) {
	    this.localFastq = cli.getOptionValue("input");
	}
	if (cli.hasOption("blockSize")) {
	    this.blockSize = Integer.parseInt(cli.getOptionValue("blockSize"));
	}
	if (cli.hasOption("numMapTasks")) {
	    this.numMapTasks = Integer.parseInt(cli.getOptionValue("numMapTasks"));
	}
	if (cli.hasOption("kmer")) {
	    this.kmer = Integer.parseInt(cli.getOptionValue("kmer"));
	}
	if (cli.hasOption("kmerDistThreshold")) {
	    this.kmerDistThreshold = Double.parseDouble(cli.getOptionValue("kmerDistThreshold"));
	}
	if (cli.hasOption("hclusterThreshold")) {
	    this.hclusterThreshold = Double.parseDouble(cli.getOptionValue("hclusterThreshold"));
	}
	if (cli.hasOption("bandedK")) {
	    this.bandedK = Double.parseDouble(cli.getOptionValue("bandedK"));
	}
	if (cli.hasOption("match")) {
	    Integer tempMatch = Integer.parseInt(cli.getOptionValue("match"));
	    this.match = tempMatch.doubleValue();
	}
	if (cli.hasOption("mismatch")) {
	    Integer tempMismatch = Integer.parseInt(cli.getOptionValue("mismatch"));
	    this.mismatch = tempMismatch.doubleValue();
	}
	if (cli.hasOption("gapOpen")) {
	    Integer tempGapOpen = Integer.parseInt(cli.getOptionValue("gapOpen"));
	    this.gapOpen = tempGapOpen.doubleValue();
	}
	if (cli.hasOption("gapExtension")) {
	    Integer tempGapExtension = Integer.parseInt(cli.getOptionValue("gapExtension"));
	    this.gapExtension = tempGapExtension.doubleValue();
	}
	if (cli.hasOption("minClusterSize")) {
	    this.minClusterSize = Integer.parseInt(cli.getOptionValue("minClusterSize"));
	}

	// Folders to create (except hdfsMROutput)
	this.hdfsOutputInDir   = this.hdfsOutputDir + "/input";
	this.hdfsMROutput      = this.hdfsOutputDir + "/output";	
	this.localOutputOutDir = this.localOutputDir + "/output";
	this.localTempDir      = this.localOutputDir + "/tmp";

	this.hdfsSequenceFile  = this.hdfsOutputInDir + "/hdfsSequenceFile.bin";
	this.localSorted       = this.localOutputOutDir + "/sorted.bin";
	this.localTree         = this.localOutputOutDir + "/tree.bin";
	this.localClusters     = this.localOutputOutDir + "/clusters.txt";
	this.localIndexFile    = this.localOutputOutDir + "/localIndexFile.txt";

	//AWS String localFastq = outputDir + "/localFastq.fq";
    }

    public void s3Option() {
	//AWS String bucketName = args[0];
	//AWS String key = args[1];
	//AWS AmazonS3 s3 = new AmazonS3Client(new ClasspathPropertiesFileCredentialsProvider());
	//AWS S3Object object = s3.getObject(new GetObjectRequest(bucketName, key));
	//AWS InputStream s3Fastq = object.getObjectContent();
	//AWS byte[] buf = new byte[1024];
	//AWS OutputStream localOut = new FileOutputStream(localFastq);
	//AWS Integer count = 0;
	//AWS while ( (count = s3Fastq.read(buf)) != -1 ) {
	//AWS     if (Thread.interrupted()) {
	//AWS   	throw new InterruptedException();
	//AWS     }
	//AWS     localOut.write(buf, 0, count);
	//AWS }
	//AWS localOut.close();
	//AWS s3Fastq.close();
    }

    /**
     * Starts the CrispyPipeline run
     * hadoop jar <jarFile> crispy.CrispyTK 
     * --input <input> 
     * --hdfsOutputDir <hdfsOutputDir> 
     * --localOutputDir <localOutputDir>
     * --blockSize <blockSize> 
     * --numMapTasks <numMapTasks>
     * --kmer <kmer>
     * --kmerDistThreshold <kmerDistThreshold>
     * --hclusterDistThreshold <hclusterDistThreshold>
     * --bandedK <bandedK>
     * --match <match>
     * --mismatch <mismatch>
     * --gapOpen <gapOpen>
     * --gapExtension <gapExtension>
     * --minClusterSize <minClusterSize>
     *
     * @return int
     */
    public int run(String[] args) throws Exception {

	// Parse Command Line Arguments
	this.parseCLIArguments(args);

	// Get FileSystem configurations
	Configuration conf = getConf();
	
	// Make directories in hdfsOutputDir localOutputDir
	FileSystem fs = FileSystem.get(conf);
	LocalFileSystem fslocal = FileSystem.getLocal(conf);
	fs.mkdirs(new Path(this.hdfsOutputInDir));
	fslocal.mkdirs(new Path(this.localOutputOutDir));
	fslocal.mkdirs(new Path(this.localTempDir));

	// Build SequenceFile input
	SequenceFileBuilder sfb = new SequenceFileBuilder(this.localFastq, 
							  this.localIndexFile, 
							  this.hdfsSequenceFile, 
							  this.blockSize);
	
	// Set Configurations
	this.totalSize = sfb.totalSize;
	conf.set("blockSize", blockSize.toString());
	conf.set("kmer", this.kmer.toString());
	conf.set("kmerDistThreshold", this.kmerDistThreshold.toString());
	conf.set("bandedK", this.bandedK.toString());
	conf.set("match", this.match.toString());
	conf.set("mismatch", this.mismatch.toString());
	conf.set("gapOpen", this.gapOpen.toString());
	conf.set("gapExtension", this.gapExtension.toString());

	JobConf job = new JobConf(conf);
	job.setJarByClass(CrispyTK.class);
	job.setJobName("CrispyPipeline");
	
	System.out.println("Hello");

	job.setInputFormat(SequenceFileInputFormat.class);
	job.setOutputFormat(SequenceFileOutputFormat.class);
	job.setOutputKeyClass(DoubleWritable.class);
	job.setOutputValueClass(PointWritable.class);

	SequenceFileInputFormat.addInputPath(job, new Path(this.hdfsSequenceFile));
	TextOutputFormat.setOutputPath(job, new Path(this.hdfsMROutput));

	JobConf map1Conf = new JobConf(false);
	ChainMapper.addMapper(job, KmerDistSeqFileMapper.class,
			      IntWritable.class, MapArrayWritable.class,
			      DoubleWritable.class, MapWritable.class,
			      false, map1Conf);

	JobConf map2Conf = new JobConf(false);
	ChainMapper.addMapper(job, GeneticDistSeqFileMapper.class,
			      DoubleWritable.class, MapWritable.class,
			      DoubleWritable.class, PointWritable.class,
			      false, map2Conf);

	//JobConf map3Conf = new JobConf(false);
	//ChainMapper.addMapper(job, TreeSortSeqFileMapper.class,
	//		      IntWritable.class, MapWritable.class,
	//		      IntWritable.class, PointArrayWritable.class,
	//		      false, map3Conf);

	// Compute the Maximum number of tasks available 
	// based on the block size partition
	Integer maxNumTasks = this.calculateMaxNumTasks();
	if (this.numMapTasks > maxNumTasks) { 
	    job.setNumMapTasks(maxNumTasks);
	} else {
	    job.setNumMapTasks(this.numMapTasks);
	}
	job.setNumReduceTasks(0);

        JobClient.runJob(job);

	
	// Transfer files from HDFS to Local and MergeSort SequenceFiles
	// Serial Computation
	SequenceFile.Sorter sfsort = new SequenceFile.Sorter(fslocal, DoubleWritable.class, 
							     PointWritable.class, conf);
	
	// Glob Pattern to search for part-00000 files in directory
	Path pattern = new Path(this.hdfsMROutput + "/part-[0-9]*");
	FileStatus[] files = fs.globStatus(pattern);

	// Initialize outputCopy
	Path[] outputCopy = new Path[files.length];

	// Initialize Path of outputSort
	Path outputSort = new Path(this.localSorted);

	// Iterate through files and copy them to LocalFileSystem
	for (int i = 0; i < files.length; i++) {
	    Path localCopy = files[i].getPath();
	    String[] fx = localCopy.toString().split("/");
	    String fileName = fx[fx.length-1].replace("part", "local");
	    outputCopy[i] = new Path(this.localTempDir + "/" + fileName);
	    FileUtil.copy(fs, localCopy, fslocal, outputCopy[i], false, conf);
	}
	
	// MergeSort the SequenceFiles
	sfsort.sort(outputCopy, outputSort, false);

	// Hcluster
	// Tree Construction and Thresholding
	// Serial Computation
	HclusterSingleLinkage hcsl = new HclusterSingleLinkage(this.totalSize, 
							       this.minClusterSize);
	hcsl.constructTree(this.localSorted);
	//hcsl.writeTreeToFile(this.localTree); //TODO
	hcsl.search(this.hclusterThreshold);
	hcsl.writeClustersToFile(this.localClusters);
	

	return 0;
    }

    public static void main(String[] args) throws Exception {
	
	int res = ToolRunner.run(new Configuration(), new CrispyTK(), args);

	System.exit(res);
    }

    public Integer calculateMaxNumTasks() {
	Double numBlocks = Math.ceil(this.totalSize.doubleValue() / 
				     this.blockSize.doubleValue());
	Double totalBlocks = ((numBlocks * (numBlocks - 1.00)) / 2.00) + 
	    Math.ceil(numBlocks/2.00);

	return totalBlocks.intValue();
    }

    public static String parseTrailingForwardSlash(String s) {
	String outputDir = "";
	if (s.endsWith("/")) {
	    outputDir = s.substring(0,(s.length()-1));
	} else {
	    outputDir = s;
	}
	return outputDir;
    }
}