package crispy;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.lib.NLineInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import crispy.partition.BalanceIndexMapper;
import crispy.gendist.GeneticDistMapper;

public class Genetic extends Configured implements Tool {

    public static String inputFasta = "";
    public static String inputIndex = "";
    public static String blocks = "";
    public static String output = "";
    public static String parentDirectory = "";
    public static String outputDirectory = "";

    public int run(String[] args) throws Exception {

	// implement Tools
	// hadoop jar crispy.jar crispy.Genetic <working-directory> <output-directory> <blocks> <inputFasta>
	parentDirectory = args[0];
	outputDirectory = args[1];
	blocks          = args[2];
	inputFasta      = parentDirectory + args[3];
	inputIndex      = parentDirectory + "inputIndex.txt";

	Configuration conf = getConf();
	conf.set("InputFasta", inputFasta);

	BalanceIndexMapper.assign(inputFasta, inputIndex, blocks);

        // Job1 - GeneticDist
        JobConf job1 = new JobConf(conf);
        job1.setJarByClass(Genetic.class);
        job1.setJobName("CalculateGeneticDist");

        // The Actual Input/Output format of the file to be output
        // Set all OutputKey Classes (Map)
        job1.setInputFormat(NLineInputFormat.class);
        job1.setOutputFormat(TextOutputFormat.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        // Path of the Files
        // TextInputFormat is a subclass of FileInputFormat
        TextInputFormat.addInputPath(job1, new Path(inputIndex));
        TextOutputFormat.setOutputPath(job1, new Path(outputDirectory));
		
        // Set mapper class for "KmerDist" Job
        // Note: There is no reducer class for "KmerDist"
        job1.setMapperClass(GeneticDistMapper.class);
        job1.setNumReduceTasks(0);

	// Set mapper output key class
	//job2.setMapOutputKeyClass(IntArrayWritable.class);
	//job2.setMapOutputValueClass(Text.class);

        JobClient.runJob(job1);

	return 0;
    }

    public static void main(String[] args) throws Exception {
		
        int res = ToolRunner.run(new Configuration(), new Genetic(), args);

	System.exit(res);
    }
}
