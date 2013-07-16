package crispy.test;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import crispy.test.TestMapper;
import crispy.partition.SequenceFileBuilder;

public class Test extends Configured implements Tool {

    public int run(String[] args) throws Exception {
	//String input = args[0];
	//String output = args[1];

	String input = "/data/chenga/projects/crispy-hadoop-0.20/data/real/SRR029122/SRR029122.fastq";
	String output1 = "IndexFile.txt";
	String output2 = "SequenceFile.bin";
	Integer lines = 10000;
	SequenceFileBuilder sfb = new SequenceFileBuilder(input, output1, 
							  output2, lines);
	//Configuration conf = getConf();
	//JobConf job = new JobConf(conf);
	//job.setJarByClass(Test.class);
	//job.setJobName("Test");
	
	//job.setInputFormat(TextInputFormat.class);
	//job.setOutputFormat(TextOutputFormat.class);
	//job.setOutputKeyClass(Text.class);
	//job.setOutputValueClass(Text.class);

	//TextInputFormat.addInputPath(job, new Path(input));
	//TextOutputFormat.setOutputPath(job, new Path(output));

	//job.setMapperClass(TestMapper.class);
	//job.setNumReduceTasks(0);

	//JobClient.runJob(job);

	return 0;
    }

    public static void main(String[] args) throws Exception {
	
	int res = ToolRunner.run(new Configuration(), new Test(), args);

	System.exit(res);
    }
}