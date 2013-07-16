package crispy.gendist;

import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.io.InputStreamReader;

import crispy.gendist.GeneticDist;

public class GeneticDistMapper 
    extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    static int counter = 1;
    static HashMap<String, String> data = new HashMap<String, String>();
    static String input;
    static String width;

    public void configure(JobConf job) {
	input = job.get("InputFasta");
	width = job.get("bandedK");
    }

    public void map(LongWritable key, Text value, 
		    OutputCollector<Text, Text> output, Reporter context) throws IOException {

        BufferedReader f = null;

        Pattern p1 = Pattern.compile("^\\((.+),(.+)\\)\\((.+),(.+)\\)");
        int x_start = 0;
        int x_end = 0;
        int y_start = 0;
        int y_end = 0;
        int file_counter = 0;
        String text = "";

        try {

            // Store key and value into HashMap<String, Integer[]> data for fast calculation
	    String [] new_value = value.toString().replaceAll(">","").replaceAll("\n","").split(":");
	    for (int ii = 0; ii < new_value.length; ii++) {
		Matcher m = p1.matcher(new_value[ii]);
		if (m.matches()) {
		    x_start = Integer.valueOf(m.group(1));
		    x_end = Integer.valueOf(m.group(2));
		    y_start = Integer.valueOf(m.group(3));
		    y_end = Integer.valueOf(m.group(4));

		    // reinitialize the file_counter
		    file_counter = 0;
		    // Store index of EnumeratedSortKmer
		    if (x_start == y_start && x_end == y_end) {
			// get only from one range
			Path path = new Path(input);
			FileSystem fs = FileSystem.get(new Configuration());
			f = new BufferedReader(new InputStreamReader(fs.open(path)));
			text = f.readLine();
			while (text != null) {
			    if (file_counter >= x_start && file_counter <= x_end) {
				String[] input_data = text.replaceAll("\n","").split("\t");
				data.put(input_data[0], input_data[1]);
			    }
			    text = f.readLine();
			    file_counter++;
			}
		    } else {
			// get from two ranges
			int[] start_array = new int[2];
			int[] end_array = new int[2];
			if (x_start < y_start) {
			    start_array[0] = x_start;
			    start_array[1] = y_start;
			    end_array[0] = x_end;
			    end_array[1] = y_end;
			} else {
			    start_array[0] = y_start;
			    start_array[1] = x_start;
			    end_array[0] = y_end;
			    end_array[1] = x_end;
			}
			for (int i = 0; i < 2; i++) {
			    Path path = new Path(input);
			    FileSystem fs = FileSystem.get(new Configuration());
			    f = new BufferedReader(new InputStreamReader(fs.open(path)));
			    text = f.readLine();
			    file_counter = 0;
			    while (text != null) {
				if (file_counter >= start_array[i] && file_counter <= end_array[i]) {
				    String[] input_data = text.replaceAll("\n","").split("\t");
				    data.put(input_data[0], input_data[1]);
				}
				text = f.readLine();
				file_counter++;
			    }
			}	
		    }
		}
	    
		// Run calculations for the Block
		if (x_start == y_start) {
		    for (Integer k = y_start; k < y_end+1; k++) {
			for (Integer l = x_start; l < (x_start+(k-y_start)); l++) {
			    String new_key = "(" + k + "," + l + ")";
			    // IntWritable[] new_key = new IntWritable[2];
			    // new_key[0] = new IntWritable(k);
			    // new_key[1] = new IntWritable(l);
			    String read1 = data.get(k.toString());
			    String read2 = data.get(l.toString());

			    // calculate KmerDist
			    GeneticDist gd = new GeneticDist();
			    Double distance = gd.execute(read1, read2,
							 Double.parseDouble(width));
			    output.collect(new Text(new_key), new Text(distance.toString()));
			}
		    }
		} else {
		    for (Integer k = y_start; k < y_end+1; k++) {
			for (Integer l = x_start; l < x_end+1; l++) {
			    String new_key = "(" + k + "," + l + ")";
			    // IntWritable[] new_key = new IntWritable[2];
			    // new_key[0] = new IntWritable(k);
			    // new_key[1] = new IntWritable(l);

			    String read1 = data.get(k.toString());
			    String read2 = data.get(l.toString());

			    // calculate KmerDist
			    GeneticDist gd = new GeneticDist();
			    Double distance = gd.execute(read1, read2, 
							 Double.parseDouble(width));
			    output.collect(new Text(new_key), new Text(distance.toString()));
			}
		    }
		} 	
	    }	   

	} catch (IOException e) {
	    e.printStackTrace();
	} catch (Exception e) {
	    e.printStackTrace();
	}
	/*
	  finally {
	      try {
	          f.reset();
	      } catch (IOException e) {
	          e.printStackTrace();
	      }
	  }
	*/


    }
}