package crispy;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class IndexTextInputFormat extends TextInputFormat{
	
	@Override
	// Ensure that this file is not split at all
	public boolean isSplitable(JobContext context, Path file) {
		return false;
	}

}
