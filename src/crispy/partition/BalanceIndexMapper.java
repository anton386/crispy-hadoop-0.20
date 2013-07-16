package crispy.partition;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Vector;
import java.util.Arrays;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;


/*
 * Command:
 * java BalanceIndexMapper <input> <output> <nproc>
 */
public class BalanceIndexMapper {

    public static void main(String[] args) {
	assign(args[0], args[1], args[2]);
    }

    public static Integer count(String input) {
	BufferedReader in = null;
	Integer counter = 0;
	try {
	    Path path = new Path(input);
	    FileSystem fs = FileSystem.get(new Configuration());
	    in = new BufferedReader(new InputStreamReader(fs.open(path)));
	    String i = in.readLine();
	    while (i != null) {
		counter++;
		i = in.readLine();
	    }
	} catch (IOException e) {
	    e.printStackTrace();
	}
	return counter;
    }

    public static void assign(String input, String output, String nprocInput) {
	BufferedWriter out = null;
	int nsize = count(input);
	int nproc = Integer.valueOf(nprocInput);
	int[] blocks = assignBlocks(nproc, nsize);
	int[] end_blocks = cumulativeBlocks(blocks);
	int[] half_blocks = new int[2];


	try {
	    Path path = new Path(output);
	    FileSystem fs = FileSystem.get(new Configuration());
	    out = new BufferedWriter(new OutputStreamWriter(fs.create(path)));
	    for (int i = 0; i < end_blocks.length; i++) {
		for (int j = 0; j <= i; j++) {
		    int x_start = end_blocks[j] - blocks[j] + 1;
		    int x_end = end_blocks[j];
		    int y_start = end_blocks[i] - blocks[i] + 1;
		    int y_end = end_blocks[i];
		    
		    // store half block into memory
		    if (x_start == y_start && x_end == y_end) {
			if (half_blocks[0] == 0 && half_blocks[1] == 0) {
			    half_blocks[0] = x_start;
			    half_blocks[1] = x_end;
			} else {
			    out.write(">"+"("+half_blocks[0]+","+half_blocks[1]+")");
			    out.write("("+half_blocks[0]+","+half_blocks[1]+")"+":");
			    out.write("("+x_start+","+x_end+")");
			    out.write("("+y_start+","+y_end+")"+"\n");
			    half_blocks = new int[2];
			}
		    } else {
			out.write(">"+"("+x_start+","+x_end+")");
			out.write("("+y_start+","+y_end+")"+"\n");
		    }
		}
	    }
	    // write out last of any buffers in half blocks
	    if (half_blocks[0] != 0 && half_blocks[1] != 0) {
		out.write(">"+"("+half_blocks[0]+","+half_blocks[1]+")");
		out.write("("+half_blocks[0]+","+half_blocks[1]+")");
	    }
	} catch (FileNotFoundException e) {
	    e.printStackTrace();
	} catch (IOException e) {
	    e.printStackTrace();
	} finally {
	    try {
		out.close();
	    } catch (IOException e) {
		e.printStackTrace();
	    }
	}
    }

    public static int[] checkBlock(int i, int j, int[] end_blocks) {
	int x_quadrant = 0;
	int y_quadrant = 0;
	for (int k = 0; k < end_blocks.length; k++) {
	    if (i > end_blocks[k]) {
		
	    } else {
		x_quadrant = k+1;
		break;
	    }
	}
	for (int l = 0; l < end_blocks.length; l++) {
	    if (j > end_blocks[l]) {
	    
	    } else {
		y_quadrant = l+1;
		break;
	    }
	}
	
	int[] quadrants = new int[2];
	quadrants[0] = x_quadrant;
	quadrants[1] = y_quadrant;
	return quadrants;
    }

    public static int[] assignBlocks(int nproc, int nsize) {
	int[] blocks = new int[nproc];
	int upper_limit = nsize/nproc + 1;
	int lower_limit = nsize/nproc;
	int num_of_upper = nsize % nproc;
	int num_of_lower = nproc - num_of_upper;

	// Assign by lower first, than upper
	for (int i = 0; i < num_of_lower; i++) {
	    blocks[i] = lower_limit;
	}
	for (int j = num_of_lower; j < nproc; j++) {
	    blocks[j] = upper_limit;
	}
	return blocks;
    }

    public static int[] cumulativeBlocks(int[] blocks) {
	int[] end_blocks = new int[blocks.length];
	int total = -1;
	for (int i = 0; i < blocks.length; i++) {
	    total = total + blocks[i];
	    end_blocks[i] = total;
	}
	return end_blocks;
    }

}