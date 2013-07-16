package crispy;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.Vector;

public class IndexMapper extends Mapper<LongWritable, Text, Text, Text> {
    static Integer counter = 0;
    static Vector<String[]> index = new Vector<String[]>();
    // index[0] =// ['lcl|P5.022609_10
    // CGACAGCTGACA',
    // 'CTGGACCGTGTCTCAGTTCCAATGTGGGGG']
    // index[1] =
    // ['lcl|P2.030409_12674
    // CGTACAGTTATC',
    // 'TTGGGCCGTGTCTCAGTCCCAATGTGGCCG']

    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

        String[] h = null;
        String[] first_read = null;
        String[] second_read = null;

        // Parsing the input file
        String line = value.toString();
        h = line.replaceAll(">", "").replaceAll("\n", "").split("\t");

        // Add fasta description and sequence into the index
        index.add(h);

        // lets map the index if possible
        if (counter > 0) {
            for (Integer i = 0; i < counter; i++) {
                if (i != counter) {
                    first_read = index.get(counter);
                    second_read = index.get(i);

                    // String index_representation = "(" + i.toString() + ","
                    // + counter.toString() + ")";
                    // String[] index_representation = {counter.toString(),
                    // i.toString()};
                    String index_representation = counter.toString() + ","
                        + i.toString();

                    // MapWritable map_representation = new MapWritable();
                    // String[] description_representation = {first_read[0],
                    // second_read[0]};
                    // String[] sequence_representation = {first_read[1],
                    // second_read[1]};
                    // map_representation.put(new Text("description"), new
                    // ArrayWritable(description_representation));
                    // map_representation.put(new Text("sequence"), new
                    // ArrayWritable(sequence_representation));
                    String map_representation = first_read[0] + "\t"
                        + first_read[1] + "\t" + second_read[0] + "\t"
                        + second_read[1];

                    // Let's serialize the index and the corresponding
                    // description and sequence
                    context.write(new Text(index_representation), new Text(
                        map_representation));
                }
            }
        }

        counter += 1;
    }
}
