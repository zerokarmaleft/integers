import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MatrixMultiplicationNMapper 
    extends Mapper<LongWritable, Text, LongWritable, LongTriplet> {

    @Override public void map (LongWritable key,
                               Text value,
                               Context context)
        throws IOException, InterruptedException {

        String [] tuple = value.toString ().split (",");
        long j    = Long.parseLong (tuple [0]);
        long k    = Long.parseLong (tuple [1]);
        long n_jk = Long.parseLong (tuple [2]);

        context.write (new LongWritable (j),
                       new LongTriplet (1, k, n_jk));
    }
}
