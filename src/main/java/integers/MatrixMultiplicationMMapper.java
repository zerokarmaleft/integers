import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MatrixMultiplicationMMapper 
    extends Mapper<LongWritable, Text, LongWritable, LongTriplet> {

    @Override public void map (LongWritable key,
                               Text value,
                               Context context)
        throws IOException, InterruptedException {

        String [] tuple = value.toString ().split (",");
        long i    = Long.parseLong (tuple [0]);
        long j    = Long.parseLong (tuple [1]);
        long m_ij = Long.parseLong (tuple [2]);

        context.write (new LongWritable (j),
                       new LongTriplet (0, i, m_ij));
    }
}
