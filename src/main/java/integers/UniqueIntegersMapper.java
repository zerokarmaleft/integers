import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UniqueIntegersMapper
    extends Mapper<LongWritable, Text, LongWritable, NullWritable> {

    @Override public void map(LongWritable key,
                              Text value,
                              Context context)
        throws IOException, InterruptedException {

        long number = Long.parseLong(value.toString());
        LongWritable k2 = new LongWritable(number);

        context.write(k2, NullWritable.get());
    }
}
