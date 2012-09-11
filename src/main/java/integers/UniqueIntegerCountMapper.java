import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UniqueIntegerCountMapper
    extends Mapper<LongWritable, Text, LongWritable, LongWritable> {

    @Override public void map(LongWritable key,
                              Text value,
                              Context context)
        throws IOException, InterruptedException {

        long number = Long.parseLong(value.toString());
        context.write(new LongWritable(number), new LongWritable(1));
    }
}
