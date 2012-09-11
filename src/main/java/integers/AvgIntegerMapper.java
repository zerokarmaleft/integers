import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AvgIntegerMapper
    extends Mapper<LongWritable, Text, Text, LongWritable> {

    @Override public void map(LongWritable key,
                              Text value,
                              Context context)
        throws IOException, InterruptedException {

        long number = Long.parseLong(value.toString());
        context.write(new Text("Avg integer: "), new LongWritable(number));
    }
}
