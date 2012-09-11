import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UniqueIntegerCountReducer
    extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {

    @Override public void reduce(LongWritable key,
                                 Iterable<LongWritable> values,
                                 Context context)
        throws IOException, InterruptedException {

        long sum = 0;
        for (LongWritable value : values) {
            sum += Long.parseLong(value.toString());
        }

        context.write(key, new LongWritable(sum));
    }
}
