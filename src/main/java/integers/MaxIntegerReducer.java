import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxIntegerReducer
    extends Reducer<Text, LongWritable, Text, LongWritable> {

    @Override public void reduce(Text key,
                                 Iterable<LongWritable> values,
                                 Context context)
        throws IOException, InterruptedException {

        long maxValue = Long.MIN_VALUE;
        for (LongWritable value : values) {
            maxValue = Math.max(maxValue, value.get());
        }
        context.write(key, new LongWritable(maxValue));
    }
}
