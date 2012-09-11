import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AvgIntegerReducer
    extends Reducer<Text, LongWritable, Text, LongWritable> {

    @Override public void reduce(Text key,
                                 Iterable<LongWritable> values,
                                 Context context)
        throws IOException, InterruptedException {

        int sum = 0;
        int n = 0;
        for (LongWritable value : values) {
            sum += value.get();
            n++;
        }
        context.write(key, new LongWritable(sum / n));
    }
}
