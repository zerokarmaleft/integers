import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.*;

public class AvgIntegerReducerTest {

    @Test public void returnsAvgIntegerValue()
        throws IOException, InterruptedException {

        new ReduceDriver<Text, LongWritable, Text, LongWritable>()
            .withReducer(new AvgIntegerReducer())
            .withInputKey(new Text("Avg integer: "))
            .withInputValues(Arrays.asList(new LongWritable(0),
                                           new LongWritable(1),
                                           new LongWritable(1),
                                           new LongWritable(2),
                                           new LongWritable(3),
                                           new LongWritable(5),
                                           new LongWritable(8),
                                           new LongWritable(13)))
            .withOutput(new Text("Avg integer: "), new LongWritable(4))
            .runTest();
    }
}
