import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.*;

public class MaxIntegerReducerTest {

    @Test public void returnsMaxIntegerValue()
        throws IOException, InterruptedException {

        new ReduceDriver<Text, LongWritable, Text, LongWritable>()
            .withReducer(new MaxIntegerReducer())
            .withInputKey(new Text("Max integer: "))
            .withInputValues(Arrays.asList(new LongWritable(0),
                                           new LongWritable(2),
                                           new LongWritable(4)))
            .withOutput(new Text("Max integer: "), new LongWritable(4))
            .runTest();
    }
}
