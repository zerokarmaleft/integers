import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.*;

public class MaxIntegerMapperTest {

    @Test public void processesValidRecord()
        throws IOException, InterruptedException {

        Text value = new Text("0");

        new MapDriver<LongWritable, Text, Text, LongWritable>()
            .withMapper(new MaxIntegerMapper())
            .withInputValue(value)
            .withOutput(new Text("Max integer: "), new LongWritable(0))
            .runTest();
    }
}
