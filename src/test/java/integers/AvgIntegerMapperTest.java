import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.*;

public class AvgIntegerMapperTest {

    @Test public void proccessValidRecord()
        throws IOException, InterruptedException {

        Text value = new Text("0");

        new MapDriver<LongWritable, Text, Text, LongWritable>()
            .withMapper(new AvgIntegerMapper())
            .withInputValue(value)
            .withOutput(new Text("Avg integer: "), new LongWritable(0))
            .runTest();
    }
}
