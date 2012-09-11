import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class UniqueIntegers {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: UniqueIntegerCount <input path> <output path>");
            System.exit(-1);
        }

        Job job = new Job();
        job.setJarByClass(UniqueIntegers.class);
        job.setJobName("Unique integers");

        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(UniqueIntegersMapper.class);
        job.setReducerClass(UniqueIntegersReducer.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(NullWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

