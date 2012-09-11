import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class AvgInteger {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: AvgInteger <input path> <output path>");
            System.exit(-1);
        }

        Job job = new Job();
        job.setJarByClass(AvgInteger.class);
        job.setJobName("Avg integer");

        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(AvgIntegerMapper.class);
        job.setReducerClass(AvgIntegerReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
