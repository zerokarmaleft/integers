import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MatrixMultiplicationValueProductReducer 
    extends Reducer<LongWritable, LongTriplet, LongWritable, Text> {
    
    @Override protected void reduce (LongWritable key,
                                     Iterable<LongTriplet> values,
                                     Context context)
        throws IOException, InterruptedException {

        ArrayList<LongTriplet> mTuples = new ArrayList<LongTriplet>();
        ArrayList<LongTriplet> nTuples = new ArrayList<LongTriplet>();
        for (LongTriplet value : values) {
            if (value.getFirst().get() == 0) {
                mTuples.add (new LongTriplet(value.getFirst().get(),
                                             value.getSecond().get(),
                                             value.getThird().get()));
                System.err.printf("%d,%d,%d added to M\n",
                                  value.getFirst().get(),
                                  value.getSecond().get(),
                                  value.getThird().get());
            }
            if (value.getFirst().get() == 1) {
                nTuples.add (new LongTriplet(value.getFirst().get(),
                                             value.getSecond().get(),
                                             value.getThird().get()));
                System.err.printf("%d,%d,%d added to N\n",
                                  value.getFirst().get(),
                                  value.getSecond().get(),
                                  value.getThird().get());
            }
        }

        for (LongTriplet mTuple : mTuples) {
            for (LongTriplet nTuple : nTuples) {
                System.err.printf("%s cross %s\n",
                                  mTuple.toString(),
                                  nTuple.toString());

                long i    = mTuple.getSecond().get ();
                long k    = nTuple.getSecond().get ();
                long m_ij = mTuple.getThird ().get ();
                long n_jk = nTuple.getThird ().get ();

                context.write(key,
                              new Text(i + "," +
                                       k + "," +
                                       (m_ij * n_jk)));
            }
        }
    }
}
