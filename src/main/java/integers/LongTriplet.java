import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

public class LongTriplet
    implements WritableComparable<LongTriplet> {

    private LongWritable first;
    private LongWritable second;
    private LongWritable third;

    public LongTriplet () {
        set (new LongWritable (), new LongWritable (), new LongWritable ());
    }

    public LongTriplet (long first, long second, long third) {
        set (new LongWritable (first),
             new LongWritable (second),
             new LongWritable (third));
    }

    public LongTriplet (LongWritable first, 
                        LongWritable second, 
                        LongWritable third) {
        set (first, second, third);
    }

    public void set (LongWritable first,
                     LongWritable second,
                     LongWritable third) {
        this.first  = first;
        this.second = second;
        this.third  = third;
    }

    public LongWritable getFirst () {
        return first;
    }

    public LongWritable getSecond () {
        return second;
    }

    public LongWritable getThird () {
        return third;
    }

    @Override public void write (DataOutput out) throws IOException {
        first.write (out);
        second.write (out);
        third.write (out);
    }

    @Override public void readFields (DataInput in) throws IOException {
        first.readFields (in);
        second.readFields (in);
        third.readFields (in);
    }
    
    @Override public int hashCode () {
        return first.hashCode () + second.hashCode () + third.hashCode ();
    }

    @Override public boolean equals (Object o) {
        if (o instanceof LongTriplet) {
            LongTriplet t = (LongTriplet) o;
            return (first.equals (t.first) &&
                    second.equals (t.second) &&
                    third.equals (t.third));
        }

        return false;
    }

    @Override public int compareTo (LongTriplet t) {
        int result = first.compareTo (t.first);
        if (result != 0) return result;

        result = second.compareTo (t.second);
        if (result != 0) return result;

        return third.compareTo (t.third);
    }

    @Override public String toString () {
        return first + "," + second +  "," + third ;
    }
}
