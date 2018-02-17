package hw2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

class KeyComparator extends WritableComparator {
    protected KeyComparator() {
        super(DoubleWritable.class, true);
    }

    @SuppressWarnings("rawtypes")
    @Override
    // comparing in decreasing order
    public int compare(WritableComparable w1, WritableComparable w2) {
    	DoubleWritable key1 = (DoubleWritable) w1;
    	DoubleWritable key2 = (DoubleWritable) w2;          
        return -1 * key1.compareTo(key2);
    }
}
