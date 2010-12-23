package ru.sgu.csit.spec.hadoop.pagerank;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * User: KhurtinDN ( KhurtinDN@gmail.com )
 * Date: 12/23/10
 * Time: 3:49 AM
 */
public class DoubleSumCombiner extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private static final double D = 0.85;

    private DoubleWritable result = new DoubleWritable();

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {

        double sum = 1.0 - D;
        for (DoubleWritable value : values) {
            sum += value.get();
        }
        result.set(sum);

        context.write(key, result);
    }
}
