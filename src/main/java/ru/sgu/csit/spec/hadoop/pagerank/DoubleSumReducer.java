package ru.sgu.csit.spec.hadoop.pagerank;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * User: KhurtinDN ( KhurtinDN@gmail.com )
 * Date: 12/16/10
 * Time: 9:31 PM
 */
public class DoubleSumReducer extends Reducer<Text, DoubleWritable, DoubleWritable, Text> {
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

        context.write(result, key);
    }
}
