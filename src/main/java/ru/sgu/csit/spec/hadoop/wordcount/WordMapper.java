package ru.sgu.csit.spec.hadoop.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * User: KhurtinDN ( KhurtinDN@gmail.com )
 * Date: 12/16/10
 * Time: 9:02 PM
 */
public class WordMapper extends Mapper<Object, Text, Text, IntWritable> {
    private static final Pattern pattern = Pattern.compile("[^А-Яа-яA-Za-z]+");

    private static final IntWritable one = new IntWritable(1);
    private Text wordText = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[] words = pattern.split(value.toString());

        for (String word : words) {
            word = word.toLowerCase();
            if (!word.isEmpty()) {
                wordText.set( word );
                context.write(wordText, one);
            }
        }
    }
}
