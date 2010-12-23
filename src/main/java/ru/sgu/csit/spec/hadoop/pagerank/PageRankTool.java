package ru.sgu.csit.spec.hadoop.pagerank;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.Tool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * User: KhurtinDN ( KhurtinDN@gmail.com )
 * Date: 12/16/10
 * Time: 9:08 PM
 */
public class PageRankTool extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Usage: <input_file> <output>");
            return 2;
        }

        Job job = new Job(getConf(), "SimplePageRank");
        job.setJarByClass(PageRankTool.class);

        job.setMapperClass(PageRankMapper.class);
        job.setCombinerClass(DoubleSumCombiner.class);
        job.setReducerClass(DoubleSumReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);

        if (result) {
            Path fileNamePath = new Path(args[1] + "/part-r-00000");

            FileSystem fileSystem = fileNamePath.getFileSystem(job.getConfiguration());

            if (fileSystem.exists(fileNamePath)) {

                List<OutputRecord> top = new ArrayList<OutputRecord>();

                FSDataInputStream fsDataInputStream = fileSystem.open(fileNamePath);
                LineReader lineReader = new LineReader(fsDataInputStream);

                Text tmp = new Text();
                while (lineReader.readLine(tmp) > 0) {
                    String[] lines = tmp.toString().split("\\s+");
                    OutputRecord outputRecord = new OutputRecord(lines[1], Double.parseDouble(lines[0]));
                    top.add(outputRecord);
                }

                fsDataInputStream.close();

                Collections.sort(top);

                System.out.println("\n\n\nTop 10 in PageRank:");
                for (int i = 0; i < 10; ++i) {
                    System.out.println(top.get(i));
                }
                System.out.println("\nВот как то так...\n\n");
            }
        }

        return result ? 0 : 1;
    }

    private class OutputRecord implements Comparable<OutputRecord> {
        private String ref;
        private Double PR;

        private int direct = -1; // for sort by ubivaniu

        private OutputRecord(String ref, Double PR) {
            this.ref = ref;
            this.PR = PR;
        }

        public int compareTo(OutputRecord o) {
            return direct * (PR < o.PR ? -1 : 1);
        }

        @Override
        public String toString() {
            return String.format("%.3f\t%s", PR, ref);
        }
    }
}
