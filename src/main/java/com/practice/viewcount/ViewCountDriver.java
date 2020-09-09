package com.practice.viewcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This class is a driver for view count.
 *
 * input args(Program arguments) : src/main/resources/data/input src/main/resources/outData/
 */
public class ViewCountDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {

        int exitCode = ToolRunner.run(new ViewCountDriver(), args);
        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {

        Configuration configuration = this.getConf();
        Job job = Job.getInstance(configuration);
        job.setJobName("ViewCount");
        job.setJarByClass(ViewCountDriver.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(ViewCountMapper.class);
        job.setReducerClass(ViewCountReducer.class);

        Path inputFilePath = new Path(args[0]);
        Path outputFilePath = new Path(args[1]);


        FileInputFormat.addInputPath(job, inputFilePath);
        FileOutputFormat.setOutputPath(job, outputFilePath);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
