package com.practice.maxtemp;

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
 * This program is a map reduce code showing the implementation of finding max temperature
 * per day.
 *
 * The input args are : src/main/resources/temp src/main/resources/outData/
 */
public class MaxTempDriver extends Configured implements Tool {

    public static void main(String args[]) throws Exception {
        int exitCode = ToolRunner.run(new MaxTempDriver(), args);
        System.exit(exitCode);
    }
    public int run(String[] args) throws Exception {

        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf);
        job.setJobName("MaxTemperaturePerDay");
        job.setJarByClass(MaxTempDriver.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(MaxTempMapper.class);
        job.setReducerClass(MaxTempReducer.class);

        Path inputFilePath = new Path(args[0]);
        Path outputFilePath = new Path(args[1]);

        FileInputFormat.addInputPath(job, inputFilePath);
        FileOutputFormat.setOutputPath(job, outputFilePath);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
