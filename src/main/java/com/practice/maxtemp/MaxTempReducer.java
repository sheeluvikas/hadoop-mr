package com.practice.maxtemp;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxTempReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int big = Integer.MIN_VALUE;
        for (IntWritable value : values) {
            if (value.get() > big) {
                big = value.get();
            }
        }
        context.write(key, new IntWritable(big));
    }
}