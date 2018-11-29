package com.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Administrator on 2017/10/11.
 */
public class MaxReducer extends Reducer<Text, Text, Text, IntWritable> {

    @Override
    protected void reduce( Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException {

        int total = 0;
        for (Text text:values){
            int upflow =Integer.parseInt(text.toString().split("|")[0]);
            int downflow =Integer.parseInt(text.toString().split("|")[1]);
            total+=upflow+downflow;
        }
        context.write(key,new IntWritable(total));
    }
}
