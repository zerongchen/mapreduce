package com.mapreduce;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Administrator on 2017/10/11.
 */
public class MaxMapper extends Mapper<LongWritable,Text,Text,Text> {

    @Override
    protected void map( LongWritable key, Text value, Context context ) throws IOException, InterruptedException {
        String[] txt = value.toString().split("|");

        String date= txt[15];
        String totaFlow = txt[0]+"|"+txt[1];
        context.write(new Text(date),new Text(totaFlow));

    }
}
