package com.mapreduce;

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
 * Created by Administrator on 2017/10/12.
 */
public class MaxDriver extends Configured implements Tool {


    @Override
    public int run( String[] strings ) throws Exception {
        if(strings.length!=2){
            System.err.println("error");
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }
        Job job  = new Job(getConf(),"test0");
        job.setJarByClass(getClass());

        FileInputFormat.addInputPath(job,new Path(strings[0]));
        FileOutputFormat.setOutputPath(job,new Path(strings[1]));

        job.setMapperClass(MaxMapper.class);
        job.setReducerClass(MaxReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        int exicode=ToolRunner.run(new MaxDriver(),args);
        System.exit(exicode);

    }
}
