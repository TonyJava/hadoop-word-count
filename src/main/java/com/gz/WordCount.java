package com.gz;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by Administrator on 2017/6/4.
 * 统计文件中各个字符串的出现次数
 */
public class WordCount {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = new Job();
        job.setJobName("word count");

        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //InputPath和OutputPath路径可以任意指定，并且可以同时处理多个文件
        //input中的路径参数需要精确到文件名,否则报错（Exception in thread "main" java.lang.RuntimeException: Error while running command to get file permissions : java.io.IOException: (null) entry in command string: null ls -F E:\Idea-projects\demo\hadoop\hadoop-word-count\input\22）
        FileInputFormat.addInputPath(job, new Path("E:\\Idea-projects\\demo\\hadoop\\hadoop-filesystem\\input\\22"));
        //FileInputFormat.addInputPath(job, new Path("E:\\Idea-projects\\demo\\hadoop\\testData\\input\\22"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\Idea-projects\\demo\\hadoop\\hadoop-filesystem\\output"));
        //FileOutputFormat.setOutputPath(job, new Path(args[1]));//args[1]默认为上述路径
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
