package com.gz;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.net.URI;

/**
 * Created by Administrator on 2017/7/8.
 * 使用seek()方法，将hadoop文件系统上的一个文件在标准输出上显示两次
 */
public class FileSystemDoubleCat {
    public static void main(String[] args) throws IOException {
        String uri = "file:///E:/Idea-projects/demo/hadoop/hadoop-filesystem/input/22";
        //String uri = "http://139.159.212.236:8888/login"; //No FileSystem for scheme: http
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(URI.create(uri),configuration);
        FSDataInputStream in = null;
        try {
            in = fileSystem.open(new Path(uri));
            IOUtils.copyBytes(in, System.out, 4096, false);
            in.seek(0);
            IOUtils.copyBytes(in,System.out,4096,false);
            /*for(int i=0;i<2;i++) {
                IOUtils.copyBytes(in, System.out, 4096, false);
                in.seek(0);
            }*/
        } finally {
            IOUtils.closeStream(in);
        }
    }
}
