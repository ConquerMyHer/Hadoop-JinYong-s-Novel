import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.lang.reflect.Array;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;

class GetResultMapper extends Mapper<Text, Text, Text, Text> {
    private Text resKey = new Text();  // 要传入reduce的key和value
    private Text resValue = new Text();

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String name = key.toString();
        Double rank = Double.parseDouble(value.toString().split("#")[0]);//去除后面的关系表，只保留rank
        resKey.set(name);
        resValue.set(rank.toString());
        context.write(resKey, resValue);
    }
}

class GetResultReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text t : values) {
            context.write(key, t);  // 不做处理
        }
    }
}

public class GetResult {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("PageResult neet 2 paras as input and output");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "PageResult");
        job.setJarByClass(GetResult.class);     // 设置配置文件信息
        job.setMapperClass(GetResultMapper.class);
        job.setPartitionerClass(HashPartitioner.class);
        job.setReducerClass(GetResultReducer.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);// 以/t切割key与value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1); //reducer设置为一个，使结果为一个文件
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));  // 设置输入路径
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));// 设置输出路径
        System.exit(job.waitForCompletion(true) ? 0 : 1);//等待程序结束退出
    }
}
