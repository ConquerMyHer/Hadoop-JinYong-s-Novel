package PageRankSort;
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
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


class PageRankMapper extends Mapper<Text, Text, Text, Text> {
    private Text resKey = new Text();      // 要传入reduce的key和value
    private Text resValue = new Text();

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String name = key.toString();
        Double rank = 0.0;
        String relations = new String();
        if (value.toString().contains("#")) {// 不是第一次迭代
            rank = Double.parseDouble(value.toString().split("#")[0]); // 原本rank
            relations = value.toString().split("#")[1];       // 出度表 格式为[名字,影响|名字,影响|...]其中影响为对key中人物的影响
        } else {//第一次迭代，设置默认rank
            rank = 1.0;                // 初始化rank值为1
            relations = value.toString(); // 出度表           
        }
        resKey.set(name);
        resValue.set("#" + relations);
        context.write(resKey, resValue);  // key为名字 value以#开头，后面为出度表，生成此键值对

        String[] slist = relations.split("\\[|\\]")[1].split("\\|");    // 分离出度表
        String target = new String();
        Double weight = 0.0;
        Double res = 0.0;
        for (String s : slist) {
            target = s.split(",")[0];   // 贡献影响的人名
            weight = Double.parseDouble(s.split(",")[1]);   // 影响值作为权重
            resKey.set(target);
            res = weight * rank;//得到此人对其造成的影响值
            resValue.set(res.toString());
            context.write(resKey, resValue);  // 发送键值对，key为受影响的人物名字，value为造成的影响值
        }

    }
}

class PageRankReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Text resValue = new Text();
        String relations = new String();
        double sum = 0.0;
        for (Text t : values) {
            String value = t.toString();
            if (value.startsWith("#")) { // 以#开头的为出度表
                relations = value;
            } else {          // 否则为受影响值
                sum += Double.parseDouble(value);//；累加得到其rank值
            }
        }
        resValue.set(String.format("%.6f", sum) + relations);
        context.write(key, resValue);
    }
}

public class PageRank {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("PageRank neet 2 paras as input and output");
            System.exit(2);
        }
        String input = otherArgs[0];                // 输入目录
        String output = new String();
        for (Integer i = 0; i < 15; i++) {     // 迭代15轮
            if (i == 14) {
                output = otherArgs[1] + "final";    // 最终输出
            } else {
                output = otherArgs[1] + i.toString();    // 中间文件输出目录
            }
            Job job = Job.getInstance(conf, "PageRank");
            job.setJarByClass(PageRank.class);
            job.setMapperClass(PageRankMapper.class);
            job.setPartitionerClass(HashPartitioner.class);
            job.setReducerClass(PageRankReducer.class);
            job.setInputFormatClass(KeyValueTextInputFormat.class);// 以/t切割key与value
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setNumReduceTasks(5);                   // 设置五个Reducer节点
            FileInputFormat.addInputPath(job, new Path(input)); // 设置输入路径
            FileOutputFormat.setOutputPath(job, new Path(output));  // 设置输出路径
            job.waitForCompletion(true);    // 等待程序运行完毕
            input = output;
        }
    }
}
