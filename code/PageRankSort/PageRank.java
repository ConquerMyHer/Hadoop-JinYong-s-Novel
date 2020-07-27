package PageRankSort;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;


class PageRankMapper extends Mapper<Text, Text, Text, Text> {
    private Text resKey = new Text();      // 要传入reduce的key和value
    private Text resValue = new Text();    
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String name = key.toString();      // 人物名
        Double rank=0.0;
        String relations=new String();
        if(value.toString().contains("#")){// 不是第一次迭代
            rank = Double.parseDouble(value.toString().split("#")[0]); // 旧rank
            relations = value.toString().split("#")[1];       // 出度表
        }
        else{//第一次迭代，先设置默认rank
            rank = 1.0;                // 初始化rank值为1
            relations = value.toString(); // 出度表           
        }
        resKey.set(name);
        resValue.set("#"+relations);
        context.write(resKey, resValue);  // 发送value值为出度表的键值对

        String[] slist = relations.split("\\[|\\]")[1].split("\\|");    // 对出度表中每一个对象发送rank值影响
        String target = new String();
        Double weight = 0.0;
        Double res = 0.0;
        for(String s : slist) {
            target = s.split(",")[0];   // 指向的人物名
            weight = Double.parseDouble(s.split(",")[1]);   // 边权重
            resKey.set(target);
            res = weight*rank;
            resValue.set(res.toString());
            context.write(resKey, resValue);  // 发送键值对，key为受影响人物名，value为造成的影响
        }
       
    }
}

class PageRankReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Text resValue = new Text();
        String relations = new String();
        double sum = 0.0;
        for(Text t : values) {
            String value = t.toString();
            if(value.startsWith("#")) { // 出度表
                relations = value;
            }
            else {          // rank
                sum += Double.parseDouble(value);
            }
        }
        resValue.set(String.format("%.6f", sum)+relations);
        context.write(key, resValue);
    }
}

public class PageRank {
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: PageRank <in> <out>");
            System.exit(2);
        }
        String input = otherArgs[0];                // 输入目录
        String output = new String();
        int max_iter = 15;
        for(Integer i = 0; i < max_iter; i++) {     // 迭代15轮
            output = otherArgs[1]+ i.toString();    // 输出目录
            Job job = new Job(conf, "PageRank");
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
