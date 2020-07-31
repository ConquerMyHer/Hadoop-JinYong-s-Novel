package Label;
import java.io.IOException;
import java.text.CollationKey;
import java.text.Collator;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

class LabelResultMapper extends Mapper<Text, Text, Text, Text> {
    // private Text resKey = new Text(); // 要传入reduce的key和value
    private Text rKey = new Text();
    private Text rValue = new Text();

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String name = key.toString();
        String lable = value.toString().split("#")[0];// 去除后面的关系表，只保留前面的标签
        lable = lable.trim();// 去掉前后空白符
        rKey.set(lable);// 将一个标签的人输入到一起
        rValue.set(name);
        context.write(rKey, rValue);
    }
}
class ThreePartition extends Partitioner<Text,Text>{
    @Override
    public int getPartition(Text value1, Text value2, int i) {
        String part1="好";//首字母h
        String part2="跑";//首字母p
        Collator collator = Collator.getInstance(Locale.CHINA);
        CollationKey key1 = collator.getCollationKey(part1);
        CollationKey key2 = collator.getCollationKey(part2);
        CollationKey key = collator.getCollationKey(value1.toString());
        if(key.compareTo(key1)<=0){//首字母比较，分三个区
            return 0;
        }else if(key.compareTo(key2)<=0){
            return 1;
        }
        return 2;
    }
}

class LabelResultReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text name : values) {
            context.write(new Text(name.toString()+","+key.toString()),new Text(""));//中间加一个逗号
        }
    }
}
public class LabelResult {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("PageResult neet 2 paras as input and output");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "LabelResult");
        job.setJarByClass(LabelResult.class); // 设置配置文件信息
        job.setMapperClass(LabelResultMapper.class);
        job.setReducerClass(LabelResultReducer.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);// 以/t切割key与value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //job.setPartitionerClass(ThreePartition.class);//与下面二选一，这里按字母顺序分三个区
        job.setNumReduceTasks(1); // reducer设置为一个，使结果为一个文件
        FileInputFormat.addInputPath(job, new Path(otherArgs[0])); // 设置输入路径
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));// 设置输出路径
        System.exit(job.waitForCompletion(true) ? 0 : 1);// 等待程序结束退出
    }
}
