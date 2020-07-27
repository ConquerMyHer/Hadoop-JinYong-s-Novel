package Label;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

class LabeliniMapper extends Mapper<Text, Text, Text, Text> {
    private Text rKey = new Text();
    private Text rValue = new Text();
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String name = key.toString();
        String Lablelist = value.toString();
        Lablelist=Lablelist.trim();
        rKey.set(name);
        rValue.set(name+"#"+Lablelist);
        context.write(rKey, rValue);
        //context.write(new Text(name), new Text(name + "#" + list));
    }
}

class LabeliniReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text v : values) {
            context.write(key, v);
        }
    }
}

public class LabelInitial {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("PageResult neet 2 paras as input and output");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "LabelInitial");
        job.setJarByClass(LabelInitial.class);
        job.setMapperClass(LabeliniMapper.class);
        job.setReducerClass(LabeliniReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);// 以/t切割key与value
        //设置任务数据的输入路径；
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        Path path = new Path(otherArgs[1]);
        FileSystem fileSystem = path.getFileSystem(conf);// 根据path找到这个文件
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);// true的意思是，就算output有东西，也一带删除
        }
        //设置任务输出数据的保存路径；
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        //调用job.waitForCompletion(true) 执行任务，执行成功后退出；
        //job.waitForCompletion(true);
        job.waitForCompletion(true);
    }
}

