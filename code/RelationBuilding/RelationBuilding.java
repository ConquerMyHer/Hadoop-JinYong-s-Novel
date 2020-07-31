package RelationBuilding;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


class RelationMapper extends Mapper<Text, Text, Text, Text> {
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String str = key.toString();
        String[] names = str.substring(1, str.length() - 1).split(","); // 提取两个人名。
        context.write(new Text(names[0]), new Text(names[1] + " " + value)); // 第一个人名为key，第二个人名和同现次数为value。
    }
}


class RelationReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder newValue = new StringBuilder("["); // 邻接表对应的字符串。
        int sum = 0; // 该人名所有同现次数之和。
        HashMap<String, Integer> weights = new HashMap<>(); // 相应同现人物以及同现次数的Map。
        for (Text value : values) {
            String[] name_weight = value.toString().split(" ");
            Integer weight = Integer.parseInt(name_weight[1]);
            weights.put(name_weight[0], weight);
            sum += weight;
        }
        for (Map.Entry<String, Integer> entry : weights.entrySet()) { // 遍历以生成邻接表。
            double percent = entry.getValue() / (double) sum;
            newValue.append(entry.getKey()).append(",").append(String.format("%.6f", percent)).append("|");
        }
        newValue.setCharAt(newValue.length() - 1, ']');
        context.write(key, new Text(newValue.toString()));
    }
}


public class RelationBuilding {
    public static void main(String []args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: RelationBuilding <in> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "RelationBuilding");
        job.setJarByClass(RelationBuilding.class);
        job.setMapperClass(RelationMapper.class);
        job.setReducerClass(RelationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(5);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true)? 0:1);
    }
}
