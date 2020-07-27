package Label;
import java.io.IOException;
import java.util.*;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
class LabelAnaMapper extends Mapper<Text, Text, Text, Text> {
    //private Text rkey = new Text();
    //private Text rvalue = new Text();
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String name = key.toString();//名字
        String list = value.toString();//value （标签+邻接表）
        String label = list.split("#")[0];//标签

        String relationlist = "#"+list.split("#")[1];//#+邻接表
        //rkey.set(name);
        //rvalue.set(relations);
        context.write(new Text(name), new Text(relationlist));//发送邻接表维护网络结构
        String[] slist = relationlist.split("\\[|\\]")[1].split("\\|");//将每个关系分离

        for (String s : slist) {
            String target = s.split(",")[0];//和名字主人产生影响的人名
            String weight = s.split(",")[1];//权值
            //keyInfo.set(target);
            //valueInfo.set(label + "|" + weight);
            //context.write(keyInfo, valueInfo);
            String LabelWeight=label+":"+weight;
            context.write(new Text(target), new Text(LabelWeight));
        }
    }
}
class LabelAnaReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
            InterruptedException {
        //String name = key.toString();
        HashMap<String, Double> neighborMap = new HashMap<>();
        String list = "";
        for (Text value : values) {
            //context.write(key, t);
            String tempstr = value.toString();
            if (tempstr.startsWith("#")) {
                list = tempstr.substring(1);//从#后面截取整个邻接表
            } else {
                String l = tempstr.split(":")[0];//标签
                Double w = Double.parseDouble(tempstr.split(":")[1]);//对应权值
                if (!neighborMap.containsKey(l)) {//如果哈希表中没有这个标签
                    neighborMap.put(l, w);//将标签，权值加入哈希表中
                } else {
                    Double m = neighborMap.get(l) + w;//将权值加上
                    neighborMap.put(l, m);
                }
            }
        }

        String label = "";
        Double maxWeight = 0.0;
        for (Map.Entry<String, Double> entry : neighborMap.entrySet()) {
            String keylable = entry.getKey();
            Double veight = entry.getValue();
            if (veight > maxWeight) {//循环比较找到权值最大的标签
                maxWeight = veight;
                label = keylable;
            }
        }



        String reslist = label + "#" + list;
        context.write(key, new Text(reslist));

    }
}

public class LabelAnalyse {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("PageRank neet 2 paras as input and output");
            System.exit(2);
        }
        String input = otherArgs[0];                // 输入目录
        String output = new String();
        int maxIteration = 15;
        for (Integer i = 0; i < maxIteration; i++) {
            if (i == maxIteration-1) {
                output = otherArgs[1] + "final";    // 最终输出
            } else {
                output = otherArgs[1] + i.toString();    // 中间文件输出目录
            }
            Job job = Job.getInstance(conf, "LabelAnalyse");
            job.setJarByClass(LabelAnalyse.class);
            job.setMapperClass(LabelAnaMapper.class);
            job.setReducerClass(LabelAnaReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setInputFormatClass(KeyValueTextInputFormat.class);// 以/t切割key与value
            job.setNumReduceTasks(5);//5个reducer
            //job.getConfiguration().set("labeled", labeled);

            FileInputFormat.addInputPath(job, new Path(input));

            Path path = new Path(output);
            FileSystem fileSystem = path.getFileSystem(conf);// 根据path找到这个文件
            if (fileSystem.exists(path)) {
                fileSystem.delete(path, true);// 设置true，就算output有东西，也一带删除
            }
            FileOutputFormat.setOutputPath(job, new Path(output));

            job.waitForCompletion(true);
            input = output;
        }
    }
}
