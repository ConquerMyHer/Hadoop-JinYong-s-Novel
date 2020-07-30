package PreProcess2;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.IOException;

import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

class GetNameMapper extends Mapper<Object, Text, Text, NullWritable> {
    private HashSet<String> name_set = new HashSet<>();// 使用HashSet存储名单，以便快速比对单词与姓名
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        String names = context.getConfiguration().get("Names");
        // 在setup中从configuration中获取名单字符串
        for (String name : names.split(" ")) {
            name_set.add(name);
        }
        // 使用空格分词后依次存入HashSet
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder();// 使用stringBuilder快速对单行有效姓名进行连接
        for (String term : value.toString().split(" ")) {
            if (name_set.contains(term)) {
                sb.append(term);
                sb.append(" ");
            }
        }
        // 使用空格分词，依次比对查找以后将有效姓名append到StringBuilder上

        if (sb.length() > 0) {
            context.write(new Text(sb.toString()), NullWritable.get());
        }
        // 把构建好的单行姓名列表输出
    }
}


class GetNameReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
    public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        for (NullWritable ignored : values) {
            context.write(key, NullWritable.get());
        }
    }
    // 不做操作，直接输出
}


class NameLoader {
    public String load(String filename) throws IOException {
        FileInputStream fIS = new FileInputStream(filename);// 使用文件名初始化文件输入流
        BufferedReader bR = new BufferedReader(new InputStreamReader(fIS));// 使用缓冲区读入文件流

        String ll = "";
        StringBuilder strAllName = new StringBuilder();// 使用StringBuilder快速构建名单
        while((ll = bR.readLine()) != null) {// 获取单行不为空
            strAllName.append(ll);
            strAllName.append(" ");
        }

        fIS.close();
        bR.close();

        return strAllName.toString();// 将构建好的名单字符串转成字符串格式
    }
}


public class GetCharaName {
    public static void main(String []args) throws Exception {
        Configuration myCfig = new Configuration(); // 配置初始化
        String[] argRemain = new GenericOptionsParser(myCfig, args).getRemainingArgs(); // 参数获取
        if (argRemain.length != 2 && argRemain.length != 3) {// 判断参数，错误判断，获取文件位置
            System.err.println("Usage: GetCharaName <in> <out> <<names>>");
            System.exit(2);
        }
        String pathNmFile = "../data/people_name_list.txt";// 给一个默认的文件路径
        if (argRemain.length == 3) {
            pathNmFile = argRemain[2];
        }
        NameLoader loader = new NameLoader();// 使用名单装载器装入名单
        String strAllName = loader.load(pathNmFile);

        Job job = Job.getInstance(myCfig, "GetCharaName");// 开始job，进行一系列设置
        job.setJarByClass(GetCharaName.class);
        job.setMapperClass(GetNameMapper.class);
        job.setReducerClass(GetNameReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.getConfiguration().set("Names", strAllName);// 使用configuration进行名单配置

        FileInputFormat.addInputPath(job, new Path(argRemain[0]));// 配置输入输出路径
        FileOutputFormat.setOutputPath(job, new Path(argRemain[1]));

        System.exit(job.waitForCompletion(true)? 0:1);

    }
}












