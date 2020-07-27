package PreProcess2;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.IOException;

import java.net.URI;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

class CharacterMapper extends Mapper<Object, Text, Text, NullWritable> {
    private HashSet<String> name_set = new HashSet<>();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        String names = context.getConfiguration().get("Names");
        for (String name : names.split(" ")) {
            name_set.add(name);
        }
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder();
        for (String term : value.toString().split(" ")) {
            if (name_set.contains(term)) {
                sb.append(term);
                sb.append(" ");
            }
        }

        if (sb.length() > 0) {
            context.write(new Text(sb.toString()), NullWritable.get());
        }
    }
}


class CharacterReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
    public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        for (NullWritable ignored : values) {
            context.write(key, NullWritable.get());
        }
    }
}


class CharacterLoader {
    public String load(String filename) throws IOException {
        FileInputStream inputStream = new FileInputStream(filename);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

        String line, allNames = "";
        while((line = bufferedReader.readLine()) != null) {
            allNames += line + " ";
        }

        inputStream.close();
        bufferedReader.close();

        return allNames;
    }
}


public class CharacterExtracting {
    public static void main(String []args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2 && otherArgs.length != 3) {
            System.err.println("Usage: CharacterExtracting <in> <out> <<names>>");
            System.exit(2);
        }
        String nameFile = "../data/people_name_list.txt";
        if (otherArgs.length == 3) {
            nameFile = otherArgs[2];
        }
        CharacterLoader loader = new CharacterLoader();
        String allNames = loader.load(nameFile);

        Job job = Job.getInstance(conf, "CharacterExtracting");
        job.setJarByClass(CharacterExtracting.class);
        job.setMapperClass(CharacterMapper.class);
        job.setReducerClass(CharacterReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //job.setNumReduceTasks(3);
        job.getConfiguration().set("Names", allNames);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true)? 0:1);
//        FileSystem fs = FileSystem.get(URI.create("hdfs://master01:9000"), conf);
//        FileStatus[] fileStatuses = fs.listStatus(new Path(otherArgs[0]));
//        if (!fs.exists(new Path(otherArgs[1]))) {
//            fs.mkdirs(new Path(otherArgs[1]));
//        }

//        for (int i = 0; i < fileStatuses.length; i++) {
//            Job job = Job.getInstance(conf, "CharacterExtracting");
//            job.setJarByClass(CharacterExtracting.class);
//            job.setMapperClass(CharacterMapper.class);
//            job.setReducerClass(CharacterReducer.class);
//            job.setOutputKeyClass(Text.class);
//            job.setOutputValueClass(NullWritable.class);
//            job.getConfiguration().set("Names", allNames);
//            job.setNumReduceTasks(1);
//
//            FileInputFormat.addInputPath(job, new Path(otherArgs[0] + "/" + fileStatuses[i].getPath().getName()));
//            FileOutputFormat.setOutputPath(job, new Path(otherArgs[1] + "/" + fileStatuses[i].getPath().getName()));
//
//            job.waitForCompletion(true);
//        }

//        for (int i = 0; i < fileStatuses.length; i++) {
//            fs.rename(new Path(otherArgs[1] + "/" + fileStatuses[i].getPath().getName() + "/part-r-00000"),
//                    new Path(otherArgs[1] + "/" + fileStatuses[i].getPath().getName().replace(".txt", "")));
//            fs.delete(new Path(otherArgs[1] + "/" + fileStatuses[i].getPath().getName()), true);
//        }
    }
}












