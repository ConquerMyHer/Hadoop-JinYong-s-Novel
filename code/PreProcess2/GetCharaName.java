package PreProcess2;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.IOException;

import java.net.URI;
import java.util.HashSet;

import org.apache.hadoop.myCfig.Configuration;
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

class GetNameMapper extends Mapper<Object, Text, Text, NullWritable> {
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


class GetNameReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
    public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        for (NullWritable ignored : values) {
            context.write(key, NullWritable.get());
        }
    }
}


class NameLoader {
    public String load(String filename) throws IOException {
        FileInputStream fIS = new FileInputStream(filename);
        BufferedReader bR = new BufferedReader(new InputStreamReader(fIS));

        String ll = "";
        StringBuilder strAllName = new StringBuilder();
        while((ll = bR.readLine()) != null) {
            strAllName.append(ll);
            strAllName.append(" ");
        }

        fIS.close();
        bR.close();

        return strAllName.toString();
    }
}


public class GetCharaName {
    public static void main(String []args) throws Exception {
        Configuration myCfig = new Configuration();
        String[] argRemain = new GenericOptionsParser(myCfig, args).getRemainingArgs();
        if (argRemain.length != 2 && argRemain.length != 3) {
            System.err.println("Usage: GetCharaName <in> <out> <<names>>");
            System.exit(2);
        }
        String pathNmFile = "../data/people_name_list.txt";
        if (argRemain.length == 3) {
            pathNmFile = argRemain[2];
        }
        NameLoader loader = new NameLoader();
        String strAllName = loader.load(pathNmFile);

        Job job = Job.getInstance(myCfig, "GetCharaName");
        job.setJarByClass(GetCharaName.class);
        job.setMapperClass(GetNameMapper.class);
        job.setReducerClass(GetNameReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.getConfiguration().set("Names", strAllName);

        FileInputFormat.addInputPath(job, new Path(argRemain[0]));
        FileOutputFormat.setOutputPath(job, new Path(argRemain[1]));

        System.exit(job.waitForCompletion(true)? 0:1);

    }
}












