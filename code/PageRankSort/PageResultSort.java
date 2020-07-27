package PageRankSort;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

class SortDoubleWritable extends DoubleWritable { // 自定义DoubleWritable类 降序排列
	public SortDoubleWritable() {
		super();
	}

	public SortDoubleWritable(Double d) {
		super(d);
	}

	@Override
	public int compareTo(DoubleWritable o) { // 将返回值置反
		if (this.get() == o.get()) {
			return 0;
		} else if (this.get() > o.get()) {
			return -1;
		} else {
			return 1;
		}
	}
}

class PageSortMapper extends Mapper<Text, Text, SortDoubleWritable, Text> {
	// private Text resKey = new Text(); // 要传入reduce的key和value
	private SortDoubleWritable rKey = new SortDoubleWritable();
	private Text rValue = new Text();

	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		String name = key.toString();
		String rank = value.toString().split("#")[0];// 去除后面的关系表，只保留rank
		rank = rank.trim();// 去掉前后空白符
		rKey.set(Double.parseDouble(rank));// 简单的全排序，只有一个分区
		rValue.set(name);
		context.write(rKey, rValue);
	}
}

class RankSortPartitioner extends HashPartitioner<SortDoubleWritable, Text> {// 重写HashPartitioner
	@Override
	public int getPartition(SortDoubleWritable key, Text value, int numReduceTasks) {// 重载getPartition函数，自定义的SortDoubleWritable类
		// TODO Auto-generated method stub
		return super.getPartition(key, value, numReduceTasks);
	}
	
}

class PageSortReducer extends Reducer<SortDoubleWritable, Text, Text, Text> {
	@Override
    protected void reduce(SortDoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text name : values) {
            context.write(name, new Text(key.toString()));  // 将key和value置反
        }
    }
}

public class PageResultSort {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("PageResult neet 2 paras as input and output");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "PageResultSort");
		job.setJarByClass(PageResultSort.class); // 设置配置文件信息
		job.setMapperClass(PageSortMapper.class);
		job.setPartitionerClass(RankSortPartitioner.class);
		job.setReducerClass(PageSortReducer.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);// 以/t切割key与value
		job.setOutputKeyClass(SortDoubleWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1); // reducer设置为一个，使结果为一个文件
		FileInputFormat.addInputPath(job, new Path(otherArgs[0])); // 设置输入路径
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));// 设置输出路径
		System.exit(job.waitForCompletion(true) ? 0 : 1);// 等待程序结束退出
	}
}
