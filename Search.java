import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Search {
	public static class MyMapper extends Mapper <LongWritable,Text,Text,IntWritable>
	
	{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
		{
			String line =value.toString().toLowerCase();
		    String word=context.getConfiguration().get("my text");
		    String text=word.toLowerCase();
		    if(word!=null)
		    {
		    	if(line.contains(text))
		    	{
		    		context.write(new Text (line), new IntWritable (1));
		    	}
		    }
		}
	}
public static class MyReduce extends Reducer <Text,IntWritable,Text,IntWritable>
{
	public void reduce(Text key,Iterable<IntWritable> value,Context context) throws IOException, InterruptedException
	{
		int sum=0;
		for(IntWritable s:value)
		{
			sum+=s.get();
		}
		context.write(new Text(key), new IntWritable(sum));
	}
}
public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
{
	Configuration conf=new Configuration();
	if(args.length>2)
	{
		conf.set("my text", args[2]);
	}
	Job job =new Job (conf,"search word");
	job.setJarByClass(Search.class);
	job.setMapperClass(MyMapper.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(IntWritable.class);
	job.setReducerClass(MyReduce.class);
	FileInputFormat.addInputPath(job, new Path (args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	System.exit(job.waitForCompletion(true)? 0:1);
}
}
