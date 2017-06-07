import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordCount{
public static class MyMapper extends Mapper <LongWritable,Text,Object,Object>{
	public void map(LongWritable Key,Text Value,Context Context) throws IOException, InterruptedException
	{
		StringTokenizer itr=new StringTokenizer(Value.toString());
		while(itr.hasMoreTokens())
		{
		String word=itr.nextToken().toLowerCase();
		Context.write(new Text(word),new IntWritable(1));
		}
	}
}
public class MyReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
public void reducer(Text key,Iterable<IntWritable> value,Context context) throws IOException, InterruptedException
{
	int sum=0;
	for(IntWritable t:value)
	{
		sum+=t.get();
	}
	context.write(key, new IntWritable(sum));
}
}
public static void main(String[] args) throws IOException, ClassNotFoundException, Exception {
	// TODO Auto-generated method stub
	
	
	Configuration configuration=new Configuration();
	Job job=new Job(configuration, "word count");
	
	job.setJarByClass(WordCount.class);
	job.setMapperClass(MyMapper.class);
	job.setNumReduceTasks(1);
	job.setReducerClass(MyReducer.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(IntWritable.class);
	
	FileInputFormat.addInputPath(job,new Path(args[0]));
	FileOutputFormat.setOutputPath(job,new Path(args[1]));
	
	System.exit(job.waitForCompletion(true)?0:1);
}

}
