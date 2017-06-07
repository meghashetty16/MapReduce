import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
//import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobCounterUpdateEvent.CounterIncrementalUpdate;


public class MovieEachYear{
public static class Inputmovies extends Mapper <LongWritable,Text,Text,IntWritable>
{
	public void map(LongWritable key,Text value,Context context) throws InterruptedException, IOException
	{
	
	String[] line=value.toString().split(",");
	context.write(new Text(line[2]),new IntWritable(1));
	}
	}

public static class ReduceClass extends Reducer<Text,IntWritable,Text,IntWritable>
{	    
	    public void reduce(Text key,Iterable<IntWritable> value,Context context) throws IOException, InterruptedException 
	      {
	    	int sum = 0;
	      for(IntWritable s:value)
	      {
	    	  sum+=s.get();
	      }
	      context.write(key, new IntWritable(sum));  
}
}

public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
{
	Configuration conf=new Configuration();
	Job job=new Job(conf,"movie count");
	job.setJarByClass(MovieEachYear.class);
	job.setMapperClass(Inputmovies.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
	job.setNumReduceTasks(1);
	job.setReducerClass(ReduceClass.class);
    FileInputFormat.addInputPath(job, new Path (args[0]));
    FileOutputFormat.setOutputPath(job, new Path (args[1]));
    System.exit(job.waitForCompletion(true)? 0:1);
}
}

