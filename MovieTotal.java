
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class MovieTotal{
public static class Inputmovies extends Mapper <LongWritable,Text,Text,LongWritable>
{
	public void map(LongWritable key,Text value,Context context) throws InterruptedException, IOException
	{
	
	context.write(new Text("total movies"),new LongWritable(1));
	}
	}

public static class ReduceClass extends Reducer<Text,LongWritable,Text,LongWritable>
{	    
	    public void reduce(Text key,Iterable<LongWritable> value,Context context) throws IOException, InterruptedException 
	      {
	    	int sum = 0;
	      for(LongWritable s:value)
	      {
	    	  sum+=s.get();
	      }
	      context.write(key, new LongWritable(sum));  
}
}

public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
{
	Configuration conf=new Configuration();
	Job job=new Job(conf,"movie count");
	job.setJarByClass(MovieTotal.class);
	job.setMapperClass(Inputmovies.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(LongWritable.class);
	job.setNumReduceTasks(1);
	job.setReducerClass(ReduceClass.class);
    FileInputFormat.addInputPath(job, new Path (args[0]));
    FileOutputFormat.setOutputPath(job, new Path (args[1]));
    System.exit(job.waitForCompletion(true)? 0:1);
}
}


