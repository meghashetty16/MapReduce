import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class ReducerJoin {
	public static class Mappercust extends Mapper<LongWritable,Text,Text,Text>
	{
		public void map (LongWritable key,Text value,Context context) throws IOException, InterruptedException
		{
			String[] line= value.toString().split(",");
			context.write(new Text(line[0]), new Text ("cust\t"+line[1]));
		}
	}
  public static class MapperTran  extends Mapper<LongWritable,Text,Text,Text>
  {
	  public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
	  {
	  String[] tranline =value.toString().split(",");
	  context.write(new Text(tranline[2]), new Text ("tran\t"+tranline[3]));
	  }
  }
  
  public static class ReduceClass extends Reducer<Text,Text,Text,Text>
  {
	public void reduce(Text key,Iterable<Text> value,Context context) throws IOException, InterruptedException
	{
		double total=0.0;int count=0;
		String name ="";
		for(Text it:value)
		{
			String[] line=it.toString().split("\t");
			if(line[0].equals("tran"))
			{
				
				count++;
				total+=Double.valueOf(line[1]);
			}
			else 
			{
				name=line[1];
			}
			
		}
		String  str =String.format("%d\t%f",count,total);
		context.write(new Text(name), new Text(str));
	}
  }
  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
  {
	  Configuration conf = new Configuration();
	  Job job= Job.getInstance(conf);
	  job.setJarByClass(ReduceClass.class);
	  job.setOutputKeyClass(Text.class);
	  job.setOutputValueClass(Text.class);
	  job.setJobName("Reduceside Join");
	  job.setReducerClass(ReduceClass.class);
	  MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class,Mappercust.class);
	  MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class,MapperTran.class);
	  Path outputpath =new Path(args[2]);
	  FileOutputFormat.setOutputPath(job, outputpath);
	  System.exit(job.waitForCompletion(true)? 0:1);
	  
  }
}
