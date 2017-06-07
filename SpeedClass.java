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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class SpeedClass {
public static class Inputspeed extends Mapper <LongWritable,Text,Text,IntWritable>
{
	public void map(LongWritable key,Text Value,Context context) throws InterruptedException, IOException
	{
	String[] line=Value.toString().split(",");
	int speed=Integer.valueOf(line[1]);
	String vehicleNumber=line[0];
	
	context.write(new Text(vehicleNumber),new IntWritable(speed));
	}
}
	/*int count=0;
	for(Text s:id)
	{
		
	}
	if(val>65)
	{
		
	context.write(new Text(line[0]), new IntWritable(val));
	
	}
}
}
*/
	public static class SpeedReducer extends Reducer <Text,IntWritable,NullWritable,Text>
	{
	/*	private IntWritable result=new IntWritable();
		//private Text text=new Text();
*/		
		public void reduce(Text key, Iterable<IntWritable> value,Context context) throws IOException, InterruptedException
		{
			
			int vehicleVisited=0;
			final int MAX=65;
			int offence=0;
			
			for(IntWritable speed:value)
			{
				vehicleVisited++;
				if(speed.get()>MAX)
				{
					offence++;
				}
				
			}
			float offencePer = (offence*100)/vehicleVisited;
		String output="The number of times vehicle visited is"+vehicleVisited+"Crossed 65 is"+offence
				+". The Offence Percentage is "+ offencePer;
		context.write(null, new Text(output));
			
		}
	}
	
	
public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
{
	Configuration conf=new Configuration();
	Job job=new Job(conf,"speed count");
	job.setJarByClass(SpeedClass.class);
	job.setMapperClass(Inputspeed.class);
	job.setNumReduceTasks(1);
	job.setReducerClass(SpeedReducer.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path (args[0]));
    FileOutputFormat.setOutputPath(job, new Path (args[1]));
    System.exit(job.waitForCompletion(true)? 0:1);
    
}
}
