import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MapperJoin {
 public static class MyMapper extends Mapper<LongWritable,Text,Text,Text>
 {
	 private Map<String,String> abmap =new HashMap<String,String>();
	 private Map<String,String> abmap1 =new HashMap<String,String>();
	 private Text outputkey =new Text();
	 private Text outputvalue=new Text();
	 protected void setup(Context context) throws IOException, InterruptedException
	 {
		 super.setup(context);
		 URI[] files=context.getCacheFiles();
		 Path p=new Path(files[0]);
		 Path p1=new Path(files[1]);
		 if(p.getName().equals("salary.txt"))
		 {
			 BufferedReader reader=new BufferedReader(new FileReader(p.toString()));
			 String line =reader.readLine();
			 while(line!=null)
			 {
				 String[] tokens=line.split(",");
				 String empid=tokens[0];
				 String empsal=tokens[1];
				 abmap.put(empid, empsal);
				 line=reader.readLine();
			 }
			 reader.close();
		 }
		 if(p1.getName().equals("desig.txt"))
		 {
			 BufferedReader reader=new BufferedReader(new FileReader(p1.toString()));
			 String line=reader.readLine();
			 while(line!=null)
			 {
				 String[] token=line.split(",");
				 String empid =token[0];
				 String empdesig =token[1];
				 abmap1.put(empid, empdesig);
				 line=reader.readLine();
			 }
			 reader.close();
		 }
		 if(abmap.isEmpty()){
			 throw new IOException("mydata :unable to load salary data");
		 }
		 if(abmap1.isEmpty()){
			 throw new IOException("mydata :unable to load designation data");
		 }
 }
	 protected void map(LongWritable key,Text Value,Context context) throws IOException, InterruptedException{
		 String row=Value.toString();
		 String[] token=row.split(",");
		 String empid=token[0];
		 String empsal=abmap.get(empid);
		 String empdesig=abmap1.get(empid);
		 String saldesig=empsal +","+empdesig;
		 outputkey.set(row);
		 outputvalue.set(saldesig);
		 context.write(outputkey,outputvalue);
	 	 }
}
public static void main(String[] arg) throws ClassNotFoundException, IOException, InterruptedException
{
	Configuration conf = new Configuration();
	conf.set("mapreduce.output.textoutputformat.separator",",");
	Job job=Job.getInstance(conf);
	job.setJarByClass(MapperJoin.class);
	job.setJobName("Map side join");
	job.setMapperClass(MyMapper.class);
	job.addCacheFile(new Path("salary.txt").toUri());
	job.addCacheFile(new Path("desig.txt").toUri());
	job.setNumReduceTasks(0);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job,new Path(arg[0]));
	FileOutputFormat.setOutputPath(job, new Path(arg[1]));
	job.waitForCompletion(true);
}
}
	
	
	
	
	
