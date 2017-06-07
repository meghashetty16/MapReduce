import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class STDCalls {
	
	public static class MapClass extends Mapper<LongWritable,Text,Text,IntWritable>
	   {
			Text phoneNumber = new Text();
			//LongWritable phone1 = new LongWritable();
			IntWritable durationInMinutes = new IntWritable();
			
	      public void map(LongWritable key, Text value, Context context)
	      {	    	  
	         try{
	        	 String[] parts = value.toString().split(",");
	        	 if (parts[4].equals("1")) {
	        		 phoneNumber.set(parts[0]);
	        		 //long callerid = Long.parseLong(parts[0]);
	                 String callEndTime = parts[3];
	                 String callStartTime = parts[2];
	                 long duration = toMillis(callEndTime) - toMillis(callStartTime);
	                 durationInMinutes.set((int)(duration/(1000 * 60)));
		             context.write(phoneNumber,durationInMinutes);
	        	 }
	        	 
	         }
	         
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	        private long toMillis(String date) {
	        	 
	            SimpleDateFormat format = new SimpleDateFormat(
	                    "yyyy-MM-dd HH:mm:ss");
	            Date dateFrm = null;
	            try {
	                dateFrm = format.parse(date);

	            } catch (ParseException e) {
	 
	                e.printStackTrace();
	           }
	            return dateFrm.getTime();
	        }

	   }
	
	  public static class ReduceClass extends Reducer<Text,IntWritable,Text,IntWritable>
	   {
	        private IntWritable result = new IntWritable();

		    public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
                long sum = 0;
                for (IntWritable val : values) {
                    sum += val.get();
                }
                if (sum >= 60) {
                	result.set((int) sum);
                	context.write(key, result);
                }
		    }
	   }
	  public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    conf.set("mapreduce.output.textoutputformat.separator",",");
		    Job job = Job.getInstance(conf, "STD Calls");
		    job.setJarByClass(STDCalls.class);
		    job.setMapperClass(MapClass.class);
		    //job.setCombinerClass(ReduceClass.class);
		    job.setReducerClass(ReduceClass.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		    job.setInputFormatClass(TextInputFormat.class);
		    job.setOutputFormatClass(TextOutputFormat.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
}