package examples;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import util.split.SplitTable;



public class NewWordCountForWikipedia {

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, IntWritable>{
    
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }
  
  public static class IntSumReducer 
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
	int minSize[] = SplitTable.minimumSize;
	long maxSize[] = SplitTable.maximumSize;
	
	//int i = 7;
	//for(int i = 0; i < minSize.length; i++) {
	for(int i = 4; i <= 4; i++) {
		Configuration conf = new Configuration();
	    
	    int minimumSize = minSize[i*8-1];
		//int minimumSize = minSize[4-1];
		conf.setInt("mapred.min.split.size", minimumSize);
		
		long maximumSize = maxSize[i*8-1];
		//long maximumSize = maxSize[4-1];
		conf.setLong("mapred.max.split.size", maximumSize);
	    
conf.setInt("io.sort.mb", 500);
conf.setFloat("io.sort.record.percent", (float) 0.3);
conf.setInt("child.monitor.metrics.seconds", 2);
conf.setBoolean("child.monitor.counters", true);
conf.setInt("child.monitor.jvm.seconds", 2);
conf.setInt("mapred.job.reuse.jvm.num.tasks", 1);

	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: NewWordCountForWikipedia <in> <out>");
	      System.exit(2);
	    }
	    Job job = new Job(conf, "NewWordCountForWikipedia " + i*64 + "MB");
	    job.setJarByClass(NewWordCountForWikipedia.class);
	    job.setMapperClass(TokenizerMapper.class);
	    job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(IntSumReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	    
	    FileSystem.get(conf).delete(new Path(otherArgs[1]), true);
	    
	    job.setNumReduceTasks(1);
	    System.out.println(job.getJobID());
	    job.waitForCompletion(true);
	    
	    Thread.sleep(10000);
	    
	}
    
    
  }
}

