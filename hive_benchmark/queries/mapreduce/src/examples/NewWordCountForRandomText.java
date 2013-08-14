package examples;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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



public class NewWordCountForRandomText {

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, IntWritable>{
    
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private List<String> buffer = new ArrayList<String>();
    
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        //word.set(itr.nextToken());
        //context.write(word, one);
    	buffer.add(itr.nextToken());
      }
    }
    @Override
    protected void cleanup(Context context) throws IOException ,InterruptedException {
    	for(String t : buffer) {
    		word.set(t);
    		context.write(word, one);
    	}
    	buffer.clear();
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
	//for(int c = 1; c <= 5; c++) {
		
	for(int i = 8; i <= 8; i = i + 8) {
	//int i = 31;	
	Configuration conf = new Configuration();
	    
	    int minimumSize = minSize[i-1];
		conf.setInt("mapred.min.split.size", minimumSize);
		
		long maximumSize = maxSize[i-1];
		conf.setLong("mapred.max.split.size", maximumSize);
	    
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: NewWordCountForRamdomText <in> <out>");
	      System.exit(2);
	    }
	    //设置jobtracker
	    //conf.set("mapred.job.tracker", "local");
	    conf.setInt("child.monitor.jvm.seconds", 2);
	    conf.setInt("child.monitor.metrics.seconds", 2);
	    conf.setBoolean("child.monitor.counters", true);
	    //conf.set("mapred.child.java.opts", "-Xmx800m");
	    //
	    Job job = new Job(conf, "NewWordCountForRandomText " + i*8 + "MB");
	    job.setJarByClass(NewWordCountForRandomText.class);
	    job.setMapperClass(TokenizerMapper.class);
	    job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(IntSumReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	    
	    FileSystem.get(conf).delete(new Path(otherArgs[1]), true);
	    
	    job.setNumReduceTasks(2);
	    System.out.println(job.getJobID());
	    job.waitForCompletion(true);
	    
		//Thread.sleep(60000);
	    
	}
	//}
    
  }
}
