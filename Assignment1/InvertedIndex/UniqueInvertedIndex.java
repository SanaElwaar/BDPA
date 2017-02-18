package invInd;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;
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

public class UniqueInvertedIndex{// extends Configured implements Tools{

	public static class MapInd extends Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();
		private Text docname = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			HashSet<String> stopwords = new HashSet<String>();
			BufferedReader Reader = new BufferedReader(new FileReader( new File("/home/cloudera/workspace/StopWords.txt")));
			String line;
			while ((line = Reader.readLine()) != null) {
				stopwords.add(line.toLowerCase());
			}

			String filenameString = ((FileSplit) context.getInputSplit())
					.getPath().getName();
			docname = new Text(filenameString);

			for (String token : value.toString().split("\\s+")) {
				//skip the words in the stopwords hashset
				if (!stopwords.contains(token.toLowerCase())) {
					word.set(token.toLowerCase());
				}
			}

			context.write(word, docname);
			
		}
	}
	public static class ReduceInd
       extends Reducer<Text,Text,Text,Text> {

		public void reduce(Text key, Iterable<Text> values,
	                       Context context
	                       ) throws IOException, InterruptedException {

			HashSet<String> s = new HashSet<String>();//hashset disable duplication
			for (Text value : values) {
	            s.add(value.toString());
	        }

			StringBuilder builder = new StringBuilder();
	
			String prefix = "";//initialize the prefix that will be written before the first document
			for (String doc : s) {
				builder.append(prefix);
				prefix = ", ";
				builder.append(doc);
			}
	
			context.write(key, new Text(builder.toString()));
	    
	    }
	  }


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "stop words");
    job.setJarByClass(UniqueInvertedIndex.class);
    FileOutputFormat.setCompressOutput(job, true);
	FileOutputFormat.setOutputCompressorClass(job,
			org.apache.hadoop.io.compress.SnappyCodec.class);
	//job.setCompressMapOutput(true);
    job.setMapperClass(MapInd.class);
    job.setCombinerClass(ReduceInd.class);
    job.setReducerClass(ReduceInd.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(10);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

