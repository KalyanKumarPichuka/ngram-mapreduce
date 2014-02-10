import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;
import java.util.LinkedHashSet;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Ngram {

   
   public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration();

      conf.setInt("ngramN", Integer.parseInt(args[0]));
      conf.set("queryFile", args[1]);
      conf.set("xmlStart", "<page>");
      conf.set("xmlEnd", "</page>");
      Job job = new Job(conf, "Ngram");
      job.setJarByClass(Ngram.class);

      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(IntWritable.class); //consistant with Mapper<1,2,3,4>types
      job.setOutputKeyClass(IntWritable.class);//cnt
      job.setOutputValueClass(Text.class);//title

      job.setMapperClass(NgramMapper.class);
      job.setReducerClass(NgramReducer.class);

      job.setInputFormatClass(XmlInputFormat.class);//self-implemented, extends TextInputFormat
      job.setOutputFormatClass(TextOutputFormat.class);

      FileInputFormat.addInputPath(job, new Path(args[2]));
      FileOutputFormat.setOutputPath(job, new Path(args[3]));

      job.waitForCompletion(true);
   }
   
   // Mapper class
   public static class NgramMapper extends
         Mapper<LongWritable, Text, Text, IntWritable> {

      @Override
      protected void map(LongWritable key, Text value, Context context)
               throws IOException, InterruptedException {
         IntWritable ONE = new IntWritable(1);

         String page = value.toString();
         String titleString=null;
         Pattern pattern = Pattern.compile("<title>(.*?)</title>", Pattern.DOTALL);
         Matcher matcher = pattern.matcher(page);
         if ((matcher.find()) && (matcher.groupCount() > 0)) {
            titleString = matcher.group(1);
            page = page.substring(matcher.end());
         }
         context.write(new Text(titleString), ONE);
      }
   }
      // Reducer Class
   public static class NgramReducer extends
         Reducer<Text, IntWritable, IntWritable, Text> {

      @Override
      public void reduce(Text key, Iterable<IntWritable> values, Context context)
               throws IOException, InterruptedException {
         IntWritable ONE = new IntWritable(1);
         context.write(ONE, key);
      }
   }
}
