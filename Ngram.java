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

      conf.setInt("param.n", Integer.parseInt(args[0]));
      conf.set("query.pathname", args[1]);
      conf.set("xmlStart", "<page>");
      conf.set("xmlEnd", "</page>");
      Job job = new Job(conf, "Ngram");
      job.setJarByClass(Ngram.class);

      // Map Input: <number> <page_content>
      // Map Output: <?> <?>
      // Reduce Input: ?
      // Reduce Output: ?
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(IntWritable.class);
      job.setOutputKeyClass(IntWritable.class);//cnt
      job.setOutputValueClass(Text.class);//title

      job.setMapperClass(NgramMapper.class);
      job.setReducerClass(NgramReducer.class);

      job.setInputFormatClass(XmlInputFormat.class); // XML Input Format
      job.setOutputFormatClass(TextOutputFormat.class);

      FileInputFormat.addInputPath(job, new Path(args[2]));
      FileOutputFormat.setOutputPath(job, new Path(args[3]));

      job.waitForCompletion(true);
   }
   
   public static class NgramMapper extends
         Mapper<LongWritable, Text, Text, IntWritable> {

      private final Hashset queryNgramSet = new HashSet();

      @Override
      protected void setup(Context context) 
            throws IOException, InterruptedException {
         super.setup(context);
         Configuration conf = context.getConfiguration();
         try {
            Path queryPath = new Path(conf.get("query.pathname"));
            final int N = conf.getInt("param.n",0);
            assert N > 0;
            FileSystem fs = FileSystem.get(conf);
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(queryPath)));
            String line;
            int cnt = 0;
            boolean ngramReady = false;
            while ((line = br.readLine()) != null) {
               Tokenizer parser = new Tokenizer(sCurrentLine);
               while (parser.hasNext()) {
                  ngram[cnt++] = new String(parser.next());
                  if (cnt==N) {
                     cnt = 0;
                     ngramReady = true;
                  }
                  if (ngramReady) {
                     String ngramString = ngram[cnt];
                     for (int i=1;i<N;i++) ngramString += " "+ngram[(cnt+i)%N];
                     System.out.println(ngramString);
                     queryNgramSet.add(ngramString);
                  }
               }
            }
         } catch (Exception e) {}
      }
      @Override
      protected void map(LongWritable key, Text value, Context context)
               throws IOException, InterruptedException {
         IntWritable ONE = new IntWritable(1);
         // Pre-processing
         String page = value.toString();
         String titleString=null;
         Pattern pattern = Pattern.compile("<title>(.*?)</title>", Pattern.DOTALL);
         Matcher matcher = pattern.matcher(page);
         if ((matcher.find()) && (matcher.groupCount() > 0)) {
            titleString = matcher.group(1);
            page = page.substring(matcher.end());
         }
         // Evaluate score
         int cnt = 0;
         int score = 0;
         boolean ngramReady = false;
         //Hashset docNgramSet = new HashSet();
         Tokenizer parser = new Tokenizer(page);
         while (parser.hasNext()) {
            ngram[cnt++] = new String(parser.next());
            if (cnt==N) {
               cnt = 0;
               ngramReady = true;
            }
            if (ngramReady) {
               String ngramString = ngram[cnt];
               for (int i=1;i<N;i++) ngramString += " "+ngram[(cnt+i)%N];
               if (queryNgramSet.contains(ngramString)) {
                  score++;
               }
            }
         }
         // Write output
         context.write(new Text(titleString), new IntWritable(score);
      }
   }

   public static class NgramReducer extends
         Reducer<Text, IntWritable, IntWritable, Text> {

      @Override
      public void reduce(Text key, Iterable<IntWritable> values, Context context)
               throws IOException, InterruptedException {
         IntWritable score;
         for (IntWritable val : values) {
            score = val;
            break;
         }
         //IntWritable ONE = new IntWritable(1);
         context.write(score, key);
      }
   }
}
