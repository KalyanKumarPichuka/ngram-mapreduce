/*
 * n-gram text search
 * Hadoop 1.2.1 implementation
 * Author: Ruizhongtai (Charles) Qi
 *
 */

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashSet;
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
      conf.setInt("topk", 20);
      conf.set("query.pathname", args[1]);
      conf.set("xmlStart", "<page>");
      conf.set("xmlEnd", "</page>");
      Job job = new Job(conf, "Ngram");
      job.setJarByClass(Ngram.class);

      // Map Input: <number> <page_content>
      // Map Output: <score> <title>
      // Reduce Input: <score> Iterable<title>
      // Reduce Output: <title> <score>
      job.setMapOutputKeyClass(LongWritable.class);
      job.setMapOutputValueClass(Text.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(LongWritable.class);

      job.setMapperClass(NgramMapper.class);
      job.setReducerClass(NgramReducer.class);

      job.setInputFormatClass(XmlInputFormat.class); // XML Input Format
      job.setOutputFormatClass(TextOutputFormat.class);

      job.setSortComparatorClass(LongWritable.DecreasingComparator.class);

      FileInputFormat.addInputPath(job, new Path(args[2]));
      FileOutputFormat.setOutputPath(job, new Path(args[3]));

      job.waitForCompletion(true);
   }
   
   public static class NgramMapper extends
         Mapper<LongWritable, Text, LongWritable, Text> {

      private final HashSet queryNgramSet = new HashSet();
      private int N;

      @Override
      protected void setup(Context context) 
            throws IOException, InterruptedException {
         super.setup(context);
         Configuration conf = context.getConfiguration();
         try {
            Path queryPath = new Path(conf.get("query.pathname"));
            N = conf.getInt("param.n",0);
            assert N > 0;
            FileSystem fs = FileSystem.get(conf);
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(queryPath)));
            String line;
            int cnt = 0;
            String ngram[] = new String[N];
            boolean ngramReady = false;
            while ((line = br.readLine()) != null) {
               Tokenizer parser = new Tokenizer(line);
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
         String ngram[] = new String[N];
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
         if (score==0) return;
         context.write(new LongWritable(score), new Text(titleString));
      }
   }

   public static class NgramReducer extends
         Reducer<LongWritable, Text, Text, LongWritable> {
      private int topK;
      private int topCnt = 0;

      @Override
      protected void setup(Context context) 
            throws IOException, InterruptedException {
         super.setup(context);
         Configuration conf = context.getConfiguration();
         try {
            topK = conf.getInt("topk",1);
         } catch (Exception e) {}
      }

      @Override
      public void reduce(LongWritable key, Iterable<Text> values, Context context)
               throws IOException, InterruptedException {
         for (Text val : values) {
             if (topCnt<topK) {
                 context.write(val, key);
             } else return;
             topCnt++;
         }
      }
   }
}
