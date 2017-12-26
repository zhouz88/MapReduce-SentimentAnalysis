import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class SentimentAnalysis {
    public static class SentimentMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Map<String, String> map = new HashMap<String, String>();

        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String path = conf.get("path", "");

            BufferedReader br = new BufferedReader(new FileReader(path));

            String content = br.readLine();
            while (content != null) {
                String[] s = content.split("\t");
                map.put(s[0].toLowerCase().trim(), s[1].trim());
            }
            br.close();
        }

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
             String fileName= ((FileSplit)context.getInputSplit()).getPath().getName();
             String s = value.toString().toLowerCase().trim();
             s = s.replaceAll("[^a-z]", " ");
             String[] line = s.split("\\s+");

             if (line.length < 1)
                 throw new RuntimeException();

             for (String temp : line) {
                 if (map.containsKey(temp)) {
                     context.write(new Text(fileName + "\t" + map.get(temp)), new IntWritable(1) );
                 }
             }
        }
    }

    public static class SentimentReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int total = 0;
            while (values.iterator().hasNext()) {
                total += values.iterator().next().get();
            }
            context.write(key, new IntWritable(total));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.set("path", args[2]);

        Job job = Job.getInstance(configuration);
        job.setJarByClass(SentimentAnalysis.class);
        job.setMapperClass(SentimentMapper.class);
        job.setReducerClass(SentimentReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
