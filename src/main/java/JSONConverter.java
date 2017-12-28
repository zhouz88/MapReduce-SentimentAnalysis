
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import java.io.*;

public class JSONConverter {

    public static void main(String[] args) throws IOException, JSONException {

        JSONArray jsonArray = new JSONArray();
        /*
        BufferedReader br = new BufferedReader(new FileReader(args[0]));
        */
        Path pt = new Path(args[0]);//Location of file in HDFS
        Path end = new Path(args[1]);
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
        
        String line = br.readLine();
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fs.open(end)));
        
        while (line != null) {
            JSONObject article = new JSONObject();
            String[] title_emotion_count = line.split("\t");
            JSONObject emotionList = new JSONObject();

            emotionList.put(title_emotion_count[1], title_emotion_count[2]);
            article.put("title", title_emotion_count[0]);
            for (int i = 0; i < 2; i++) {
                line = br.readLine();
                title_emotion_count = line.split("\t");
                emotionList.put(title_emotion_count[1], title_emotion_count[2]);
            }
            article.put("data", emotionList);
            jsonArray.put(article);

            line = br.readLine();
        }

        bufferedWriter.write(jsonArray.toString());

        br.close();
        bufferedWriter.close();

    }
}
