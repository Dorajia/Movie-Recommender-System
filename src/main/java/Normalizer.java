import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Normalizer {
    public static class NormalizerMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // movie1:movie2\tnumber => movie1 movie2=number;
            String line = value.toString().trim();
            String[] words = line.split("\t");
            String[] movies = words[0].split(":");
            String outputKey = movies[0];
            String outputValue = movies[1] + "=" + words[1];
            context.write(new Text(outputKey), new Text(outputValue));
        }
    }

    public static class NormalizerReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            Map<String, Integer> map = new HashMap<String, Integer>();
            for (Text value:values) {
                String relationSt = value.toString().trim();
                String[] words = relationSt.split("=");
                String movies = words[0];
                int relation = Integer.parseInt(words[1]);
                map.put(movies, relation);
                sum += relation;
            }

            for (Map.Entry<String, Integer> item: map.entrySet()) {
                double propotion = (double) item.getValue()/sum;
                String outputKey = item.getKey();
                String outputValue = key.toString() + "="+ propotion;
                context.write(new Text(outputKey), new Text(outputValue));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        Job job = Job.getInstance(config);

        job.setJarByClass(Normalizer.class);
        job.setMapperClass(NormalizerMapper.class);
        job.setReducerClass(NormalizerReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
