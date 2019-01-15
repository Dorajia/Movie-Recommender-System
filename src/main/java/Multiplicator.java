import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.map.MultithreadedMapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Multiplicator {
    public static class coCorrencyMatrixMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            String[] words = line.split("\t");
            String outputKey = words[0];
            String outputValue = words[1];
            context.write(new Text(outputKey), new Text(outputValue));

        }

    }

    public static class userRatingMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //user, movie, rating
            String line = value.toString().trim();
            String[] words = line.split(",");
            if (words.length < 2) {
                return;
            }
            String outputKey = words[1];
            String outputValue = words[0] + ":" + words[2];
            context.write(new Text(outputKey), new Text(outputValue));

        }
    }

    public static class MultiplicationReducer extends Reducer<Text, Text, Text, DoubleWritable> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //movie2: user1:rating, movie1:propotion,user2:rating, movie3:propotion
            // user1, movie1 predictionRating
            Map<String, Double> relationMap = new HashMap<String, Double>();
            Map<String, Double> userRating = new HashMap<String, Double>();

            for (Text value:values) {
                String valueSt = value.toString().trim();
                if (valueSt.contains("=")) {
                    String[] words = valueSt.split("=");
                    relationMap.put(words[0], Double.parseDouble(words[1]));
                } else {
                    String[] words = valueSt.split(":");
                    relationMap.put(words[0], Double.parseDouble(words[1]));
                }
            }

            for (Map.Entry<String, Double> entry: relationMap.entrySet()) {
                String movie = entry.getKey();
                double relation = entry.getValue();
                for (Map.Entry<String, Double> userEntry: userRating.entrySet()) {
                    String user = userEntry.getKey();
                    double rating = userEntry.getValue();
                    String outputKey = user + ":" + movie;
                    double outputValue = relation * rating;
                    context.write(new Text(outputKey), new DoubleWritable(outputValue));
                }
            }

         }

    }

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        Job job = Job.getInstance(config);
        job.setJarByClass(Multiplicator.class);

        job.setReducerClass(MultiplicationReducer.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, coCorrencyMatrixMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, userRatingMapper.class);

        TextOutputFormat.setOutputPath(job, new Path(args[2]));


        job.waitForCompletion(true);
    }
}
