import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class userAverageRating {
    public static class userAverageRatingMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String line = value.toString().trim();
            String[] words = line.split(",");
            if (words.length < 2) {
                return;
            }
            String user = words[0];
            double rating = Double.parseDouble(words[2]);
            context.write(new Text(user), new DoubleWritable(rating));
        }
    }

    public static class userAverageRatingReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0.00;
            int number = 0;
            for (DoubleWritable value: values) {
                sum += value.get();
                number += 1;
            }
            double average = sum / number;
            context.write(key, new DoubleWritable(average));
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        Job job = Job.getInstance(config);

        job.setJarByClass(Normalizer.class);
        job.setMapperClass(userAverageRatingMapper.class);
        job.setReducerClass(userAverageRatingReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
