import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class MovieRelationFinder {

    public static class MovieRelationFinderMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            String[] words = line.split("\t");
            if (words.length < 2) {
                return;
            }
            String[] movieRatings = words[1].split(",");
            for(int i = 0; i < movieRatings.length; i++) {
                for (int j = i; j < movieRatings.length; j++) {
                    StringBuilder sb = new StringBuilder();
                    sb.append(movieRatings[i].split(":")[0]);
                    sb.append(":");
                    sb.append(movieRatings[j].split(":")[0]);
                    String outputKey = sb.toString();
                    context.write(new Text(outputKey), new IntWritable(1));
                }
            }
        }
    }

    public static class MovieRelationFinderReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value: values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(MovieRelationFinder.class);
        job.setMapperClass(MovieRelationFinderMapper.class);
        job.setReducerClass(MovieRelationFinderReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
