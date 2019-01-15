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

public class UserDivider {
    public static class UserDividerMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                //user, movie, rating
            String line = value.toString().trim();
            String[] words = line.split(",");
            if (words.length < 2) {
                return;
            }
            int outputKey = Integer.parseInt(words[0]);
            String outputValue = words[1] + ":" + words[2];
            context.write(new IntWritable(outputKey), new Text(outputValue));
        }

    }

    public static class UserDividerReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws InterruptedException, IOException{
            StringBuilder sb = new StringBuilder();
            for (Text value: values) {
                String valuest = value.toString().trim();
                sb.append(valuest);
                sb.append(",");
            }
            sb.deleteCharAt(sb.length() - 1);
            String outputValue = sb.toString();
            context.write(key, new Text(outputValue));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(UserDivider.class);
        job.setMapperClass(UserDividerMapper.class);
        job.setReducerClass(UserDividerReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);

    }
}
