import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxLoginTime {

    public static class LogMapper extends Mapper<Object, Text, Text, IntWritable> {

        private Text user = new Text();
        private IntWritable time = new IntWritable();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split(" ");

            user.set(parts[0]);
            time.set(Integer.parseInt(parts[1]));

            context.write(user, time);
        }
    }

    public static class LogReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private int maxTime = 0;
        private String maxUser = "";

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;

            for (IntWritable val : values) {
                sum += val.get();
            }

            if (sum > maxTime) {
                maxTime = sum;
                maxUser = key.toString();
            }
        }

        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            context.write(new Text(maxUser), new IntWritable(maxTime));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Max Login Time");

        job.setJarByClass(MaxLoginTime.class);

        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
