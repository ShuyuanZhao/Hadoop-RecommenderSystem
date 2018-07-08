import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class DataDividerByUser {
	public static class DataDividerMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			//input user,movie,rating
			// outputKey = user
            // outputValue movieID: rating
			//divide data by user
            String[] user_movie_rating = value.toString().trim().split(",");
            String outputKey = user_movie_rating[0];
            String outputValue = user_movie_rating[1] + ":" + user_movie_rating[2];
            context.write(new IntWritable(Integer.parseInt(outputKey)), new Text(outputValue));
		}
	}

	public static class DataDividerReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		// reduce method
		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			//merge data for one user
            //Output key = user value = <movie1:rating1, movie2:rating2>
            StringBuilder outputValue = new StringBuilder();
            for (Text value:values){
                outputValue.append(value.toString());
                outputValue.append(",");
            }

            outputValue = outputValue.deleteCharAt(outputValue.length() - 1);
            context.write(key, new Text(outputValue.toString()));
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setMapperClass(DataDividerMapper.class);
		job.setReducerClass(DataDividerReducer.class);

		job.setJarByClass(DataDividerByUser.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}
