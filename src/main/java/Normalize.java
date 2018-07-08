import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Normalize {

    public static class NormalizeMapper extends Mapper<LongWritable, Text, Text, Text> {

        // map method
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //InputValue: movieA:movieB \t relation

            //collect the relationship list for movieA
            // outputKey = movieA  outputValue = movieB = relation

            String[] movie_relation = value.toString().trim().split("\t");

            if (movie_relation.length != 2){
                // throw Exception
                return;
            }

            String movieA =  movie_relation[0].split(":")[0];
            String movieB = movie_relation[0].split(":")[1];
            context.write(new Text(movieA),new Text(movieB + "=" + movie_relation[1]));

        }
    }

    public static class NormalizeReducer extends Reducer<Text, Text, Text, Text> {
        // reduce method
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            //inputKey = movieA,
            //inputValue=<movieB=relation, movieC=relation...>
            //normalize each unit of co-occurrence matrix

            // collect all relations -> denominator
            // iterate all movies -> relativeRelation = relation*/denominator
            // outputKey = movieB
            // outputValue = movieA = relativeRelation
            int denominator = 0;
            Map<String, Integer> movieB_relation = new HashMap<String, Integer>();
            // movieB_relation<movieB:relation>
            for (Text value: values){
                // value: movieB* = relation
                String[] movie_relation = value.toString().trim().split("=");
                int relation = Integer.parseInt(movie_relation[1]);
                denominator += relation;

                movieB_relation.put(movie_relation[0],relation);
            }
            for (Map.Entry<String, Integer> entry: movieB_relation.entrySet()){
                //entry: movieB -- relation
                String outputKey = entry.getKey();
                String outputValue = key.toString() + "=" + (double)entry.getValue()/denominator;
                context.write(new Text(outputKey), new Text(outputValue));
            }

        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setMapperClass(NormalizeMapper.class);
        job.setReducerClass(NormalizeReducer.class);

        job.setJarByClass(Normalize.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
