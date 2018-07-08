import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Sum {

    public static class SumMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        // map method
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //pass data to reducer
            String[] line = value.toString().split("\t");
            context.write(new Text(line[0]),  new DoubleWritable(Double.parseDouble(line[1])));

        }
    }

    public static class SumReducer extends Reducer<Text, DoubleWritable, DBOutputWritable, NullWritable> {

        // reduce method
        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            //user:movie relation
           //calculate the sum
            double sum = 0;
            for (DoubleWritable value: values){
                sum += value.get();
            }
            String[] res = key.toString().trim().split(":");
            context.write(new DBOutputWritable(res[0], res[1], sum), NullWritable.get());
        }
    }

    public static class DBOutputWritable implements DBWritable {

        private String user_id;
        private String movie_id;
        private double user_rate;

        public DBOutputWritable(String user_id, String movie_id, double user_rate)  {
            this.user_id = user_id;
            this.movie_id = movie_id;
            this.user_rate= user_rate;
        }

        public void readFields(ResultSet arg0) throws SQLException {
            this.user_id = arg0.getString(1);
            this.movie_id = arg0.getString(2);
            this.user_rate = arg0.getDouble(3);
        }
        public void write(PreparedStatement arg0) throws SQLException {
            arg0.setString(1, user_id);
            arg0.setString(2, movie_id);
            arg0.setDouble(3, user_rate);

        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        // Use dbConfiguration to configure all the jdbcDriver, db user, db password, database
        DBConfiguration.configureDB(conf,
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://192.168.1.152:8889/test",
                "root",
                "root");


        Job job = Job.getInstance(conf);
        // job2.setJobName("Model");
        job.setJarByClass(Sum.class);

        //How to add external dependency to current project?
		/*
		  1. upload dependency to hdfs
		  2. use this "addArchiveToClassPath" method to define the dependency path on hdfs
		 */
        job.addArchiveToClassPath(new Path("/mysql/mysql-connector-java-5.1.39-bin.jar"));

        job.setOutputKeyClass(DBOutputWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapperClass(SumMapper.class);
        job.setReducerClass(SumReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(DBOutputFormat.class);

        //use dbOutputformat to define the table name and columns
        DBOutputFormat.setOutput(job, "output",
                new String[] {"user_id", "movie_id", "user_rate"});
        //TextOutputFormat.setOutputPath(job, new Path(args[1]));
        TextInputFormat.setInputPaths(job, new Path(args[0]));



        job.waitForCompletion(true);
    }
}
