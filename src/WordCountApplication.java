import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountApplication {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // Configuration Object are the files which are located in the etc/hadoop/ directory.
        Configuration conf = new Configuration();
        /* The Job class is the most important class in the MapReduce API.
         * It allows the user to configure the job, submit it, control its execution, and query the state.
         * We create the application, describe the various facets of the job, and then submit the job and monitor its progress.
         */
        Job job = Job.getInstance(conf, "WordCount");
        // Class with Main Function.
        job.setJarByClass(WordCountApplication.class);
        // Class with Map Function.
        job.setMapperClass(WordCountMapper.class);
        // Class with Reduce Function.
        job.setCombinerClass(WordCountReducer.class);
        // Class with Reduce Function.
        job.setReducerClass(WordCountReducer.class);
        // Class that is the generic type of Output Key.
        job.setOutputKeyClass(Text.class);
        // Class that is the generic type of Output Value.
        job.setOutputValueClass(IntWritable.class);
        // Take Input File Path from CLI first argument.
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // Take Output File Path from CLI second argument.
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
