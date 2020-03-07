/* WordCountApplication.java
 *  Main Application Class for the Hadoop WordCount Application.
 *  Author: Amr Elzawawy
 *  Created: 3 March 2020
 *  Developed For: Assignment 1 for Netcentric Computing & Distributed Systems Course offering at Spring 2020
 */

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
        // Configuration Object: Responsible for the files which are located in the etc/hadoop/ directory.
        Configuration hadoopConfiguration = new Configuration();
        /*
          Job class: It is the most important class in the MapReduce API.
          It allows the user to configure the job, submit it, control its execution, and query the state.
          We create the application, describe the various facets of the job, and then submit the job and monitor its progress.
         */
        Job hadoopJob = Job.getInstance(hadoopConfiguration, "WordCount");
        // Class with Main Function.
        hadoopJob.setJarByClass(WordCountApplication.class);
        // Class with Map Function.
        hadoopJob.setMapperClass(WordCountMapper.class);
        // Class with Reduce Function.
        hadoopJob.setCombinerClass(WordCountReducer.class);
        // Class with Reduce Function.
        hadoopJob.setReducerClass(WordCountReducer.class);
        // Class that is the generic type of Output Key.
        hadoopJob.setOutputKeyClass(Text.class);
        // Class that is the generic type of Output Value.
        hadoopJob.setOutputValueClass(IntWritable.class);
        // Take Input File Path from CLI first argument.
        FileInputFormat.addInputPath(hadoopJob, new Path(args[0]));
        // Take Output File Path from CLI second argument.
        FileOutputFormat.setOutputPath(hadoopJob, new Path(args[1]));
        System.exit(hadoopJob.waitForCompletion(true) ? 0 : 1);
    }
}
