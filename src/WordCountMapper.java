/* WordCountMapper.java
 *  Mapper Class for the Hadoop WordCount Application.
 *  Author: Amr Elzawawy
 *  Created: 3 March 2020
 *  Developed For: Assignment 1 for Netcentric Computing & Distributed Systems Course offering at Spring 2020
 */

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Mapper class takes FOUR types of generics in the following order:
 * KEY_IN: Input Key for Map Function Generic Type
 * VALUE_IN: Input Value for Map Function Generic Type
 * KEY_OUT: Output Key for Map Function Generic Type
 * VALUE_OUT: Output Value for Map Function Generic Type
 */
public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {
    /**
     * Why use those weird Hadoop Types ?
     * String and Integer are simply too "fat." !
     * Their Hadoop Alternatives are Text and IntWritable, respectively.
     * They provide a much easier abstraction on top of byte arrays representing the same type of information.
     */
    private final static IntWritable oneIntegerWritable = new IntWritable(1); //basically int x = 1;
    private Text word = new Text();     //basically a string in the Hadoop programming world.

    /**
     * Map Function:
     * It takes a set of data and converts it into another set of data.
     * Individual elements are broken down into tuples (Key-Value pair).
     *
     * @param inputKey   : The input word currently in hand.
     * @param inputValue : Input Set of data to operate on the word count map.
     * @param context    : Allows the Mapper/Reducer to interact with the rest of the Hadoop system.
     *                   It includes configuration data for the job as well as interfaces which allow it to emit output.
     *                   Applications can use the Context: to report progress. to set application-level status messages.
     * @throws IOException          : due to the usage of Mapper.context.write() operation.
     * @throws InterruptedException : due to the usage of Mapper.context.write() operation.
     */
    public void map(Object inputKey, Text inputValue, Context context) throws IOException, InterruptedException {
        StringTokenizer stringTokenizer = new StringTokenizer(inputValue.toString());
        // for each token in the input data set.
        while (stringTokenizer.hasMoreTokens()) {
            //set the Word to the current token in hand.
            word.set(stringTokenizer.nextToken());
            // create (KEY_OUT, VALUE_OUT) pair.
            context.write(word, oneIntegerWritable);
        }
    }
}
