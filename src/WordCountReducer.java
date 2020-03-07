/* WordCountReducer.java
 *  Reducer Class for the Hadoop WordCount Application.
 *  Author: Amr Elzawawy
 *  Created: 3 March 2020
 *  Developed For: Assignment 1 for Netcentric Computing & Distributed Systems Course offering at Spring 2020
 */

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Mapper class takes FOUR types of generics in the following order:
 * KEY_IN: Input Key for Map Function Generic Type
 * VALUE_IN: Input Value for Map Function Generic Type
 * KEY_OUT: Output Key for Map Function Generic Type
 * VALUE_OUT: Output Value for Map Function Generic Type
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable reducedIntegerWritable = new IntWritable(); //here it is a variable integer as our main result.

    /**
     * Reduce Function:
     * Takes the output from Map as an input and combines those data tuples into a smaller set of tuples.
     *
     * @param inputKey    : The input key (word) being reduced in hand.
     * @param inputValues : Set of Tuples Output from the Map Function in Mapper class.
     * @param context     : Allows the Mapper/Reducer to interact with the rest of the Hadoop system.
     *                    It includes configuration data for the job as well as interfaces which allow it to emit output.
     *                    Applications can use the Context: to report progress. to set application-level status messages.
     * @throws IOException          : due to the usage of Mapper.context.write() operation.
     * @throws InterruptedException : due to the usage of Mapper.context.write() operation.
     */
    public void reduce(Text inputKey, Iterable<IntWritable> inputValues, Context context) throws IOException, InterruptedException {
        int wordCount = 0;
        // for each value calculate sum of occurrences to have a word count.
        for (IntWritable value : inputValues)
            wordCount += value.get();
        // create (KEY_OUT, VALUE_OUT) pair.
        reducedIntegerWritable.set(wordCount);
        context.write(inputKey, reducedIntegerWritable);
    }
}
