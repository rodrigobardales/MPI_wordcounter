package Preprocessor;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class PreprocessorMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{
      private final static IntWritable one = new IntWritable(1);
      private final Text word = new T

      public void map(final LongWritable key, final Text value, final OutputCollector<Text, IntWritable> output, 
                  inal Reporter repo rter) throws IOException{
            final String line = value.toString();
            final StringTokenizer tokenizer = new StringTokenizer(
    
            while (tokenizer.hasMoreTokens()){
               word.set(tokenizer.nextToken());
              output.collect(word, one);
            }
       }
}