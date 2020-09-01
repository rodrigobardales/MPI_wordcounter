# word-count-in-hadoop

## Description

In this repository, we will se how to create a simple wordcount with MapReduce.

## Requisits

1. Hadoop 
2. Eclipse
3. Java

## Install hadoop

To install the hadoop on Unix SO, i recommended this two tutorials: [Installation Video](https://www.youtube.com/watch?v=YY8QL25KCOg) and [Installation Guide](http://www.scratchtoskills.com/install-hadoop-2-7-2-on-ubuntu-15-10-single-node-cluster/)

## Install Eclipse

To install Eclipse, use the [official link](http://www.eclipse.org/downloads/)

## Install Java

```
sudo apt-get install default-jdk
```

## The Word Count

1. Creating WordCount.java

    ```
    import org.apache.hadoop.fs.Path;
    import org.apache.hadoop.conf.*;
    import org.apache.hadoop.io.*;
    import org.apache.hadoop.mapred.*;
    import org.apache.hadoop.util.*;


    public class WordCount extends Configured implements Tool{
          public int run(String[] args) throws Exception
          {
                //creating a JobConf object and assigning a job name for identification purposes
                JobConf conf = new JobConf(getConf(), WordCount.class);
                conf.setJobName("WordCount");

                //Setting configuration object with the Data Type of output Key and Value
                conf.setOutputKeyClass(Text.class);
                conf.setOutputValueClass(IntWritable.class);

                //Providing the mapper and reducer class names
                conf.setMapperClass(WordCountMapper.class);
                conf.setReducerClass(WordCountReducer.class);
                //We wil give 2 arguments at the run time, one in input path and other is output path
                Path inp = new Path(args[0]);
                Path out = new Path(args[1]);
                //the hdfs input and output directory to be fetched from the command line
                FileInputFormat.addInputPath(conf, inp);
                FileOutputFormat.setOutputPath(conf, out);

                JobClient.runJob(conf);
                return 0;
          }

          public static void main(String[] args) throws Exception
          {
                // this main function will call run method defined above.
            int res = ToolRunner.run(new Configuration(), new WordCount(),args);
                System.exit(res);
          }
    }
    ```

2. Creating WordCountMapper.java

    ```
    import java.io.IOException;
    import java.util.StringTokenizer;

    import org.apache.hadoop.io.*;
    import org.apache.hadoop.mapred.*;

    public class WordCountMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>
    {
          //hadoop supported data types
          private final static IntWritable one = new IntWritable(1);
          private Text word = new Text();

          //map method that performs the tokenizer job and framing the initial key value pairs
          // after all lines are converted into key-value pairs, reducer is called.
          public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException
          {
                //taking one line at a time from input file and tokenizing the same
                String line = value.toString();
                StringTokenizer tokenizer = new StringTokenizer(line);

              //iterating through all the words available in that line and forming the key value pair
                while (tokenizer.hasMoreTokens())
                {
                   word.set(tokenizer.nextToken());
                   //sending to output collector which inturn passes the same to reducer
                     output.collect(word, one);
                }
           }
    }
    ```

3. Creating WordCountReducer.java

    ```
    import java.io.IOException;
    import java.util.Iterator;

    import org.apache.hadoop.io.*;
    import org.apache.hadoop.mapred.*;

    public class WordCountReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>
    {
          //reduce method accepts the Key Value pairs from mappers, do the aggregation based on keys and produce the final out put
          public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException
          {
                int sum = 0;
                /*iterates through all the values available with a key and add them together and give the
                final result as the key and sum of its values*/
              while (values.hasNext())
              {
                   sum += values.next().get();
              }
              output.collect(key, new IntWritable(sum));
          }
    }
    ```

4. Fix dependencies errors

    To fix all the dependencies error, we will need to add external .jar. Those files are in the hadoop source folder.
    Click on Project -> Properties -> Librarys -> Add External JARs and select all jars in the following folder:

    ```
    /usr/local/hadoop/share/hadoop/common
    ```
    and
    ```
    /usr/local/hadoop/share/hadoop/mapreduce
    ```

    Now, all the dependencies error were fixed

5. Preparing to create the jar file

    Now, we need to prepare the project to create the jar file. This jar will be necessary to execute in hadoop.

    Click on Run Tab -> Run-configurations -> New Configuration -> Apply

    * Name – WordCountConfig
    * Project – Browse and select your project
    * Main Class – Select WordCount.java

6. Creating the jar file

    Now, we go to File tab -> Export -> Runnable Jar.
        
    * Launch Configuration - Select the configuration above
    * Export destination - Set a destination
    * Library handing - Extract required libraries into generated JAR
        
    Now we have the jar file. 
    Right-click the jar file -> Properties -> Permision -> Check allow executing file as program.
    
7. Run Hadoop

    * Go to hduser: ``` sudo su hduser ```
    * Go to hadoop config folder: ``` cd /usr/local/hadoop/etc/hadoop ```
    * Delete temp folders: ``` sudo rm -R /app/* ``` and ``` sudo rm -R /tmp/* ```
    * Format the namenode: ``` hadoop namenode -format ```
    * Start all daemons: ``` start-dfs.sh && start-yarn.sh ```
    
8. Execute the wordcount.jar

    * Make a hdfs directory (You will can not see this folder in terminal ls): ``` hadoop dfs -mkdir -p /usr/local/hadoop/input ```
    * Copy the input file txt to the hdfs directory: ```  hadoop dfs -copyFromLocal /home/caiogranero/word-count-in-hadoop: sample.txt /usr/local/hadoop/input ```
    * Execute the program: ``` hadoop jar wordcount.jar /usr/local/hadoop/input /usr/local/hadoop/output ```
    * See the result: ``` hdfs dfs -cat /usr/local/hadoop/output/part-00000 ```
    
9. Finish Hadoop
    
    * Finish all daemons: ``` stop-dfs.sh && stop-yarn.sh ```
    
## References

The following tutorial were based on the [Apache Word Count tutorial](https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html)