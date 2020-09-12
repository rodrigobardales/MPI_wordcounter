import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordPairCount extends Configured implements Tool {
    public static void main(final String[] args) throws Exception {
        final int exitCode = ToolRunner.run(new WordPairCount(), args);
        System.exit(exitCode);
    }

    public int run(final String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s needs two arguments <input> <output> files\n", getClass().getSimpleName());
            return -1;
        }

        final Job job = new Job();
        job.setJarByClass(WordPairCount.class);
        job.setJobName("WordPairCounter");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        final int returnValue = job.waitForCompletion(true) ? 0 : 1;

        if (job.isSuccessful()) {
            System.out.println("Job was successful");
        } else if (!job.isSuccessful()) {
            System.out.println("Job was not successful");
        }

        return returnValue;
    }

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        public void map(final Object key, final Text value, final Context context)
                throws IOException, InterruptedException {
            final String delim = "[^a-z A-Z 0-9]+";
            final String validValue = value.toString().replaceAll(delim, " ");
            String word1 = null, word2 = null;
            final StringTokenizer itr = new StringTokenizer(validValue, " ");

            if (itr.hasMoreTokens()) {
                word1 = new String(itr.nextToken().trim());
            }

            while (itr.hasMoreTokens()) {
                word2 = new String(itr.nextToken().trim());
                if (!word1.isEmpty() && !word2.isEmpty()) {
                    final Text outKey = new Text(word1 + " " + word2);
                    final IntWritable outVal = new IntWritable(1);
                    context.write(outKey, outVal);
                    word1 = new String(word2);
                }
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable();

        public void reduce(final Text key, final Iterable<IntWritable> values, final Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (final IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
}