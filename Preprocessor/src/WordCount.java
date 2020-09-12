import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class WordCount extends Configured implements Tool {

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

		static enum Counters {
			INPUT_WORDS
		}

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		private boolean caseSensitive = true;
		private Set<String> patternsToSkip = new HashSet<String>();

		private long numRecords = 0;
		private String inputFile;

		public void configure(JobConf job) {
			caseSensitive = job.getBoolean("wordcount.case.sensitive", true);
			inputFile = job.get("map.input.file");

			if (job.getBoolean("wordcount.skip.patterns", false)) {
				Path[] patternsFiles = new Path[0];
				try {
					patternsFiles = DistributedCache.getLocalCacheFiles(job);
				} catch (IOException ioe) {
					System.err.println(
							"Caught exception while getting cached files: " + StringUtils.stringifyException(ioe));
				}
				for (Path patternsFile : patternsFiles) {
					parseSkipFile(patternsFile);
				}
			}
		}

		private void parseSkipFile(Path patternsFile) {
			try {
				BufferedReader fis = new BufferedReader(new FileReader(patternsFile.toString()));
				String pattern = null;
				while ((pattern = fis.readLine()) != null) {
					patternsToSkip.add(pattern);
				}
			} catch (IOException ioe) {
				System.err.println("Caught exception while parsing the cached file '" + patternsFile + "' : "
						+ StringUtils.stringifyException(ioe));
			}
		}

		public String min(String a, String b) {

			return a.compareTo(b) > 0 ? b : a;
		}

		public String max(String a, String b) {

			return a.compareTo(b) > 0 ? a : b;
		}

		public String middleTerm(String a, String b, String c) {
			return a.compareTo(b) > 0 ? (a.compareTo(c) < 0 ? a : b.compareTo(c) > 0 ? b : c)
					: (a.compareTo(c) > 0 ? a : (b.compareTo(c) < 0 ? b : c));
		}

		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			String line = (caseSensitive) ? value.toString() : value.toString().toLowerCase();
			String[] wordPatterns = { "is", "of", "on", "in", "to", "an", "with", "without", "we", "they", "how",
					"what", "when", "it", "who", "ok", "okay", "do", "does", "can", "cant", "dont", "doesnt", "want",
					"need", "from", "that", "these", "this", "or", "for", "the", "and", "it" };

			for (int i = 33; i < 256; i++) {
				if ((i > 64 && i < 91) || (i > 96 && i < 123)) {
					char newChar = (char) i;
					String newPattern = " " + newChar + " ";

					line = line.replaceAll(newPattern, " ");
				} else if (i > 47 && i < 58) {
					char newChar = (char) i;
					String newPattern = "" + newChar;

					line = line.replaceAll(newPattern, "");
				} else {
					char newChar = (char) i;
					String newPattern = "\\" + newChar;

					line = line.replaceAll(newPattern, "");
				}
			}

			for (int i = 0; i < wordPatterns.length; i++) {
				line = line.replaceAll(" " + wordPatterns[i] + " ", " ");
			}

			String[] lineWordsTemp = line.split("\\s");
			int deleted = 0;

			for (int i = 0; i < wordPatterns.length; i++) {
				for (int j = 0; j < lineWordsTemp.length; j++) {
					if (wordPatterns[i].equals(lineWordsTemp[j])) {
						lineWordsTemp[j] = " ";
						deleted++;
					}

					for (int k = 33; k < 256; k++) {
						if ((k > 64 && k < 91) || (k > 96 && k < 123)) {
							char newChar = (char) k;
							String newPattern = "" + newChar;

							if (newPattern.equals(lineWordsTemp[j])) {
								lineWordsTemp[j] = " ";
								deleted++;
							}

						} else if (k > 47 && k < 58) {
							char newChar = (char) k;
							String newPattern = "" + newChar;

							if (newPattern.equals(lineWordsTemp[j])) {
								lineWordsTemp[j] = " ";
								deleted++;
							}

						} else {
							char newChar = (char) k;
							String newPattern = "\\" + newChar;

							if (newPattern.equals(lineWordsTemp[j])) {
								lineWordsTemp[j] = " ";
								deleted++;
							}

						}
					}
				}
			}

			String[] lineWords = new String[lineWordsTemp.length - deleted];
			int indexLineWords = 0;

			for (int i = 0; i < lineWordsTemp.length; i++) {
				if (!lineWordsTemp[i].equals(" ")) {
					lineWords[indexLineWords] = lineWordsTemp[i];
					indexLineWords++;
				}
			}

			String firstWord = "";
			String secondWord = "";
			String thirdWord = "";
			for (int i = 0; i < lineWords.length; i++) {
				//one word
				firstWord = lineWords[i];

				word.set(firstWord);
				output.collect(word, one);
				reporter.incrCounter(Counters.INPUT_WORDS, 1);
				for (int j = i + 1; j < lineWords.length; j++) {
					secondWord = lineWords[j];
					//two words
					if (firstWord.equals(secondWord)) {
						continue;
					}

					String keyWord = firstWord + " " + secondWord;
					String inverseKey = secondWord + " " + firstWord;
					if (firstWord.compareTo(secondWord) > 0) {
						keyWord = inverseKey;
					}

					word.set(keyWord);
					output.collect(word, one);
					reporter.incrCounter(Counters.INPUT_WORDS, 1);

					for (int k = j + 1; k < lineWords.length; k++) {
						//three words
						thirdWord = lineWords[k];

						if (firstWord.equals(thirdWord) || secondWord.equals(thirdWord)
								|| firstWord.equals(secondWord)) {
							continue;
						}

						String first = min(min(firstWord, secondWord), thirdWord);
						String last = max(max(firstWord, secondWord), thirdWord);
						String middle = middleTerm(firstWord, secondWord, thirdWord);

						keyWord = first + " " + middle + " " + last;
						word.set(keyWord);
						output.collect(word, one);
						reporter.incrCounter(Counters.INPUT_WORDS, 1);
					}
				}
			}

			if ((++numRecords % 100) == 0) {
				reporter.setStatus(
						"Finished processing " + numRecords + " records " + "from the input file: " + inputFile);
			}
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,
				Reporter reporter) throws IOException {

			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}

			boolean writeKey = true;

			for (int j = 0; j < mostRepeatedWords.length; j++) {
				if (mostRepeatedWords[j].equals(key)) {
					writeKey = false;
					break;
				}
			}

			if (writeKey) {

				for (int i = mostRepeatedValues.length - 1; i >= 0; i--) {
					if (sum > mostRepeatedValues[i] && writeKey) {
						if (i < mostRepeatedValues.length - 1) {
							mostRepeatedValues[i + 1] = mostRepeatedValues[i];
							mostRepeatedWords[i + 1] = mostRepeatedWords[i];
							mostRepeatedValues[i] = 0;
							mostRepeatedWords[i] = "";
						}
						mostRepeatedValues[i] = sum;
						mostRepeatedWords[i] = key.toString();
					}
				}

			}

			output.collect(key, new IntWritable(sum));
		}
	}

	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), WordCount.class);
		conf.setJobName("wordcount");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		List<String> other_args = new ArrayList<String>();
		for (int i = 0; i < args.length; ++i) {
			if ("-skip".equals(args[i])) {
				DistributedCache.addCacheFile(new Path(args[++i]).toUri(), conf);
				conf.setBoolean("wordcount.skip.patterns", true);
			} else {
				other_args.add(args[i]);
			}
		}

		FileInputFormat.setInputPaths(conf, new Path(other_args.get(0)));
		FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));

		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WordCount(), args);

		System.out.println("LA PALABRA MAS REPETIDA ES: " + mostRepeatedWords[3] + " :\t" + mostRepeatedValues[3]);

		System.exit(res);
	}

	static String[] mostRepeatedWords = { "", "", "", "", "" };
	static int[] mostRepeatedValues = { 0, 0, 0, 0, 0 };
}
