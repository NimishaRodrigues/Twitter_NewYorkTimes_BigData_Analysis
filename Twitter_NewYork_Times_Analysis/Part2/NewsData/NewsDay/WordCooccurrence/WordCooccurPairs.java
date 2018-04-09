import java.io.IOException;
import java.util.StringTokenizer;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URI;
import java.util.*;
import java.io.*;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;

public class WordCooccurPairs extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(WordCooccurPairs.class);

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new WordCooccurPairs(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		//Configuration conf = new Configuration();
		Job job = Job.getInstance(getConf(), "word co-occurrence - Pairs Approach");
		for (int i = 0; i < args.length; i += 1) {
			if ("-skip".equals(args[i])) {
				job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
				i += 1;
				job.addCacheFile(new Path(args[i]).toUri());
				// this demonstrates logging
				LOG.info("Added file to the distributed cache: " + args[i]);
			}
		}
		job.setJarByClass(this.getClass());
		// Use TextInputFormat, the default unless job.setInputFormatClass is used
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(PairsMapper.class);
		job.setCombinerClass(PairSumReducer.class);
		job.setReducerClass(PairSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}


	public static class PairsMapper
	extends Mapper<LongWritable, Text, Text, IntWritable>{

		private final static IntWritable one = new IntWritable(1);
		private Text wordpair = new Text();
		private String currentWord ;
		private String nextWord;
		//String[] arr = new String[12];
		private boolean caseSensitive = false;
		private long numRecords = 0;
		private String input;
		private Set<String> patternsToSkip = new HashSet<String>();
		private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

		protected void setup(Mapper.Context context)
				throws IOException,
				InterruptedException {
			if (context.getInputSplit() instanceof FileSplit) {
				this.input = ((FileSplit) context.getInputSplit()).getPath().toString();
			} else {
				this.input = context.getInputSplit().toString();
			}
			Configuration config = context.getConfiguration();
			this.caseSensitive = config.getBoolean("wordcount.case.sensitive", false);
			if (config.getBoolean("wordcount.skip.patterns", false)) {
				URI[] localPaths = context.getCacheFiles();
				parseSkipFile(localPaths[0]);
			}
		}

		private void parseSkipFile(URI patternsURI) {
			LOG.info("Added file to the distributed cache: " + patternsURI);
			try {
				BufferedReader fis = new BufferedReader(new FileReader(new File(patternsURI.getPath()).getName()));
				String pattern;
				while ((pattern = fis.readLine()) != null) {
					patternsToSkip.add(pattern);
				}
			} catch (IOException ioe) {
				System.err.println("Caught exception while parsing the cached file '"
						+ patternsURI + "' : " + StringUtils.stringifyException(ioe));
			}
		}

		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			BufferedReader in = new BufferedReader(new FileReader("english_dict.txt"));
			String temp;
			List<String> list = new ArrayList<String>();
			while((temp = in.readLine()) != null){
				list.add(temp);
			}
			String[] stringArr = list.toArray(new String[list.size()]);	
			
			String[] arr = {"images","take","list","kids","science","arts","master","square","new","watch"};

			String line = lineText.toString();  // based on kpshadoop.blogspot.com/2014/06/word-co-occurrence-problem.html
			line = line.replaceAll("[^\\p{Alpha}]", "\n");
			if (!caseSensitive) {
				line = line.toLowerCase();
			}
			   
			String[] tokens = line.split("\\W+");

			for(int x=0; x<=arr.length-1; x++)
			{
				
				
				for ( int i =0; i<= tokens.length-1;i++)
				{
					
					if (tokens[i].isEmpty() || tokens[i].length() == 1 || patternsToSkip.contains(tokens[i]) || tokens[i].contains("na")) 
					{
						continue;
					}

					else if (ArrayUtils.contains(stringArr, tokens[i]))
					{
						
						currentWord = tokens[i].replaceAll("\\s","");
						
						if (tokens[i].trim().equals(arr[x]))
						{
							for(int j=1; i+j<=tokens.length-1;j++)
							{
								nextWord = tokens[i+j];
								if (nextWord.isEmpty() || nextWord.length() == 1 || arr[x].trim().equals(nextWord) || patternsToSkip.contains(nextWord) || nextWord.contains("na")) 
								{
									continue;
								}
								else if (ArrayUtils.contains(stringArr, nextWord))
								
									{
									wordpair.set(currentWord +" "+ nextWord);
									
								
								context.write(wordpair,one);
									}
								
							}
							
						} 
					}
					
				}
			}

		}


	}

	public static class PairSumReducer
	extends Reducer<Text,IntWritable,Text,IntWritable> {

		@Override
		public void reduce(Text word, Iterable<IntWritable> counts, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable count : counts) {
				sum += count.get();
			}
			context.write(word, new IntWritable(sum));
		}
	}


}