package jay.mr.collective;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.*;
import java.io.*;
public class ChainedWordCount {

	/**
	 * This hadoop map reduce program reads file from input folder
	 * Each line has two fields name and quote separated by tab
	 * This program generates a output file where each line will show
	 * the name of the person and then list of words and count of occurrence
	 * in his/her quote separated by commas
	 * 
	 * This map reduce program has three phases. Out put of one is the input of subsequent phases
	 * Phase 1: Creates output file of records having  name and used word (> 3 char) in each line
	 * Phase 2: Create output file of records having  name, used word and the count in each line
	 * Phase 3 : Create output file of records containing name and list of words with count using comma delimiter
	 * Input : files in input folder
	 * Output : files in part-r-00000 file in outputCWC folder
	 */

	/*
	 * Mapper class for first phase takes input records from the original input folder
	 * creates records having name  and words of 4 char or more
	 */
	public static class FirstPhaseMap extends Mapper<LongWritable, Text, Text, Text> {
		private Text quoter = new Text();
		private Text word = new Text();
			protected void map(LongWritable key, Text data, Context context) throws IOException, InterruptedException{
				String quoteLine = data.toString();
				int intQuoteStart = quoteLine.lastIndexOf("\t");
				StringTokenizer tokenizer;
				if (intQuoteStart> 1) {
					String name = quoteLine.substring(0,intQuoteStart);
					quoter.set(name);
					String line = quoteLine.substring(intQuoteStart+1);
					tokenizer = new StringTokenizer(line);
					String strToken = "";
					while (tokenizer.hasMoreTokens()){
						strToken = tokenizer.nextToken();
						strToken = strToken.replaceAll("[^A-Za-z]", "").toLowerCase();
						if (strToken.length() > 3) {
							word.set(strToken);
							context.write(quoter, word);
						}
					}
				}
			
			}
		
		}
	
	/*
	 * Mapper class for second phase takes input records from the output folder of phase1 job
	 * For each person name and words of 4 char or more is listed and grouped together
	 */
	public static class SecondPhaseMap extends Mapper<LongWritable, Text, Text, Text> {

		private Text line = new Text();
		private Text count = new Text("1");
		protected void map(LongWritable key, Text data, Context context) throws IOException, InterruptedException {
			String quoteLine = data.toString();

			line.set(quoteLine);
			context.write(line, count);
		}
	}

	/*
	 * This reducer class takes input data from the output of second phase mapper for every name and creates records
	 * as follows 
	 */
	public static class SecondPhaseReduce extends Reducer<Text, Text, Text, Text> {
	
		public void reduce(Text name, Iterable<Text> counts, Context context) throws IOException, InterruptedException {
			int countPerName = 0;
			int curCount = 0;
			String countPerNameStr = "0";
			final Text keyText = new Text(name.toString() + " : ");

			for (Text count : counts) {
				countPerNameStr = count.toString();
				curCount = Integer.parseInt(countPerNameStr);
				countPerName += curCount;
				countPerNameStr = Integer.toString(countPerName);	
			}

		context.write(keyText, new Text(countPerNameStr));
		}
	}
	
	/*
	 * This mapper class takes input data from the output of second phase reducer for every name and creates output
	 * for subsequent reducer ThirdPhaseReduce for each person
	 */	
	public static class ThirdPhaseMap extends Mapper<LongWritable, Text, Text, Text> {
		private Text quoter = new Text();
		private Text countStr = new Text("");
		protected void map(LongWritable key, Text data, Context context) throws IOException, InterruptedException {
		String quoteLine = data.toString();
		int intQuoteStart = quoteLine.indexOf("\t");
		if (intQuoteStart> 1) {
			String name = quoteLine.substring(0,intQuoteStart-1);
			quoter.set(name + "=>");
			String line = quoteLine.substring(intQuoteStart+1,quoteLine.length());
			countStr.set(line);
			context.write(quoter, countStr);
			}
		}
	}
	
	/*
	 * This reducer class takes input data from the output of third phase mapper for every name and creates output
	 * records (final answer) in part-r-00000 file in output folder for each person
	 */	
	public static class ThirdPhaseReduce extends Reducer<Text, Text, Text, Text> {
		
		protected void reduce (Text name, Iterable<Text> wordCounts, Context context) throws IOException, InterruptedException {

		StringBuilder countPerNameStr = new StringBuilder();
		for (Text wordCount : wordCounts) {
			if (countPerNameStr.toString().length() > 0)
				countPerNameStr.append(", ");
			countPerNameStr.append(wordCount.toString());
		}

			context.write(name, new Text(countPerNameStr.toString()));
		}
		
	}
	
	/*
	 * The main Hadoop map reduce program consisting of three cascading jobs phase1Job, phase2Job and phase3Job
	 * in a way phas2 can't start until phas1 finishes and phas3 can't start until phase 2 finishes
	 * Output of phase1 goes as input of phase 2 and output of phase2 goes as input of phase 3
	 * Phase1 doesn't have custom reducer
	 */
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		String crcFileName = ".part-r-00000.crc";
		String inpouFolder = "input";
		String outputFolderFinal = "outputCWC";
		String outputFolderPhase1 = "outputPhase1";
		String outputFolderPhase2 = "outputPhase2";
		
		Job phase1Job = new Job(conf);
		phase1Job.setJarByClass(jay.mr.collective.ChainedWordCount.class);
		phase1Job.setJobName("ChainedWordCount");
		
		phase1Job.setMapperClass(jay.mr.collective.ChainedWordCount.FirstPhaseMap.class);
 	
		phase1Job.setOutputKeyClass(Text.class);
		phase1Job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(phase1Job, new Path(inpouFolder));
		Path outputPath1 = new Path(outputFolderPhase1);
		fs.delete(outputPath1, true);
		FileOutputFormat.setOutputPath(phase1Job, outputPath1);
		phase1Job.waitForCompletion(true);

			
		Job phase2Job = new Job(conf);
		phase2Job.setJarByClass(jay.mr.collective.ChainedWordCount.class);
		phase2Job.setJobName("ChainedWordCount");
		
		phase2Job.setMapperClass(jay.mr.collective.ChainedWordCount.SecondPhaseMap.class);
		 
		phase2Job.setReducerClass(SecondPhaseReduce.class);
 	
		phase2Job.setOutputKeyClass(Text.class);
		phase2Job.setOutputValueClass(Text.class);
		
		// Delete the crc file from previous stage output
		File outFileCRC = new File(outputFolderPhase1, crcFileName).getAbsoluteFile();
		try {
			outFileCRC.delete();
		}
		catch (Exception ex) {
			ex.printStackTrace();
		}
		
		FileInputFormat.setInputPaths(phase2Job, new Path(outputFolderPhase1));
		Path outputPath2 = new Path(outputFolderPhase2);
		fs.delete(outputPath2, true);

		FileOutputFormat.setOutputPath(phase2Job, outputPath2);
		
		phase2Job.waitForCompletion(true);

		
		Job phase3Job = new Job(conf,"ChainedWordCount");
		phase3Job.setJarByClass(jay.mr.collective.ChainedWordCount.class);
		
		phase3Job.setMapperClass(jay.mr.collective.ChainedWordCount.ThirdPhaseMap.class);
		phase3Job.setReducerClass(jay.mr.collective.ChainedWordCount.ThirdPhaseReduce.class);
 	
		phase3Job.setOutputKeyClass(Text.class);
		phase3Job.setOutputValueClass(Text.class);
		// Delete the crc file from previous stage output
		File outFileCRC2 = new File(outputFolderPhase2, crcFileName).getAbsoluteFile();
		try {
			outFileCRC2.delete();
		}
		catch (Exception ex) {
			ex.printStackTrace();
		}
		FileInputFormat.addInputPath(phase3Job, new Path(outputFolderPhase2));
		Path outputPath3 = new Path(outputFolderFinal);
		fs.delete(outputPath3, true);
		FileOutputFormat.setOutputPath(phase3Job, outputPath3);
		phase3Job.waitForCompletion(true);
		// Delete the crc file from previous stage output
		File outFileCRC3 = new File(outputFolderFinal, crcFileName).getAbsoluteFile();
		try {
			outFileCRC3.delete();
		}
		catch (Exception ex) {
			ex.printStackTrace();
		}		
//		System.exit((phase3Job.waitForCompletion(true) ? 0 : 1));
		
	}
}
