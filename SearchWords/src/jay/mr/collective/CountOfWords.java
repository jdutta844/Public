package jay.mr.collective;

import java.io.File;
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

public class CountOfWords {

	/**
	 * This hadoop map reduce program reads file from input folder
	 * Each line has two fields name and quote separated by tab
	 * This program generates a output file where each line will show
	 * the name of the word and the count of occurrences in quote lines irrespective of person
	 * Input : files in input folder
	 * Output : file is in part-r-00000 file in outputQOW folder
	 */


		public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException  {
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			String crcFileName = ".part-r-00000.crc";
			String inpouFolder = "input";
			String outputFolderFinal = "outputCOW";		
			
			Job mrjob = new Job(conf);
			mrjob.setJarByClass(jay.mr.collective.QuoteCountByName.class);
			mrjob.setJobName("countofwords");
			
			mrjob.setMapperClass(jay.mr.collective.CountOfWords.CountOfWordsMap.class);
		 
			mrjob.setReducerClass(jay.mr.collective.CountOfWords.CountOfWordsReduce.class);
	 	
			mrjob.setOutputKeyClass(Text.class);
			mrjob.setOutputValueClass(Text.class);
			
			FileInputFormat.setInputPaths(mrjob, new Path(inpouFolder));
			Path outputPath = new Path(outputFolderFinal);
			fs.delete(outputPath, true);

			FileOutputFormat.setOutputPath(mrjob, outputPath);
			
			mrjob.waitForCompletion(true);
			// Delete the crc file from previous stage output
			File outFileCRC = new File(outputFolderFinal, crcFileName).getAbsoluteFile();
			try {
				outFileCRC.delete();
			}
			catch (Exception ex) {
				ex.printStackTrace();
			}		
		}

		/*
		 * Mapper class for phase 1, the very 1st stage generates output like
		 Agatha Christie	when
		 Agatha Christie	what
		 Bette Davis	silent
		 Bette Midler	when
		 Bette Midler	what
		 */
		public static class CountOfWordsMap extends Mapper<LongWritable, Text, Text, Text> {
			private Text word = new Text();
			private Text Count = new Text("1");
			protected void map(LongWritable key, Text data, Context context) throws IOException, InterruptedException {

				
				String quoteLine = data.toString();
				int intQuoteStart = quoteLine.lastIndexOf("\t");
				StringTokenizer tokenizer;
				if (intQuoteStart> 1) {
					String line = quoteLine.substring(intQuoteStart+1);
					tokenizer = new StringTokenizer(line);
					String strToken = "";

					while (tokenizer.hasMoreTokens()){
						strToken = tokenizer.nextToken();
						strToken = strToken.replaceAll("[^A-Za-z]", "").toLowerCase();
						if (strToken.length() > 3) {
							word.set(strToken);
							context.write(word, Count);
						}
					}
				}

			}
		}

		/**
		 * Reducer class, cumulates no of words
		 * The sample output will be like following as per sample in map
		 silent : 1
		 what :	2
		 when :	2
		 * 
		 */

		public static class CountOfWordsReduce extends Reducer<Text, Text, Text, Text> {
			
			protected void reduce (Text name, Iterable<Text> values, Context context) throws IOException, InterruptedException {

				
				int sum = 0;
				int no = 0;
				String tmpCountStr = "0";
				
				for (Text value : values) {
					tmpCountStr = value.toString();
					no = Integer.parseInt(tmpCountStr);
					sum += no;
					tmpCountStr = Integer.toString(sum);			
				}
				context.write(name, new Text(" : " + tmpCountStr));

			}
			
		}
	
}
