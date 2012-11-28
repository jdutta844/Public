package jay.mr.collective;

import java.io.File;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.*;
import java.io.*;

public class QuoteCountByName {

	/**
	 * This hadoop map reduce program reads file from input folder
	 * Each line has two fields name and quote separated by tab
	 * This program generates a output file where each line will show
	 * the name of the person and the count of quotes
	 * Input : files in input folder
	 * Output : file is in part-r-00000 file in outputQCBN folder
	 */


		public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException  {
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			String crcFileName = ".part-r-00000.crc";
			String inpouFolder = "input";
			String outputFolderFinal = "outputQCBN";		
			
			Job mrjob = new Job(conf);
			mrjob.setJarByClass(jay.mr.collective.QuoteCountByName.class);
			mrjob.setJobName("quotecountbyname");
			
			mrjob.setMapperClass(jay.mr.collective.QuoteCountByName.QuoteCountByNameMap.class);
		 
			mrjob.setReducerClass(jay.mr.collective.QuoteCountByName.QuoteCountByNameReduce.class);
	 	
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
		 Agatha Christie	1
		 Agatha Christie	1
		 Bette Davis	1
		 Bette Midler	1
		 Bette Midler	1
		 */
		public static class QuoteCountByNameMap extends Mapper<LongWritable, Text, Text, Text> {
			private Text quoter = new Text();
			private Text count = new Text("1");
			protected void map(LongWritable key, Text data, Context context) throws IOException, InterruptedException {
				String quoteLine = data.toString();
				int intQuoteStart = quoteLine.lastIndexOf("\t");
				if (intQuoteStart> 1) {
					String name = quoteLine.substring(0,intQuoteStart);
					quoter.set(name);
					context.write(quoter, count);

				}
			}
		}

		/**
		 * Reducer class, cumulates no of quotes for each person
		 * The sample output will be like following as per sample in map
		 Agatha Christie	2
		 Bette Davis	1
		 Bette Midler	2
		 * 
		 */

		public static class QuoteCountByNameReduce extends Reducer<Text, Text, Text, Text> {
			
			protected void reduce (Text name, Iterable<Text> counts, Context context) throws IOException, InterruptedException {

				int countPerName = 0;
				int curCount = 0;
				String countPerNameStr = "0";
				
				for (Text count : counts) {
					countPerNameStr = count.toString();
					curCount = Integer.parseInt(countPerNameStr);
					countPerName += curCount;
					countPerNameStr = Integer.toString(countPerName);			
				}
				context.write(name, new Text(" : " + countPerNameStr));

			}
			
		}
	
}
