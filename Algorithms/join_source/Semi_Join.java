import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

import calculation.InputOptimization;
import string.StringManipulation;


public class Semi_Join {

	static String separator;


	public static void Semi_Join(String input_path, String sep, int numofreducers, Configuration config) throws IOException, InterruptedException, ClassNotFoundException
	{

		if (numofreducers < 1)
			{	System.out.println("Number of reducers must be greater than 0");
				return;
			}
		if (input_path == null)
			{	System.out.println("Invalid input path");
				return;
			}
		if (sep != "\t" && sep != " ")
			{	System.out.println("Separator must be space or tab");
				return;
			}
		input_path = StringManipulation.adjust_input(input_path);
		separator = sep;
		int num_of_reducers = numofreducers;

		long tStart = System.currentTimeMillis();

		Configuration conf = config;
		FileSystem fs = FileSystem.get(conf);

		long num_of_joins = InputOptimization.number_of_iterations(input_path, fs);
		String[] filenames = InputOptimization.create_filenames(input_path, num_of_joins + 1);
		//FIND WHICH TWO FILES IS BEST TO JOIN FIRST 
		String[] optimal_pair = InputOptimization.Outer(filenames,fs, separator);

		//GET PATHS OF THE FIRST TWO FILES
		String filename2 = optimal_pair[1];	//1st file. from input path
		String out_namefile = optimal_pair[0];	// 2nd file. the output of each join used as input for next join
							// unless swapped

		long current_num_of_files = num_of_joins + 1; // for n files we have n-1 num of joins
		
		for (int i=0; i < num_of_joins; i++)
		{	String temp_name;
			boolean swapped = false;

			// CACHE THE SMALLER FILE
			// swap files
			if (InputOptimization.compare_files(filename2, out_namefile, fs) == true)
			{	temp_name = filename2;
				filename2 = out_namefile;
				out_namefile = temp_name;
				swapped = true;
			}

			String header1 = StringManipulation.get_header(out_namefile, fs);
			String header2 = StringManipulation.get_header(filename2, fs);
			//GET THE COMMON COLUMNS OF THE TWO FILES. THEY ARE THE KEY FOR THE JOIN
			String[] keys = StringManipulation.Intersection(header2, header1, separator);
			String new_header = StringManipulation.new_header(header2, keys[3], separator); //the header of the output file

			//1st phase
			//MR-JOB TO GET THE VALUES OF THE KEY COLUMNS OF FILENAME2
			hash_semi_join.hash_semi_join(filename2, "./" + i + "-mid-output", keys[0], num_of_reducers, separator, conf);
			//2nd phase
			//MR-JOB TO KEEP THE RECORDS OF OUT_FILENAME THAT ONLY HAVE THE VALUES ABOVE
			semi_join_phase2.semi_join_phase2(out_namefile, "./" + i + "-mid-output", "./" + i + "-mid-output2", header1, keys[1], ",", num_of_reducers, separator, conf);

			fs.delete(new Path("./" + i + "-mid-output"), true);
			if (i != 0 && swapped == false) // if swapped path is still needed. delete it later
				fs.delete(new Path(input_path + current_num_of_files), true);

			//OUTPUT OF 3RD PHASE 
			current_num_of_files++;
			out_namefile = input_path + current_num_of_files + "/";

			//3rd phase
			//MAP ONLY JOB. JOIN THE OUTPUT OF 2ND PHASE WITH FILENAME2
			broadcast_join.broadcast_join(filename2, "./" + i + "-mid-output2", out_namefile, new_header, keys[0], "m", ",", separator, conf);
			

			//FIND WHICH FILE IS BEST TO JOIN WITH THE OUTPUT OF THE LAST JOB
			filename2 = InputOptimization.Inner(filenames , out_namefile , fs, separator);
			
			fs.delete(new Path("./" + i + "-mid-output2"), true);
			if (i != 0 && swapped == true)
				fs.delete(new Path(input_path + (current_num_of_files-1)), true);
			
		}
		long tEnd = System.currentTimeMillis();
         	long tDelta = tEnd - tStart;
         	double elapsedSeconds = tDelta / 1000.0;
         	System.out.println("elapsed time in seconds : "+elapsedSeconds);
	}

	public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException {
		
		Configuration conf = new Configuration();
    		GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
    		String[] otherArgs = optionParser.getRemainingArgs();

		if (otherArgs.length != 2)
			{	System.out.println("Please give input path followed by number of reducers");
				return;
			}
		
		Semi_Join(otherArgs[0], "\t", Integer.parseInt(otherArgs[1]), conf);
	}







}
