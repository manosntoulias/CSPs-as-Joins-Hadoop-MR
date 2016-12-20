import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.StringTokenizer;

import calculation.InputOptimization;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import string.StringManipulation;


import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.Partitioner;

public class CascadeJoin {

	private static String separator;

	public static class JoiningMapper extends Mapper<Object, Text, TaggedKey, Text> { //static??

    		private TaggedKey Out_Key = new TaggedKey();
    		private Text data = new Text();
    		private int joinOrder;
		private int reverseOrder;
		private int key_length=0; 
		private ArrayList<Integer> key_raws = new ArrayList<Integer>();
	

	    	@Override
	    	protected void setup(Context context) throws IOException, InterruptedException {       	
				Configuration conf = context.getConfiguration();
				String[] params;
				String[] col_index;


				FileSplit fileSplit = (FileSplit)context.getInputSplit();
				if (fileSplit.getStart() == 0)
					System.out.println("First split BRO!!!!! - in mapper setup");

				System.out.println(fileSplit.getPath().getName() + "-");
				String split_name = fileSplit.getPath().getName();
				String[] temp = conf.get("filename2").split("/");
				String filename2 = temp[temp.length-1];
				if (filename2.compareTo(split_name) == 0)
				{	joinOrder = 1;
					reverseOrder = 0;
				}
				else
				{	joinOrder = 0;
					reverseOrder = 1;
				}

				if (joinOrder == 1)
				{	params = conf.get("file1").split(separator);
					System.out.println("mapper/setup " + joinOrder);
				}
				else
				{	params = conf.get("file2").split(separator);
					System.out.println("mapper/setup " + joinOrder);
				}
				for (String str: params)
				{	System.out.println(str);
					key_raws.add(Integer.valueOf(str));
					}
				for (int num: key_raws)
					if (num > -1)
						key_length++;

			}

	    	@Override
	    	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
				String key_str = "";
				String value_str = "";
				int counter=0, i=0, col_counter=0;
				StringTokenizer itr = new StringTokenizer(value.toString());
				String[] key_array = new String[key_length];

				if (joinOrder == 0)
				{
					for (Integer raw_number: key_raws)
					{
						if (raw_number >= 0)
							key_array[raw_number] = itr.nextToken();
						else
							itr.nextToken();
					}
					data.set(reverseOrder + value.toString().trim());
					// use 1 instead of 0. lines with tag 0 are sorted first and aren't placed in a struct during reduce.
					// saves memory for problems such n queens because second file is
					// the result of previous job that keeps getting larger
				}
				else
				{
					for (Integer raw_number: key_raws)
					{
						if (raw_number >= 0)
							key_array[col_counter++] = itr.nextToken();
						else
							value_str += itr.nextToken() + separator;
					}
					while (itr.hasMoreTokens())
						value_str += itr.nextToken() + separator;
					data.set(reverseOrder + value_str.trim());
				}

				Out_Key.set(Arrays.toString(key_array).replaceAll(",|\\[|\\]", ""), reverseOrder);

					context.write(Out_Key, data);
			}

	}



	public static class JoiningReducer extends Reducer<TaggedKey, Text, Text, Text>
	{
		private ArrayList<Text> aList = new ArrayList<Text>();
		private int file_descriptor;
		private int new_descriptor;
		private String value;
		private Text right_val = new Text();
		private MultipleOutputs<Text, Text> mos;
		private String filename;
		
		@Override
	    	protected void setup(Context context) throws IOException, InterruptedException {       	
			
			Text header = new Text(context.getConfiguration().get("header"));
			filename = context.getConfiguration().get("out_namefile");
			context.write(header, null);	
		}

		public void reduce(TaggedKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			aList.clear();
			char flag = 0;
			value = values.iterator().next().toString();
			file_descriptor = Integer.parseInt(value.substring(0,1));
			
			if (file_descriptor == 1) //no entries from first file. descending order
				return;
			aList.add(new Text(value.substring(1)));

		      	for (Text val : values) {
				
				value = val.toString();

				if (Integer.parseInt(value.substring(0,1)) != file_descriptor)
				{	flag = 1;
					break;
				}
				aList.add(new Text(value.substring(1)));
		      	}
			if (flag == 1)
			{	right_val.set(value.substring(1));
				for (Text list_val : aList)
					context.write(right_val, list_val);
		     	}
			for (Text val : values) {
				right_val.set(val.toString().substring(1));
				for (Text list_val : aList)
				{	
					//mos.write(filename, right_val, list_val);
					context.write(right_val, list_val);
				}
			}/*
			System.out.println(key.getTag() + " <------ TAG");
			for (Text val: values)
				context.write(new Text(key.getJoinKey()), val);*/
    		}

	}


	public static void cascadejoin(String input_path, String sep, int numofreducers) throws IOException, InterruptedException, ClassNotFoundException {


			if (numofreducers < 1)
			{	System.out.println("Number of reducers must be greater than 0");
				return;
			}
			if (input_path == null)
			{	System.out.println("invalid input path");
				return;
			}
			if (sep != "\t" && sep != " ")
			{	System.out.println("Separator must be space or tab");
				return;
			}

			String filename2;
			int i;
			String out_namefile;

			int num_of_reducers = numofreducers;
			separator = sep;
			input_path = StringManipulation.adjust_input(input_path);

            		long tStart = System.currentTimeMillis();

			Configuration conf = new Configuration();
			conf.set("dfs.replication", "1");

			FileSystem fs = FileSystem.get(conf);
			Path dir_pt = new Path(input_path);
			ContentSummary cs = fs.getContentSummary(dir_pt);
			long fileCount = cs.getFileCount();
			System.out.println(fileCount + " <---- NUM OF FILES");
			long num_of_joins = fileCount - 1;

			String[] filenames=new String[(int) fileCount];
			for (i=0 ;i<fileCount ; i++){
				filenames[i] = input_path + (i+1);
			}

			String[] optimal_pair = InputOptimization.Outer(filenames,fs, separator);

			out_namefile = optimal_pair[0];
			filename2 = optimal_pair[1];

			for (i=0; i < num_of_joins; i++)
			{	System.out.println(fileCount + " + 1 <------ NEXT OUT FILENAME");

				//file1
				Path pt=new Path(filename2);
				BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
				String line, header;
				header = line = br.readLine();
				br.close();
				System.out.println(line);

				//file2
				Path pt2_dir;
				Path pt2 = pt2_dir =new Path(out_namefile);
				if (fs.isDirectory(pt2))
				{
					pt2_dir = new Path(out_namefile + "part-r-00000");
				}
				br=new BufferedReader(new InputStreamReader(fs.open(pt2_dir)));
				String line2=br.readLine();
				br.close();
				System.out.println(line2);

			
				//split the two keys
				String[] keys = StringManipulation.Intersection(line, line2, separator);
				if (keys[2] != separator)
					header = line2.trim() + separator + keys[2].trim();
				else
					header = line2;
				System.out.println("HEADER: " + header);
				System.out.println("Keys 1: " + keys[0]);
				System.out.println("Keys 2: " + keys[1]);
			
				//set the job
				//conf = new Configuration();
				//String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

				//set map reduce parameters
				conf.set("file1", keys[0]); // 7
				conf.set("file2", keys[1]); // 8
				conf.set("header", header);
				conf.set("filename2", filename2);
				conf.set("mapreduce.output.textoutputformat.separator", separator);
				out_namefile = String.valueOf(++fileCount);
				conf.set("out_namefile", out_namefile);

				Job job = Job.getInstance(conf, "join");
				job.setJarByClass(CascadeJoin.class);
				job.setMapperClass(JoiningMapper.class);
				job.setReducerClass(JoiningReducer.class);
				// keys/values
				job.setMapOutputKeyClass(TaggedKey.class);
				job.setMapOutputValueClass(Text.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);
				System.out.println("job done.. waiting input");
			
				//PARTINIONER & COMPARATOR
				job.setPartitionerClass(JoiningPartitioner.class);
				job.setGroupingComparatorClass(JoiningGroupingComparator.class);

				// INPUT
				job.setInputFormatClass(HeaderInputFormat.class);
				HeaderInputFormat.addInputPath(job, pt);
				HeaderInputFormat.addInputPath(job, pt2);
			
				//OUTPUT
				job.setNumReduceTasks(num_of_reducers);
				FileOutputFormat.setOutputPath(job, new Path(input_path + out_namefile));
				System.out.println("all done.. exec");
	    			job.waitForCompletion(true);

				if (i > 0)
					fs.delete(pt2, true);
					
				out_namefile = input_path + out_namefile+ "/";
				filename2 = InputOptimization.Inner(filenames , out_namefile , fs, separator);
			}

            long tEnd = System.currentTimeMillis();
            long tDelta = tEnd - tStart;
            double elapsedSeconds = tDelta / 1000.0;
            System.out.println("elapsed time in seconds : "+elapsedSeconds);

			
                        

        }

	public static void main (String [] args) throws IOException, InterruptedException, ClassNotFoundException {
		if (args.length != 2)
			{	System.out.println("Please give input path followed by number of reducers");
				return;
			}

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		cascadejoin(otherArgs[0], "\t", Integer.parseInt(otherArgs[1]));
	}

}
