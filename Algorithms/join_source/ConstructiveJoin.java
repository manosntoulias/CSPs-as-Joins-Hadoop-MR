import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;
import java.io.IOException;
import java.util.Set;
import java.util.StringTokenizer;
import java.io.BufferedReader;
import java.io.FileReader;
import java.lang.Iterable;
import java.io.OutputStreamWriter;
import java.io.BufferedWriter;
import java.util.Arrays;
import java.util.HashSet;

import com.google.common.collect.Iterables;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.NullWritable;

import string.StringManipulation;

import java.net.URI;

public class ConstructiveJoin
{
	private static String separator;

	public static class ConstrMapper extends Mapper<Object, Text, Text, NullWritable> {

		int index_of_var; // current variable that is being joined
		URI uri;
		URI[] uris;
		String cache_filename;
		String filename;
		HashMap<String, ArrayList<String>> current_sls = new HashMap<String, ArrayList<String>>();
		ArrayList<String> a_sol;
		HashMap<String, HashSet<String>> currentSls = new HashMap<String, HashSet<String>>();
		HashSet<String> aSol;
		
		ArrayList<HashMap<String, HashSet<String>>> sls_hashmaps_list =  new ArrayList<HashMap<String, HashSet<String>>>();
		
		String[] indexofkeyvars;
		int[] index_of_key_vars;
		int[][] double_index_key_vars;

		Text key_out = new Text();
		NullWritable null_value = NullWritable.get();
		String line;
		String separator;

		HashSet<String> already_constructed;

		@Override
		public void run(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String cache_solutions = conf.get("method");
			separator = conf.get("separator");
			if (cache_solutions.equals("cache_solutions"))
			{	setup(context);
				while (context.nextKeyValue()) {
				map(context.getCurrentKey(), context.getCurrentValue(), context);
				}
			}
			else // cache input files. split solution files
			{	setup2(context);
				while (context.nextKeyValue()) {
				map2(context.getCurrentKey(), context.getCurrentValue(), context);
				}
			}
			cleanup(context);
		}

		@Override
		protected void setup(Context context) throws IOException
		{
			FileSplit fs = (FileSplit)context.getInputSplit();
			Configuration conf = context.getConfiguration();
			already_constructed = new HashSet<String>();

			URI[] uris = context.getCacheFiles();
			String header = conf.get("header");
			Path cache_path;

			filename = fs.getPath().getName();
			index_of_var = Integer.parseInt(conf.get(filename));
			//System.out.println("split name is: " + filename + "index");
			String index = conf.get(filename + "index");
			if (index == null)
			{	a_sol  = new ArrayList<String>();

				for (int j=0; j < uris.length; j++)
				{	uri = uris[j];
					cache_path = new Path(uri.getPath());
          				cache_filename = cache_path.getName().toString();

					BufferedReader fis = new BufferedReader(new FileReader(cache_filename));
					fis.readLine(); //first line is header
					while ((line = fis.readLine()) != null)
					{	a_sol.add(line);

					}
					fis.close();
				}
				current_sls.put("", a_sol); //for the very first variable
				return;
			}
			indexofkeyvars = index.split(separator); //ERROR
			index_of_key_vars = new int[indexofkeyvars.length];
			
			for (int i=0; i<indexofkeyvars.length; i++)
				index_of_key_vars[i] = Integer.parseInt(indexofkeyvars[i]);

			//System.out.println("\nindexes of file " + filename + " are: " + Arrays.toString(indexofkeyvars) + "\n");
			for (int j=0; j < uris.length; j++)
			{	uri = uris[j];
				cache_path = new Path(uri.getPath());
          			cache_filename = cache_path.getName().toString();
				

				BufferedReader fis = new BufferedReader(new FileReader(cache_filename));
			
				int num_of_vars = fis.readLine().split(separator).length; //first line is header
				while ((line = fis.readLine()) != null)
				{	int current_var=0, i = 0;
					String key_vars = "";
					StringTokenizer itr = new StringTokenizer(line, separator);
					while (i < indexofkeyvars.length - 1)
					{	String token = itr.nextToken();
						if (current_var == index_of_key_vars[i])
						{	key_vars +=  token + separator;
							i++;
						}
						current_var++;
					}
					while (i < indexofkeyvars.length)
					{	String token = itr.nextToken();
						if (current_var == index_of_key_vars[i])
						{	key_vars +=  token;
							i++;
						}
						current_var++;
					}
					if ((a_sol = current_sls.get(key_vars)) == null)
					{	a_sol  = new ArrayList<String>();
						current_sls.put(key_vars, a_sol);
						
					}
					a_sol.add(line);
					
				}
				//current_sls.put("", new ArrayList<String>()); //for the very first variable
				fis.close();
			}
		}


		protected void setup2(Context context) throws IOException
		{
			FileSplit fs = (FileSplit)context.getInputSplit();
			Configuration conf = context.getConfiguration();

			uris = context.getCacheFiles();
			Path cache_path;

			for (int i =0; i < uris.length; i++)
				sls_hashmaps_list.add(new HashMap<String, HashSet<String>>());
			
			String[] files_containing_var = conf.get("all_files").split(separator);
			double_index_key_vars = new int[files_containing_var.length][];
			for (int j =0; j < files_containing_var.length; j++)
			{	String index = conf.get(files_containing_var[j] + "index");
				//System.out.println("index is ->> " + index + " <<-");
				if (index != null)
				{	indexofkeyvars = index.split(separator); //ERROR
					//System.out.println("\nindexes of key vars are " + Arrays.toString(indexofkeyvars) + " for file " + files_containing_var[j] + "\n");
					double_index_key_vars[j] = new int[indexofkeyvars.length];

					for (int i=0; i<indexofkeyvars.length; i++)
						double_index_key_vars[j][i] = Integer.parseInt(indexofkeyvars[i]);
					
				}
				else
				{	double_index_key_vars[j] = new int[0];
						
				}
			}

			//System.out.println("\nindexes of file " + filename + " are: " + Arrays.toString(indexofkeyvars) + "\n");
			for (int j=0; j < uris.length; j++)
			{	uri = uris[j];
				cache_path = new Path(uri.getPath());
          			cache_filename = cache_path.getName().toString();
				index_of_var = Integer.parseInt(conf.get(cache_filename));
				currentSls = sls_hashmaps_list.get(j);

				BufferedReader fis = new BufferedReader(new FileReader(cache_filename));
			
				String header = fis.readLine();
				//if (uris.length == sls_hashmaps_list.size())
					//System.out.println("\nheader of file with index " + j + " is " + header + "\nindex of key variables for file is " + Arrays.toString(double_index_key_vars[j]));
				int num_of_vars = header.split(separator).length; //first line is header
				while ((line = fis.readLine()) != null)
				{

					StringTokenizer itr = new StringTokenizer(line, separator);
					String build_str = "";
					

					int i;
					for (i=0; i<index_of_var-1; i++)
					{
						build_str += itr.nextToken() + separator;
					}
					if (i == index_of_var - 1)
						build_str += itr.nextToken();

					String value_var = itr.nextToken();
			
					//System.out.println("HODOOR ??? --->|" + build_str + "|<---| index: " + index_of_var);
					if ((aSol = currentSls.get(build_str)) == null) //true when build_str == ""
					{
						aSol  = new HashSet<String>();
						currentSls.put(build_str, aSol);
					}
					aSol.add(value_var);
				}
				fis.close();
			}
		}



		@Override
		protected void map(Object keyin, Text valuein, Context context) throws IOException, InterruptedException
		{
			StringTokenizer itr = new StringTokenizer(valuein.toString(), separator);
			String build_str = "";

			int i;
			for (i=0; i<index_of_var-1; i++)
			{
				build_str += itr.nextToken() + separator;
			}
			if (i == index_of_var - 1)
				build_str += itr.nextToken();

			String last_token = itr.nextToken();
			
			//System.out.println("HODOOR ??? --->|" + build_str + "|<---| index: " + index_of_var);
			if ((a_sol = current_sls.get(build_str)) != null) //true when build_str == ""
			{	if (a_sol.size() != 0)
				{
					if (already_constructed.contains(a_sol.get(0) + separator + last_token))
						return;
					else
						already_constructed.add(a_sol.get(0) + separator + last_token);
	
					for (i=0; i< a_sol.size(); i++)
					{	
						key_out.set(a_sol.get(i) + separator + last_token);
						context.write(key_out, null_value);
					}
					//System.out.println("HODOOR rest");
				}
				else
				{	if (!already_constructed.contains(last_token))
					{	already_constructed.add(last_token);
						key_out.set(last_token);
						//System.out.println("HODOOR first ->> " + key_out.toString());
						context.write(key_out, null_value);
					}
					
				}		
			}
		}


		protected void map2(Object keyin, Text valuein, Context context) throws IOException, InterruptedException
		{
			String line = valuein.toString();

			for (int j=0; j < sls_hashmaps_list.size(); j++)
			{	currentSls = sls_hashmaps_list.get(j);
				index_of_key_vars = double_index_key_vars[j];

				int current_var=0, i = 0;
				String key_vars = "";
				StringTokenizer itr = new StringTokenizer(line, separator);
				while (i < index_of_key_vars.length - 1)
				{	String token = itr.nextToken();
					if (current_var == index_of_key_vars[i])
					{	key_vars +=  token + separator;
						i++;
					}
					current_var++;
				}
				while (i < index_of_key_vars.length)
				{	String token = itr.nextToken();
					if (current_var == index_of_key_vars[i])
					{	key_vars +=  token;
						i++;
					}
					current_var++;
				}

				//System.out.println("solution line " + line + " with key_vars ->_" + key_vars + "_<-");
				if ((aSol = currentSls.get(key_vars)) != null)
				{	Iterator<String> itr_aSol = aSol.iterator();
					while (itr_aSol.hasNext())
					//for (i=0; i< aSol.size(); i++)
					{	key_out.set(line + separator + itr_aSol.next());
						context.write(key_out, null_value);
					}
				}
				else
					return;
				
			}
	/**********************************************************/

		}

	}


	public static class ConstrCombiner extends Reducer<Text, NullWritable, Text, NullWritable>
	{
		NullWritable null_value = NullWritable.get();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			
			//System.out.println("\n|^| I'm A COMBINER |^|");
		}

		@Override
		protected void reduce(Text keyin, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException
		{		//System.out.println("map OUTPUT ->> " + keyin.toString() + " <<-");
				
				context.write(keyin, null_value);
		}
	}

	public static class ConstrReducer extends Reducer<Text, NullWritable, Text, NullWritable>
	{
		String[] all_files;
		int num_of_files;
		ArrayList<ArrayList<String>> lists; // one list for each file

		Text out_key = new Text();
		NullWritable null_value = NullWritable.get();
		String separator;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{	Configuration conf = context.getConfiguration();
			separator = conf.get("separator");
			
			all_files = conf.get("all_files").split(separator);
			java.util.Arrays.sort(all_files);
			num_of_files = all_files.length;
			Text header = new Text(conf.get("header"));
			context.write(header, null);
			//System.out.println(header.toString() + " <--- reduce HEADER");
/*
			lists = new ArrayList<ArrayList<String>>();
			for (int i=0; i<num_of_files-1; i++)
			{
				lists.add(new ArrayList<String>());
			}*/
		}

		@Override
		protected void reduce(Text keyin, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException
		{	int count = Iterables.size(values);
			//System.out.println("||| reducer is: " + keyin.toString() + " ||| with length " + count + " again: "+ count);

			if (count == num_of_files)
			{	
				context.write(keyin, null_value);
			}
		}

	}


	public static void cjoin(String input_path, String sep, int numofreducers) throws IOException, InterruptedException, ClassNotFoundException
	{
		if (input_path == null)
		{	System.out.println("Please give input path");
			return;
		}

		separator = sep;

		input_path = StringManipulation.adjust_input(input_path);

       		//long tStart = System.currentTimeMillis();

		Configuration conf = new Configuration();
		conf.set("dfs.replication", "1");
		int num_of_reducers = numofreducers;

		FileSystem fs = FileSystem.get(conf);
		Path dir_pt = new Path(input_path);
		ContentSummary cs = fs.getContentSummary(dir_pt);
		long fileCount = cs.getFileCount();
		System.out.println(fileCount + " <---- NUM OF FILES");

		HashMap<String, ArrayList<String>> var_in_files = StringManipulation.var_in_files(input_path, fs, separator);
		HashMap<String, String> var_pos = StringManipulation.var_positions((int)fileCount);
	
		Iterator<Map.Entry<String, ArrayList<String>>> it = var_in_files.entrySet().iterator();
		while (it.hasNext())
		{
			Map.Entry<String, ArrayList<String>> me = it.next();
			System.out.println("<< " + me + " >>");
		}
		Set<String> keys = var_in_files.keySet();
		ArrayList<String> vars = new ArrayList<String>(keys);
		java.util.Collections.sort(vars);
		for (String el: vars)
			System.out.println("< " + el + " >");

		int  num_of_ = var_in_files.size();

		System.out.println("number of variables is: " + num_of_);

		boolean first_iteration = true;

		//create empty solutions file and cache it with every iteration
		String cache_dirname = "cache_start/";
		Path cache_dirpath = new Path(cache_dirname);
		fs.mkdirs(cache_dirpath);

		String solution_file = cache_dirname + "part-r-00000";
		Path filepath = new Path(solution_file);
		FSDataOutputStream fin = fs.create(filepath);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fin));
		bw.write("DUMMY");
		bw.close();
    		fin.close();

		String header = "";
		String output_path = cache_dirname;
		int cache_fileCount = 1;
		boolean prev_used_r = true; // previous iteration used reducers

		for (String var : vars)
		{
			//if (var.equals("X_02_06"))
				//break;

			ArrayList<String> files = var_in_files.get(var);
			String all_files = "";

			for (int j=0; j< files.size(); j++)
			{
				String file = files.get(j);
				String pos  = var_pos.get(file);
				conf.set(file, pos);
				var_pos.put(file, Integer.toString(Integer.parseInt(pos)+1));
				all_files += file + separator;

				String file_header = StringManipulation.get_header(input_path+file, fs);
				String sol_header_index = StringManipulation.get_sol_header_index(header, file_header, separator);
				conf.set(file+"index", sol_header_index);
				System.out.println("arg set with name: " + file+"index" + " and value: " + sol_header_index);
				//String[] indexofkeyvars = conf.get(filename + "index").split(separator); //TODO
			}
			conf.set("all_files", all_files);
			conf.set("separator", separator);
			conf.set("num_of_files", Integer.toString(files.size()));
			header += var + separator;
			conf.set("header", header.trim());
			
			//CHOOSE METHOD BY SIZE. CACHE INPUT FILES OR SOLUTION FILES
			cache_dirname = output_path;
			long cache_size = (long) fs.getContentSummary(new Path(cache_dirname)).getLength();
			long files_size = 0;
			for (int i =0; i < files.size(); i++)
				files_size += (long) fs.getContentSummary(new Path(input_path + files.get(i))).getLength();
			boolean cache_solutions = true;
			if (cache_size <= files_size)
				conf.set("method", "cache_solutions");
			else
			{	conf.set("method", "cache_input_files");
				cache_solutions = false;
			}


			Job job = Job.getInstance(conf, "c-join");
			job.setJarByClass(ConstructiveJoin.class);
			job.setNumReduceTasks(num_of_reducers);
			job.setMapperClass(ConstrMapper.class);
			if (cache_solutions)
			{	//job.setCombinerClass(ConstrCombiner.class);
				job.setReducerClass(ConstrReducer.class);
				
			}
			else
			{	job.setReducerClass(ConstrReducer.class);
				//job.setCombinerClass(ConstrReducer.class);
				//job.setNumReduceTasks(0);
			}
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(NullWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);

			if (cache_solutions)
			{	//put solution file to cache
				for (int i=0; i < cache_fileCount; i++)
				{	//job.addCacheFile(new Path(cache_dirname).toUri());
					job.addCacheFile(new Path(/*"/user/kyriakosmanos/" + */cache_dirname + "/part-r-" + String.format("%05d", i)).toUri());
					//if (!prev_used_r)
						//job.addCacheFile(new Path(cache_dirname + "/part-m-" + String.format("%05d", i)).toUri());
				}
			}
			else
				//put input files to cache
				for (int i =0; i < files.size(); i++)
					job.addCacheFile(new Path(/*"/user/kyriakosmanos/" + */input_path + files.get(i)).toUri());

			//INPUT
			if (cache_solutions)
				//split input files into filesplits
				for (int j=0; j< files.size(); j++)
				{
					Path pt = new Path(input_path + files.get(j));
					HeaderInputFormat.addInputPath(job, pt);
				}
			else
				//split solution files into filesplits
				for (int j=0; j< cache_fileCount; j++)
				{	Path pt = new Path(cache_dirname + "/part-r-" + String.format("%05d", j));
					//if (!prev_used_r)
						//pt = new Path(cache_dirname + "/part-m-" + String.format("%05d", j));
					HeaderInputFormat.addInputPath(job, pt);
				}
			job.setInputFormatClass(NotSplitHeaderFormat.class);
			
			//OUTPUT
			output_path = "output_" + var;
			FileOutputFormat.setOutputPath(job, new Path(output_path));
	
			Iterator<Map.Entry<String, String>> it2 = var_pos.entrySet().iterator();
			while (it2.hasNext())
			{
				Map.Entry<String, String> me = it2.next();
				System.out.println("<< " + me + " >>");
			}
			System.out.println();

			System.out.println("all done.. exec");
			if (cache_solutions)
				System.out.println("SOLUTION FILES ARE CACHED\n");
			else
				System.out.println("INPUT FILES ARE CACHED\n");
	    		job.waitForCompletion(true);


			first_iteration = false;
			cache_fileCount = num_of_reducers;
			if (cache_solutions)
				prev_used_r = true;
			else
				prev_used_r = false;

			cache_dirpath = new Path(cache_dirname);
			fs.delete(cache_dirpath, true);
		}

	}


	public static void main(String[] arguments) throws IOException, InterruptedException, ClassNotFoundException
	{
		if (arguments.length != 2)
		{	System.out.println("Give input name followed by number of reducers");
			return;
		}
	
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, arguments).getRemainingArgs();

		long tStart = System.currentTimeMillis();

		cjoin(otherArgs[0], "\t", Integer.parseInt(otherArgs[1]));

 	 	long tEnd = System.currentTimeMillis();
         	long tDelta = tEnd - tStart;
         	double elapsedSeconds = tDelta / 1000.0;
         	System.out.println("elapsed time in seconds : "+elapsedSeconds);
	}






}
