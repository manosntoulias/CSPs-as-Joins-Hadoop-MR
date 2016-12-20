import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;

import java.lang.InterruptedException;
import java.lang.ClassNotFoundException;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.OutputStreamWriter;
import java.io.BufferedWriter;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import java.net.URI;

public class produce_data_hdfs
{
	public static class permute_mapper extends Mapper<Object, Text, Text, Text>
	{
		ArrayList<Integer> color_order = new ArrayList<Integer>();
		static Integer value;

		@Override
		protected void setup(Context context)
		{
			Configuration conf = context.getConfiguration();
			int num_of_colors = Integer.parseInt(conf.get("num_of_colors"));
			// Initialize first order of colors
			for (int i=0; i<num_of_colors; i++)
				color_order.add(i);
		}

		@Override
		protected void map(Object keyin, Text valuein, Context context) throws IOException, InterruptedException
		{	int column = Integer.parseInt(valuein.toString());
	
			java.util.Collections.swap(color_order, 0, column);
			value = color_order.remove(column);
			color_order.add(1, value);

			permute_store(color_order, 2, context);

			value = color_order.remove(1);
			color_order.add(column, value);
			java.util.Collections.swap(color_order, column, 0);
		}

		static void permute_store(ArrayList<Integer> arr, int k, Context permutations) throws IOException, InterruptedException {
			int i;
			for(i=k; i < arr.size(); i++){
			    value = arr.remove(i);
			    arr.add(k, value);

			    permute_store(arr, k+1, permutations);

			    value = arr.remove(k);
			    arr.add(i, value);
			}
			if (k == arr.size() -1)
			{	if (arr.get(0) != 0)
				{	value = arr.remove(1);
			    		arr.add((int)arr.get(0), value);
				}

				permutations.write(new Text(java.util.Arrays.toString(arr.toArray()).replaceAll(",|\\[|\\]", "")), null);

				if (arr.get(0) != 0)
				{	value = arr.remove((int)arr.get(0));
					arr.add(1, value);
				}
			}
		}
	}


	public static class solutions_mapper extends Mapper<Object, Text, Text, Text>
	{
		static Integer value;

		@Override
		protected void setup(Context context)
		{	
		}

		@Override
		protected void map(Object keyin, Text valuein, Context context) throws IOException, InterruptedException
		{	Configuration conf = context.getConfiguration();
			int num_of_sols = Integer.parseInt(conf.get("num_of_colors")); //sum_of_sols == num_of_colors
			int[] fixed = new int[1];
			fixed[0] = Integer.parseInt(valuein.toString()); 
			int[] sol = init_lin_system(num_of_sols, fixed);

			int num_of_squares = num_of_sols;
			double sum = (num_of_squares+1)*num_of_squares/3-num_of_squares;
			for (int i=0; i<fixed.length; i++)
				sum -= fixed[i]-1;

			context.write(new Text(Integer.toString(fixed[0])), null);
			find_all_solutions(sol, fixed.length, sum, context);
		}

		static int[] init_lin_system(int n, int[] fixed_digits)
		{
			int sum = n*(n+1)/3;
			double m = (n+1)/3.0;
			int[] sol = new int[n];
			int f_len;
			if (fixed_digits == null)	
				f_len = 0;
			else
				f_len = fixed_digits.length;

			if (n % 3 == 1)
			{	System.out.println("Integer must not be divided by (3k+1)");
				System.exit(0);
			}
			else if (n % 3 == 2)
				for (int i=0; i<n; i++)
					sol[i] = (int)m;		
			for (int i=0; i<f_len; i++)
			{	sol[i] = fixed_digits[i];
				sum -= fixed_digits[i];
			}
			for (int i=f_len; i<n; i++)
				sol[i] = 1;
			sum -= n-f_len;
		
			int div = sum/(n-2);
			int mod = sum%(n-2);
			int i;	
			for (i=n-1; i>= n-div; i--)
				sol[i] = n-1;
			sol[i] += mod;

			System.out.println("<<" + java.util.Arrays.toString(sol) + ">>");
			return sol;
		}

		static void find_all_solutions(int[] sol, int fixed_dgts, double sum2, Context context) throws IOException, InterruptedException
		{	int k, n = sol.length;
			int digit, sum, mod2, div;
			long count=0;
			int div2 = (int)sum2 / (n-2);
			mod2 = (int)sum2 % (n-2);
			while (true)
			{	count++;
				
				context.write(new Text(java.util.Arrays.toString(sol).replaceAll(",|\\[|\\]", "")), null);

				/*******************************************************************************/

				for (k=fixed_dgts; sol[k]==n-1; k++);
				if (k - fixed_dgts == div2 && sol[k]-1 == mod2)
				{	System.out.println("iteratively Counted: " + count + " solutions");
					break;
				}
				digit=n-1; sum=-1;
				if (sol[digit] != 1 && sol[digit-1] != n-1)
				{	sol[digit]--;
					sol[digit-1]++;
					continue;
				}
				while (sol[digit] == 1)
				{	
					digit--;
				}
					
				sum += sol[digit] -1;
				sol[digit] = 1;
				digit--;
				while (sol[digit] == n-1)
				{	sum += sol[digit] -1;
					sol[digit] = 1;
					digit--;
				}
				sol[digit]++;	
				div = sum/(n-2);
				int j;
				for (j=n-1; n-1-j<div; j--)
					sol[j] = n-1;
				sol[j] += sum%(n-2);
			}
		}
	}


	public static class input_mapper extends Mapper<Object, Text, Text, Text>
	{
		String perm_name;
		URI uri;
		static Text key_out = new Text();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			Configuration conf = context.getConfiguration();
			DupleSplit fileSplit = (DupleSplit)context.getInputSplit();
			context.nextKeyValue(); //first line is header. Indicates the first digit of the solution
			URI[] uris = context.getCacheFiles();
			
			int num_of_colors = Integer.parseInt(conf.get("num_of_colors"));
			int order = fileSplit.get_order();
			int first_digit = Integer.parseInt(context.getCurrentValue().toString());
			
			//perm_path = conf.get(Integer.toString(num_of_colors - order -1)); //ALTERED
			int offset = Integer.parseInt(conf.get(Integer.toString(num_of_colors - order -1)));
			uri = uris[offset];
			Path perm_path = new Path(uri.getPath());
          		perm_name = perm_path.getName().toString();

			String pair_variables = "";
			
			for (int i=1; i<num_of_colors; i++)
				pair_variables += "X_" + String.format("%02d", i) + "_" + String.format("%02d", (num_of_colors - order - first_digit)) + "\t" +
						"X_" + String.format("%02d", i) + "_" + String.format("%02d", (num_of_colors - order)) + "\t";

			/*for (int i=1; i<=num_of_colors; i++)
				pair_variables += "X_" + i + "_" + (num_of_colors - order) + "\t";
			for (int i=1; i<num_of_colors; i++)
				pair_variables += "X_" + i + "_" + (num_of_colors - order - first_digit) + "\t";*/
			pair_variables += "X_" + String.format("%02d", num_of_colors) + "_" + String.format("%02d", (num_of_colors - order - first_digit)) + "\t" +
					"X_" + String.format("%02d", num_of_colors) + "_" + String.format("%02d", (num_of_colors - order));
			context.write(new Text(pair_variables), null);
		}

		@Override
		protected void map(Object keyin, Text valuein, Context context) throws FileNotFoundException, IOException, InterruptedException
		{
			//TODO string.split(" "). change arraylist to int[]	
			String[] sol_str = valuein.toString().split(" ");
			int[] sol = new int[sol_str.length];
			for (int i=0; i<sol_str.length; i++)
				sol[i] = Integer.parseInt(sol_str[i]);

			int index0;
			int[] used = new int[sol.length];
			int[] pair = new int[sol.length];
/*
			//File f = new File(perm_path); //ALTERED
			File f = new File(uri);

			//Read permute file
			InputStream fis = new FileInputStream(f);
    			InputStreamReader isr = new InputStreamReader(fis);
    			BufferedReader br = new BufferedReader(isr);*/
			String line = null;

			BufferedReader fis = new BufferedReader(new FileReader(perm_name));

			while ((line = fis.readLine()) != null)
			{	String[] arr_str = line.split(" ");
				int[] arr = new int[arr_str.length];
				for (int i=0; i<arr_str.length; i++)
					arr[i] = Integer.parseInt(arr_str[i]);
				for (int i=0; i<pair.length; i++)
					pair[i] = used[i] = 0;
				index0 = pair[0] = arr[0]-sol[0]; used[index0] = 1;
				if ( Math.abs(pair[0] - arr[0]) == sol[0])
				{	
					pair[index0] = 0; used[0] = 1;
					if ( Math.abs(pair[index0] - arr[index0]) == sol[index0])
						valid_dist(arr, 1, sol, context, pair, used);
				}
			}
			fis.close();
		}

		public static long number_of_records = 0;
		static void valid_dist(int[] arr, int k, int[] sol, Context context, int[] pair, int[] used) throws IOException, InterruptedException
		{	if (k == arr.length)
			{
				String record="";
				for (int j=0; j< arr.length-1; j++)
					record += pair[j] + "\t" + arr[j] + "\t";
				record += pair[pair.length -1] + "\t" + arr[arr.length -1];

				//String record = java.util.Arrays.toString(arr).replaceAll(",|\\[|\\]", "") + "\t"+ java.util.Arrays.toString(pair).replaceAll(",|\\[|\\]", "");
				//record = record.replaceAll(" ", "\t");

				key_out.set(record);
				context.write(key_out, null);
				number_of_records++;
				return;
			}
			else if (k == pair[0])
			{	valid_dist(arr, k+1, sol, context, pair, used);
				return;
			}
			if ((pair[k] = arr[k] - sol[k]) >= 0 && used[pair[k]] == 0)
			{	used[pair[k]] = 1;
				valid_dist(arr, k+1, sol, context, pair, used);
				used[pair[k]] = 0;
			}
			if ((pair[k] = arr[k] + sol[k]) < arr.length && used[pair[k]] == 0)
			{	used[pair[k]] = 1;
				valid_dist(arr, k+1, sol, context, pair, used);
				used[pair[k]] = 0;
			}
		}

	}

	public static void main(String[] Args) throws IOException, InterruptedException, ClassNotFoundException
	{
		if (Args.length != 1)
		{	System.out.println("Give number of colors");
			return;
		}

		long tStart = System.currentTimeMillis();

		int num_of_colors = Integer.parseInt(Args[0]);

		Configuration config = new Configuration();
		config.set("num_of_colors", Integer.toString(num_of_colors));
		config.set("dfs.replication", "1");
		FileSystem fs = FileSystem.get(config);
		Job job;


		String duple_dirname = "permute_" + num_of_colors;
		Path dirpath = new Path(duple_dirname);
		fs.mkdirs(dirpath);

		// CREATE DUMMY PERMUTATION FILES. EACH FILE REPRESENTS A MAPPER
		for (int i=1; i<num_of_colors; i++)
		{
			Path filepath = new Path(duple_dirname + "/" + i);
			FSDataOutputStream fin = fs.create(filepath);
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fin));
    			bw.write(Integer.toString(i));
			bw.close();
    			fin.close();	
		}

		//CREATE ACTUAL PERMUTATION FILES
		job = Job.getInstance(config, "produce_data");
		job.setJarByClass(produce_data_hdfs.class);
		job.setMapperClass(permute_mapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, dirpath);
		FileOutputFormat.setOutputPath(job, new Path("dummy_" + num_of_colors));

		job.setNumReduceTasks(0);

		job.waitForCompletion(true);

		// USE ALREADY EXISTING DUMMY PERMUTATIONS

		//CREATE ACTUAL SOLUTION FILES
		//config.set("num_of_colors", Integer.toString(num_of_colors));

		job = Job.getInstance(config, "produce_data");
		job.setJarByClass(produce_data_hdfs.class);
		job.setMapperClass(solutions_mapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, dirpath);
		FileOutputFormat.setOutputPath(job, new Path("dummy_sol_" + num_of_colors));

		job.setNumReduceTasks(0);

		job.waitForCompletion(true);

		// ORDER SOLUTION FILES BASED ON THE FIRST DIGIT OF THE SOLUTION
		

		// CREATE THE COMBINED SOLUTION, PERMUTATION FILES
		for (int i=0; i < num_of_colors -1; i++)
		{	String permute_path = "dummy_" + num_of_colors + "/part-m-" + String.format("%05d", i);
			Path filepath = new Path(permute_path);
			FSDataInputStream fin = fs.open(filepath);
			BufferedReader br = new BufferedReader(new InputStreamReader(fin));
    			String first_digit = br.readLine().split(" ")[0];
			config.set(first_digit, Integer.toString(i)); //ALTERED
			br.close();
    			fin.close();
		}

		job = Job.getInstance(config, "produce_data");
		job.setJarByClass(produce_data_hdfs.class);
		job.setMapperClass(input_mapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		//CACHE
		System.out.println("uri syntax: " + config.get("fs.defaultFS"));
		for (int i=0; i < num_of_colors -1; i++)
			// cluster version 
			//job.addCacheFile(new Path("/user/kyriakosmanos/dummy_" + num_of_colors + "/part-m-" + String.format("%05d", i)).toUri());
			// standalone version
			//job.addCacheFile(new Path(/*config.get("fs.defaultFS") + */"/home/hadoop/Desktop/ptuxiakh/input/Spatially Balanced Latin Squares/dummy_" + num_of_colors + "/part-m-" + String.format("%05d", i)).toUri());
			job.addCacheFile(new Path("dummy_" + num_of_colors + "/part-m-" + String.format("%05d", i)).toUri());

		// files with first column = n-1 must be read 1 time, with first column = n-1 must be read 1 time etc..
		job.setInputFormatClass(DupleInputFormat.class);
		for (int i=0; i<num_of_colors-1; i++)
		{	
			Path pt=new Path("dummy_sol_" + num_of_colors + "/part-m-" + String.format("%05d", i));
			BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
			String line = br.readLine();
			String[] digits = line.split(" ");
			int times = Integer.parseInt(digits[0]);

			for(int j=1; j <= num_of_colors-times; j++)
				FileInputFormat.addInputPath(job, pt);
		}
		FileOutputFormat.setOutputPath(job, new Path("SBL-Squares-" + num_of_colors + "-hdfs"));

		job.setNumReduceTasks(0);
		job.setMaxMapAttempts(3); 

		job.waitForCompletion(true);

		for (int i = 0; i < num_of_colors*(num_of_colors-1)/2; i++)
			fs.rename(new Path("SBL-Squares-" + num_of_colors + "-hdfs/part-m-"+String.format("%05d", i)),
				new Path("SBL-Squares-" + num_of_colors + "-hdfs/" + (i+1)));
		fs.delete(new Path("SBL-Squares-" + num_of_colors + "-hdfs/_SUCCESS"), true);
		fs.delete(new Path("dummy_"+num_of_colors), true);
		fs.delete(new Path("dummy_sol_"+num_of_colors), true);
		fs.delete(new Path("permute_"+num_of_colors), true);

		long tEnd = System.currentTimeMillis();
		long tDelta = tEnd - tStart;
		double elapsedSeconds = tDelta / 1000.0;
		System.out.println("\n Time elapased in seconds: " + elapsedSeconds);
	}

}
