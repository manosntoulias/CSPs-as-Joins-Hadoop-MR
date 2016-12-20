import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import string.StringManipulation;

import java.io.*;
import java.util.StringTokenizer;
import java.util.HashSet;
import java.net.URI;

public class semi_join_phase2
{
	static String separator;

	public static class Hash_Key_Mapper extends Mapper<Object, Text, Text, Text>
	{
		HashSet<String> hs = new HashSet<String>();
		int[] key_columns;
		int key_length = 0;

		Text out_data = new Text();
		Text out_key = new Text();

		String separator;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			Configuration conf = context.getConfiguration();
			separator = conf.get("separator");

			Text header = new Text(context.getConfiguration().get("header"));
			context.write(header, null);

			URI[] uris = context.getCacheFiles();

			String[] key_col = conf.get("keys").split(separator);
			
			key_columns = new int[key_col.length];
			for (int i=0; i<key_col.length; i++)
			{	if ((key_columns[i] = Integer.parseInt(key_col[i])) >= 0)
					key_length++;
			}

			for (int i=0; i<uris.length; i++)
			{
				URI uri = uris[i];
				Path perm_path = new Path(uri.getPath());
		  		String hash_path = perm_path.getName().toString();

				File f = new File(hash_path);

				//Read hashtable
				InputStream fis = new FileInputStream(f);
	    			InputStreamReader isr = new InputStreamReader(fis);
	    			BufferedReader br = new BufferedReader(isr);
				String line;

				while ((line = br.readLine()) != null)
				{
					hs.add(line);
				}
				fis.close();
			}
			
		}

		@Override
	    	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			int i, j;
			String construct_data = "";
			StringTokenizer itr = new StringTokenizer(value.toString());
			String[] key_array = new String[key_length];
			
			for (i=0, j=0; j<key_columns.length -1 - (key_array.length-i); j++)		
			{
				if (key_columns[j] >= 0)
				{	key_array[key_columns[j]] = itr.nextToken();
					i++;
				}
				else
					construct_data += itr.nextToken() + separator;
			}
			for (; j <key_columns.length; j++)
				if (key_columns[j] >= 0)			
					key_array[key_columns[j]] = itr.nextToken();		
				else
					construct_data += itr.nextToken();

			String key_string = "";//StringManipulation.Table_to_string(key_array, separator);

			for (int k=0; k<key_length-1; k++)
				key_string += key_array[k] + separator;
			key_string += key_array[key_length -1];

			//System.out.println("mapper: T " + key_string + " T " + separator + " T " + construct_data + " T");
			if (hs.contains(key_string))
			{	
				out_data.set(construct_data);
				out_key.set(key_string);
				context.write(out_key, out_data);
			}
		}
	}


	public static class Keep_Key_Reducer extends Reducer<Text, Text, Text, Text>
	{
		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			Text header = new Text(context.getConfiguration().get("header"));
			context.write(header, null);
		}

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			for (Text value: values )
			{	//System.out.println(key + separator + value);
				context.write(key, value);
			}
		}
	
	}


	public static void semi_join_phase2(String file1, String file2, String output, String header, String keys, String mr_separator, int num_of_reducers, String sep, Configuration config) throws IOException, InterruptedException, ClassNotFoundException
	{	separator = sep;
		
		Configuration conf = new Configuration();
		conf.set("dfs.replication", "1");
		FileSystem fs = FileSystem.get(conf);

		//MAPPER/REDUCER PARAMETERS
		conf.set("keys", keys);
		//conf.set("hash_path", file2);
		conf.set("header", header);
		conf.set("separator", separator);
		conf.set("mapreduce.output.textoutputformat.separator", mr_separator);
		conf.set("mapreduce.input.fileinputformat.split.maxsize", "8388608");
		
					
		//SET JOB
		Job job = Job.getInstance(conf, "semi_join_phase2");			

		//CACHE
		Path pt2 = new Path(file2);
		if (fs.isDirectory(pt2))
		{	file2 = StringManipulation.adjust_input(file2);
			for (int i=0; i<num_of_reducers; i++)
			{	
				job.addCacheFile(new Path(file2 + "part-r-" + String.format("%05d", i)).toUri());
			}
		}
		else
			job.addCacheFile(new Path(file2).toUri());

		// KEY/VALUE TYPES
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);		
		job.setJarByClass(semi_join_phase2.class);          //??????????????
		job.setMapperClass(Hash_Key_Mapper.class);
		//job.setReducerClass(Keep_Key_Reducer.class);		//**
		System.out.println("job done.. waiting input");
	
		//PARTINIONER & COMPARATOR
		//job.setPartitionerClass(JoiningPartitioner.class);
		//job.setGroupingComparatorClass(JoiningGroupingComparator.class);

		// INPUT
		job.setInputFormatClass(HeaderInputFormat.class);
		HeaderInputFormat.addInputPath(job, new Path(file1));
		
		//OUTPUT
		job.setNumReduceTasks(0);
		FileOutputFormat.setOutputPath(job, new Path(output));
		System.out.println("all done.. exec");
		job.waitForCompletion(true);

	}

}
