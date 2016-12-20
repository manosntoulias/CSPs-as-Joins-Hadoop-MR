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

import java.io.IOException;

public class hash_semi_join
{
	static String separator;

	public static class Produce_Key_Mapper extends Mapper<Object, Text, Text, NullWritable>
	{
		int[] key_columns;
		int key_length = 0;

		Text out_key = new Text();
		NullWritable out_value = NullWritable.get();

		String separator;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			Configuration conf = context.getConfiguration();
			separator = conf.get("separator");
			int i;

			String[] key_col = conf.get("1-keys").split(separator);
			key_columns = new int[key_col.length];
			for (i=0; i<key_col.length; i++)
			{	if ((key_columns[i] = Integer.parseInt(key_col[i])) >= 0)
					key_length++;
			}
		}

		@Override
	    	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String construct_key = "";
			String[] key_values = value.toString().split(separator);
			int j, i;
			
			for (i=0, j=0; i<key_length-1; j++)
				if (key_columns[j] >= 0)
				{	construct_key += key_values[j] + separator;
					i++;
				}
				
			for (; key_columns[j] < 0; j++);
			construct_key += key_values[j];

			out_key.set(construct_key);
			context.write(out_key, out_value);
		}
	}


	public static class Produce_Key_Reducer extends Reducer<Text, NullWritable, Text, NullWritable>
	{
		NullWritable out_value = NullWritable.get();

		/*@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			


		}*/

		@Override
		protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException
		{
			context.write(key, out_value);
		}
	
	}


	public static void hash_semi_join(String file1, String output, String keys, int num_of_reducers, String sep, Configuration config) throws IOException, InterruptedException, ClassNotFoundException
	{	separator = sep;
		
		Configuration conf = new Configuration();
		conf.set("dfs.replication", "1");
		FileSystem fs = FileSystem.get(conf);

		//MAPPER/REDUCER PARAMETERS
		conf.set("1-keys", keys);
		conf.set("separator", separator);
		conf.set("mapreduce.input.fileinputformat.split.maxsize", "8388608");
					

		//SET JOB
		Job job = Job.getInstance(conf, "hash_semi_join_phase_1");

		// KEY/VALUE TYPES
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);		
		job.setJarByClass(hash_semi_join.class);          //??????????????
		job.setMapperClass(Produce_Key_Mapper.class);
		job.setReducerClass(Produce_Key_Reducer.class);		//**
		System.out.println("job done.. waiting input");
	
		//PARTINIONER & COMPARATOR
		//job.setPartitionerClass(JoiningPartitioner.class);
		//job.setGroupingComparatorClass(JoiningGroupingComparator.class);

		// INPUT
		job.setInputFormatClass(HeaderInputFormat.class);
		HeaderInputFormat.addInputPath(job, new Path(file1));
		
		//OUTPUT
		job.setNumReduceTasks(num_of_reducers);
		FileOutputFormat.setOutputPath(job, new Path(output));
		System.out.println("all done.. exec");
		job.waitForCompletion(true);

	}


}
