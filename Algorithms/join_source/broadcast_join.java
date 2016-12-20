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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

import calculation.InputOptimization;
import string.StringManipulation;

public class broadcast_join
{
	static String separator;

	public static class Broadcast_Mapper extends Mapper<Object, Text, Text, Text>
	{
          	private Text rec_R = new Text();
          	private Text joinedRec = new Text();
		private HashMap<String, ArrayList<String>> hm = new HashMap<String, ArrayList<String>>();

		Text out_key = new Text();
		Text out_value = new Text();

		int[] key_columns;
		int key_length = 0;

		String separator;
		boolean all_common;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			//declare variables
			String line = "dummy;";
			int j;
			//int i=0;
			all_common = false;
	   		FileSystem hdfs;
			Path path;
			Configuration config = context.getConfiguration();
			URI[] uris = context.getCacheFiles();
			String construct_key = "";
			separator = config.get("separator");	
	   		
			
			//write header
			out_key.set(config.get("header"));
			context.write(out_key, null);

			//configure key
			String[] key_col = config.get("keys").split(separator);
			key_columns = new int[key_col.length];
			for (j=0; j<key_col.length; j++)
			{	if ((key_columns[j] = Integer.parseInt(key_col[j])) >= 0)
					key_length++;
			}
 
			//retrieve cache files and load them into main memory   
			int counter = 0;
			//System.out.println("cache file number is: " + uris.length);	  
			for (;counter < uris.length; counter++) 
	     		{     			
				
				
				try {				
					//hdfs= FileSystem.get(config);

					URI uri = uris[counter];
					Path perm_path = new Path(uri.getPath());
		  			String hash_path = perm_path.getName().toString();
					//System.out.println("cache filename is: " + hash_path);
					File f = new File(hash_path);

					//Read hashtable
					InputStream fis = new FileInputStream(f);
		    			InputStreamReader isr = new InputStreamReader(fis);//, Charset.forName("UTF-8"));
		    			BufferedReader br = new BufferedReader(isr);	     			

					String header = br.readLine(); //skip header
					ArrayList<String> a_list;
	    				while((line=br.readLine())!=null)
	    				{
	    					StringTokenizer tokenizer = new StringTokenizer(line, ",");
						construct_key = tokenizer.nextToken();
	    					if((a_list = hm.get(construct_key)) == null)
	    					{	
							a_list = new ArrayList<String>();
							hm.put(construct_key, a_list);
						}
						String final_token;
						if (tokenizer.hasMoreTokens())
							final_token = tokenizer.nextToken();
						else
						{	final_token = null;
							all_common = true;
						}
						a_list.add(final_token);
						//System.out.println("Contrsuct key for bjoin: " + construct_key);
	    					
	    					
	    				}
					fis.close();
	    				
	    			} catch (Exception e) {
	    				// TODO Auto-generated catch block
	    				e.printStackTrace();
	    				}
					    			     			   
          
      			}
		}	
			
		

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{	  // check if hm exist
			  if(!hm.isEmpty())
			  {
				  // do this: probe HR with the join column extracted from V
				  // for each match r from HR do 
				  // emit (null, new record(r, V ))
				  String construct_key = "";
				  String[] key_values = value.toString().split(separator);
			  	  int j, counter;
			
				  for (counter=0, j=0; counter < key_length-1; j++)
					if (key_columns[j] >= 0)
					{	construct_key += key_values[j] + separator;
						counter++;
					}
				  for (; counter < key_length; j++)
					if (key_columns[j] >= 0)
					{	construct_key += key_values[j];
						counter++;
					}

				  //probe the CID into the Ht_R
				  if(hm.containsKey(construct_key))
				  {
					  if (all_common)
					  {	context.write(value, null);
						return;
					  }

					  //join the two matching records
					  //out_key.set(construct_key.trim());
					  ArrayList<String> list = hm.get(construct_key);
					  for (String str: list)
					  {	out_value.set(str);
					  	context.write(value, out_value);  //adds the key-value pair to the output
					  }
				  } 
			  }
			  else
			  {
				  //to be continued
			  }        	          	                              		
            	}     
		       
          
	}


	public static void broadcast_join(String file1, String file2, String output, String header, String keys, String mode, String mr_separator, String sep, Configuration config) throws IOException, InterruptedException, ClassNotFoundException
	{	separator = sep;

		// third job, broadcast join 
		Configuration conf = new Configuration();
		conf.set("dfs.replication", "1");
		FileSystem fs = FileSystem.get(conf);

		//PARAMETERS
		conf.set("keys", keys);
		conf.set("header", header);
		conf.set("separator", separator);
		conf.set("mapreduce.input.fileinputformat.split.maxsize", "8388608");

		//SET JOB
		Job job = Job.getInstance(conf, "brodcast_join");	
      
		//CACHE
		Path pt2 = new Path(file2);
		if (fs.isDirectory(pt2))
		{	file2 = StringManipulation.adjust_input(file2);
			pt2 = new Path(file2 + "part-"+mode+"-" + String.format("%05d", 0));
			for (int i=1; fs.exists(pt2); i++)
			{	
				job.addCacheFile(pt2.toUri());
				pt2 = new Path(file2 + "part-"+mode+"-" + String.format("%05d", i));
			}
		}		
		else
			job.addCacheFile(new Path(file2).toUri());

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setJarByClass(broadcast_join.class); 
		job.setMapperClass(Broadcast_Mapper.class);
		      
		      
		//set the number of reducers to be zero
		job.setNumReduceTasks(0);            


		//INPUT & OUTPUT
		job.setInputFormatClass(HeaderInputFormat.class); 
		HeaderInputFormat.addInputPath(job, new Path(file1)); 
		FileOutputFormat.setOutputPath(job, new Path(output));
      	
		job.waitForCompletion(true);

	}

}
