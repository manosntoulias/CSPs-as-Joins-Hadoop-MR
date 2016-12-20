import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.junit.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.WordCount.IntSumReducer;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;



public class HeaderInputFormat extends FileInputFormat<LongWritable, Text>{
     Text header;

     
        @Override
        public RecordReader<LongWritable, Text> createRecordReader(InputSplit arg0,TaskAttemptContext arg1) throws IOException, InterruptedException{
            return new LineRecordReader(){
                @Override
                public void initialize(InputSplit genericSplit,TaskAttemptContext context) throws IOException{
                    super.initialize(genericSplit, context);
                    /**
                     * Read the first line.
                     */
		    FileSplit split = (FileSplit) genericSplit;
		    if (split.getStart() == 0)
		    {		System.out.println("First split! - Skip the header");
                    		super.nextKeyValue();
		    }

                }
            };
        }
 }
