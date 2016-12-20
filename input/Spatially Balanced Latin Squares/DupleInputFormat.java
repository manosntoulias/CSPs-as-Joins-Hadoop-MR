/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** An {@link InputFormat} for plain text files.  Files are broken into lines.
 * Either linefeed or carriage-return are used to signal end of line.  Keys are
 * the position in the file, and values are the line of text.. */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class DupleInputFormat extends FileInputFormat<LongWritable, Text> {

  @Override
  public RecordReader<LongWritable, Text> 
    createRecordReader(InputSplit split,
                       TaskAttemptContext context) {
    return new LineRecordReader();
  }

  @Override
  protected boolean isSplitable(JobContext context, Path file) {
    return false;
  }


  /**
   * A factory that makes the split for this class. It can be overridden
   * by sub-classes to make sub-types
   */
  protected DupleSplit makeSplit(Path file, long start, long length, 
                                String[] hosts, int order) {
    return new DupleSplit(file, start, length, hosts, order);
  }

  /** 
   * Generate the list of files and make them into FileSplits.
   * @param job the job context
   * @throws IOException
   */
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(job));
    long maxSize = getMaxSplitSize(job);

    // generate splits
    List<InputSplit> splits = new ArrayList<InputSplit>();
    List<FileStatus> files = listStatus(job);
    // times that each file exists in the files List
    ArrayList<Integer> times = new ArrayList<Integer>();
    ArrayList<Path> paths = new ArrayList<Path>();

    for (FileStatus file: files) {
      Path path = file.getPath();
      long length = file.getLen();
      if (length != 0) {
        FileSystem fs = path.getFileSystem(job.getConfiguration());
        BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);

	int index;
	if ((index = paths.indexOf(path)) != -1)
		times.set(index, times.get(index) + 1);
	else
	{	times.add(0);
		paths.add(path);
		index = times.size() -1;
	}

        // not splitable
        splits.add(makeSplit(path, 0, length, blkLocations[0].getHosts(), times.get(index)));

      } else { 
        //Create empty hosts array for zero length files
        splits.add(makeSplit(path, 0, length, new String[0]));
      }
    }
    // Save the number of input files for metrics/loadgen
    job.getConfiguration().setLong(NUM_INPUT_FILES, files.size());
    //LOG.debug("Total # of splits: " + splits.size());
    return splits;
  }

}
