import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Partitioner;

	public class JoiningPartitioner extends Partitioner<TaggedKey,Text> {

	    @Override
	    public int getPartition(TaggedKey TaggedKey, Text text, int numPartitions) {
		
		return (TaggedKey.getJoinKey().hashCode() & Integer.MAX_VALUE) % numPartitions;
	    }
	}
