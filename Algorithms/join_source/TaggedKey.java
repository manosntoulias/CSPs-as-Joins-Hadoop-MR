import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;


public class TaggedKey implements Writable, WritableComparable<TaggedKey> {

		private String joinKey = new String();
		private int tag;

		public TaggedKey() {}

		@Override
		public int compareTo(TaggedKey TaggedKey) {

			int compareValue = this.joinKey.compareTo(TaggedKey.getJoinKey());
			if(compareValue == 0 ){
			compareValue = Integer.compare(this.tag, TaggedKey.getTag());
			}
		return compareValue;
		}


		public void set(String joinKey, int joinOrder)
		{
			this.joinKey = joinKey;
			this.tag = joinOrder;
			return;
		}

		public String getJoinKey()
		{	return this.joinKey;
		}

		public int getTag()
		{	return this.tag;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			//joinKey.readFields(in);
			joinKey = WritableUtils.readString(in);
			//tag.readFields(in);
			tag = in.readInt();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			//joinKey.write(out);
			WritableUtils.writeString(out, joinKey);
		  	//tag.write(out);	
			out.writeInt(tag);		
		}
	   
	 }
