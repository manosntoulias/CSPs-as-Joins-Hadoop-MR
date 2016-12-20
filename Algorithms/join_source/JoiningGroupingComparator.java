import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;

	public class JoiningGroupingComparator extends WritableComparator {


	    public JoiningGroupingComparator() {
		super(TaggedKey.class,true);
	    }

	    @Override
	    public int compare(WritableComparable a, WritableComparable b) {
		TaggedKey TaggedKey1 = (TaggedKey)a;
		TaggedKey TaggedKey2 = (TaggedKey)b;
		return TaggedKey1.getJoinKey().compareTo(TaggedKey2.getJoinKey());
	    }
	}
