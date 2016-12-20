import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

public class DupleSplit extends FileSplit
{
	private int order;

	public DupleSplit(Path file, long start, long length, String[] hosts, int order) {

		super(file, start, length, hosts);
		this.order = order;
	}

	public DupleSplit() {
		super();
	}


	public int get_order()
	{	return order;
	}

	////////////////////////////////////////////
	// Writable methods
	////////////////////////////////////////////

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeInt(order);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		order = in.readInt();
	}
}
