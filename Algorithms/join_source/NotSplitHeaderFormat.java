import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.fs.Path;

public class NotSplitHeaderFormat extends HeaderInputFormat {

  @Override
  protected boolean isSplitable(JobContext context, Path file) {
    return false;
  }

}
