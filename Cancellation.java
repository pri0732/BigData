import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Pattern;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class cancellation
{
	public static class CancellationMapper extends Mapper<LongWritable, Text, Text, Text> 
    {
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)throws IOException, InterruptedException
        {
			String line = value.toString();
            int rowValue;
			String token[]t = line.trim().split(",");
            String regExpression = "[0-9]+";
		    Pattern pattern = Pattern.compile(regExpression);
            for(rowValue=21; rowValue<=22; rowValue++)
			{
                String can = token[22].trim();
                if(Integer.parseInt(can)=0 || can.contains("NA") || can.isEmpty())
                    output.collect(new Text(token[rowValue+1].trim), new Text("1"));
            }				
		}
	} 
	

	public static class CancelReducer extends Reducer<Text, Text, Text, Text> 
    {
		public void reduce(Text key, Iterator<Text> value, OutputCollector<Text, Text> output, Reporter reporter)throws IOException, InterruptedException
		{
			int sum = 0;
            for(Iterator<Text> val: value)
				sum += value.get();
			output.collect(key.toString(), new Text(String.valueOf(sum)));
		}
	}
}