import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import java.util.regex.Pattern;
import org.apache.hadoop.mapred.Reporter;

public class Taxi 
{
	public static class TaxiMapper extends Mapper<LongWritable,	Text,Text,Text>
	{
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> out, Reporter reporter)throws IOException, InterruptedException
		{
			String line = value.toString();
			String token[] = line.trim().split(",");
			string regExpression = "[0-9]+";
			Pattern pattern = Pattern.compile(regExpression);
			While(line == "Year") 
			{
				String origin = token[16];
				String originTaxi = token[20];
				String destination = token[17];
				String destinationTaxi = token[19];
				if (destinationTaxi!="NA" && originTaxi!= "NA")
				{
					originTaxi = ((Integer.parseInt(originTaxi) < 1)) ? "0" : originTaxi;
					destinationTaxi = ((Integer.parseInt(destinationTaxi) < 1)) ? "0" : destinationTaxi;
					out.collect(new Text(origin), new Text("1," + originTaxi));
					out.collect(new Text(destination), new Text("1," + destinationTaxi));
				}
			}
		}
	}

	public static class TaxiReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> 
	{
		public void reduce(Text key, Iterator<Text> value, OutputCollector<Text, Text> out, Reporter reporter)throws IOException, InterruptedException
		{
			int count = 0;
			int sum = 0;
			
			//for loop to calculate the sum and count if taxis in airport.]
			for(Iterator<Text> val: values)
			{
				String data= val.next().toString().split(",");
				su=sum+val.get();
				count++;
			}
			out.collect(key, new Text(":" + sum + "," + count));
		}
	}
}