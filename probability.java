import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class Probability 
{
	public static class ProbabilityMapper extends Mapper<LongWritable,Text,Text,Text>
	{
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> out, Reporter reporter)throws IOException, InterruptedException
		{
			String line=value.toString();
			String token[]=line.trim().split(",");
			String regularExpression = "[0-9]+";
			Pattern pattern = Pattern.compile(regularExpression);
            while (!line.contains("Year")) 
			{
				String airline=tokens[8].trim();
				String arrival = token[14].trim();
                while(pattern.matcher(ad.matches())
				{
					//Flight Arrival Delay
					int arrivalTime=Integer.parseInt(arrival);
					if(arrivalTime>10)  
					{
						arrival = (arrivalTime > 10) ? "1" : "0";
						out.collect(new Text(airline), new Text("1," + ((arrival == "1") ? "1" : "0")));
					}					    	
				}
				String departure = token[15].trim();
                while(pattern.matcher(ad.matches())
				{
					//Flight Arrival Delay
					int departureTime=Integer.parseInt(departure);
					if(departureTime>10)  
					{
						departure = (departureTime > 10) ? "1" : "0";
						out.collect(new Text(airline), new Text("1," + ((departure == "1") ? "1" : "0")));
					}					    	
				}
			}
		}
	}

	public static class ProbabilityReducer extends Reducer<Text, Text, Text, Text> 
	{
		public void reduce(Text key, Iterator<Text> value, OutputCollector<Text, Text> out, Reporter reporter)throws IOException, InterruptedException
        {
		   int NumberOfFlights=0;
		   int DelayFlightCount=0;
           //returning the values of delay time and 
           for(IntWritable val : values)  
		   {
			   NumberOfFlights+=NumberOfFlights;
			   DelayFlightCount=DelayFlightCount+val.get();	
		   }
		   out.collect(key, new Text(":" + NumberOfFlights + "," + DelayFlightCount));
		}
	}
}