import java.io.*;
import java.util.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.apache.hadoop.cli.util.SubstringComparator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.conf.Configuration;
import airline.Probability;
import airline.Cancellation;
import airline.Taxi;
public class Controller 
{
	public static class HeapValue 
    {
		public String key;
		public Double value;
		public HeapValue(String ky, double val) 
        {
            this.key = ky;
			this.value = val;
        }
        //Comparison codes

        //Maximum value comparison
        public static Comparator<HeapValue> MaxValue = new Comparator<HeapValue>() 
		{
			public int compare(HeapValue value1, HeapValue value2) 
			{
                if(value2.value > value1.value)
				    return value2.value;
                else
                    return value1.value;
			}
		}
        //Minimum value comparison
        public static Comparator<HeapValue> MinValue = new Comparator<HeapValue>() 
		{
			public int compare(HeapValue value1, HeapValue value2) 
			{
                if(value2.value < value1.value)
				    return value2.value;
                else
                    return value1.value;
			}
		}
    }

    public static ArrayList<HeapValue> Minimum = new ArrayList<HeapValue>();
	public static ArrayList<HeapValue> Maximum = new ArrayList<HeapValue>();


    //Main controller of the process
	public static void main(String[] args) throws IOException, InterruptedException
    {
        //Part 1 configuration for flight Probability
        Configuration conf=new Configuration();
        Job Probability=new Job(conf,"Probability");
        schedulePro.setJarByClass(Probability.class);
        FileInputFormat.addInputPath(Probability, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1] + "Flightprob")); 
		Probability.setJobName("Flightprob");
		Probability.setOutputKeyClass(output.class);
		Probability.setOutputValueClass(output.class);
		Probability.setMapperClass(probability.class);
		Probability.setCombinerClass(probability.class);
		Probability.setReducerClass(probability.class);
		Probability.setInputFormat(TextInputFormat.class);
		Probability.setOutputFormat(TextOutputFormat.class);
        Probability.waitForCompletion(true);
		JobClient.runJob(Probability);
		Minimum.clear();
        Maximum.clear();
		ProbabilityStatus(args);


        //Part 2 configuration for average taxi time for flights
        Configuration conf1=new Configuration();
        Job Taxi=new Job(conf1,"Average Taxi Time for flights");
        schedulePro.setJarByClass(Taxi.class);
		FileInputFormat.addInputPaths(Taxi, new Path(args[0]));
		FileOutputFormat.setOutputPath(Taxi, new Path(args[1] + "Taxi"));
		Taxi.setJobName("Taxi");
		Taxi.setOutputKeyClass(output.class);
		Taxi.setOutputValueClass(output.class);
		Taxi.setMapperClass(TaxiTime.class);
		Taxi.setCombinerClass(TaxiTime.class);
		Taxi.setReducerClass(TaxiTime.class);
		Taxi.setInputFormat(TextInputFormat.class);
		Taxi.setOutputFormat(TextOutputFormat.class);
        Taxi.waitForCompletion(true);
        JobClient.runJob(Taxi);
		Minimum.clear();
        Maximum.clear();
		TaxiStatus(args);

        //Part 3 configuration for flight cancellation
        configuration conf2=new Configuration();
        Job Cancellation=new Job(conf2,"Flight Cancellations");
        schedulePro.setJarByClass(Cancellation.class);
		FileInputFormat.setInputPaths(Cancellation, new Path(args[0]));
		FileOutputFormat.setOutputPath(Cancellation, new Path(args[1] + "Cancellation"));
		Cancellation.setJobName("Cancellation");
		Cancellation.setOutputKeyClass(output.class);
		Cancellation.setOutputValueClass(output.class);
		Cancellation.setMapperClass(Cancellation.class);
		Cancellation.setCombinerClass(Cancellation.class);
		Cancellation.setReducerClass(Cancellation.class);
		Cancellation.setInputFormat(TextInputFormat.class);
		Cancellation.setOutputFormat(TextOutputFormat.class);
		JobClient.runJob(Cancellation);
        Minimum.clear();
        Maximum.clear();
		CancellationStatus(args);

	}

        //Compare and print the final results in an output file to show highest and lowest probability
    	public static void ProbabilityStatus(String[] args) throws IOException, InterruptedException
        {
		    File file = new File(args[1] + "prob//part-00000");
		    BufferedReader fileInstance = new BufferedReader(new FileReader(file));
		    String s = fileInstance.readLine();
		    if (s != null) 
            {
			String subString = s.substring(1, s.indexOf(":"));
			String[] Values = s.substring(s.indexOf(":"), s.length()).split(",");
			double Total = Integer.parseInt(Values[0].trim());
			double Delay = Integer.parseInt(Values[1]);
			double probability = 1.0 - (Delay / Total);
            if (Maximum.size() == 3 && Maximum.get(2).value < probability) 
            {
			Maximum.remove(2);
			Maximum.add(new HeapValue(subString, probability);
			Collections.sort(Maximum, HeapValue.MaxValue);
		    } 
            else 
            if (Maximum.size() < 3) 
            {
			Maximum.add(new HeapValue(subString, probability));
			Collections.sort(Maximum, HeapValue.MaxValue);
            }
		    
            if (Minimum.size() == 3 && Minimum.get(2).value > probability) 
            {
			Minimum.remove(2);
			Minimum.add(new HeapValue(subString, probability));
			Collections.sort(Minimum, HeapValue.MinValue);
		    } 
            else 
            if (Minimum.size() < 3) 
            {
			Minimum.add(new HeapValue(subString, probability));
			Collections.sort(Minimum, HeapValue.MinValue);
            }
            }
            
		    PrintWriter outputFile = new PrintWriter(new FileWriter(new File(args[1] + "probability/output.txt")));
		    outputFile.println("/nHighest probability of airlines on schedukle are:/n");
		    if(Maximum.size() < 1) 
			    outputFile.println("/n------------------------------------------ Data Unavailable ------------------------------------------/n");
		    for (HeapValue info : Maximum) 
			    outputFile.println(info.key + "\t" + info.value);
		    outputFile.println("/nLowest probability of airlines on schedule are:/n");
		    if (Heap.Min.size() < 1) 
			    outputFile.println("/n------------------------------------------ Data Unavailable ------------------------------------------/n");
		    for (heapValue info1 : Heap.Min) 
			    outputFile.println(info1.key + "\t" + info1.value);
            fileInstance.close();
		    outputFile.close();
        }


    //prints the airports for longest and shortest taxi time
	private static void TaxiStatus(String[] args) throws IOException, InterruptedException
    {
		File file = new File(args[0] + "Taxi//part-00000");
		BufferedReader fileInstance = new BufferedReader(new FileReader(file));
		String s = fileInstance.readLine;
		if (s != null) 
        {
			String subString = s.substring(0, s.indexOf(":"));
			String[] data = s.substring(s.indexOf(":") + 1, s.length()).split(",");
			double Total = Integer.parseInt(data[0].trim());
			double Delay = Integer.parseInt(data[1]);
			double prob = ((Delay / Total));

            //comparison for longest Taxi time
			if (Maximum.size() == 3 && Maximum.get(2).value < probability) 
            {
			Maximum.remove(2);
			Maximum.add(new HeapValue(subString, probability);
			Collections.sort(Maximum, HeapValue.MaxValue);
		    } 
            else 
            if (Maximum.size() < 3) 
            {
			Maximum.add(new HeapValue(subString, probability));
			Collections.sort(Maximum, HeapValue.MaxValue);
            }

		    //comparison for shortest Taxi time
            if (Minimum.size() == 3 && Minimum.get(2).value > probability) 
            {
			Minimum.remove(2);
			Minimum.add(new HeapValue(subString, probability));
			Collections.sort(Minimum, HeapValue.MinValue);
		    } 
            else 
            if (Minimum.size() < 3) 
            {
			Minimum.add(new HeapValue(subString, probability));
			Collections.sort(Minimum, HeapValue.MinValue);
            }
            }
		//output for airports with longest Taxi time
		PrintWriter outputFile = new PrintWriter(new FileWriter(new File(args[1] + "Taxi/output.txt")));
		outputFile.println("\nAirports with Longest Taxi-in Time are:\n");
		if(Maximum.size() < 1) 
			    outputFile.println("\n------------------------------------------ Data Unavailable ------------------------------------------");
		else
            for (HeapValue info : Maximum) 
			    outputFile.println(info.key + "\t" + info.value);

		//output for airports with shortest Taxi time
		PrintWriter outputFile = new PrintWriter(new FileWriter(new File(args[1] + "Taxi/output.txt")));
		outputFile.println("\n-Airports with Shortest Taxi-in Time are:");
		if(Minimum.size() < 1) 
			    outputFile.println("\n------------------------------------------ Data Unavailable ------------------------------------------");
		else
            for (HeapValue info : Minimum) 
			    outputFile.println(info.key + "\t" + info.value);
        fileInstance.close();
		outputFile.close();
	}


    //analyse and print the reasons for flight cancellation
	private static void CancellationStatus(String[] args) throws IOException, InterruptedException
    {
		File file = new File(args[1] + "Cancellation//part-00000");
		BufferedReader fileInstance = new BufferedReader(new FileReader(file));
		String s = fileInstance.readLine();
        HeadpValue entry =null;
		String[] reason = null;
        int maxValue=0;
		if (s != null) 
        {
            for(HeapValue.value data:s)
            {
                String[] reasonValue = s.split("\\s");
                if(entry==null || data.getvalue().compareTo(entry.getvalue()) > 0)
                {
                entry = data;
                reason=reasonValue[0];
                maxValue = Integer.parseInt(reasonValue[0]);
                }
            }
		}
		PrintWriter outputFile = new PrintWriter(new FileWriter(new File(args[1] + "Cancellation/output.txt")));
		outputFile.println("\nMost Number of cancellations:");
		if (reason != null)
			outputFile.println("The reason for cancellation is:\t"+reason + "\t" + maxValue);
		else
			outputFile.println("Data not Found!!!!");
		fileInstance.close();
        outputFile.close();

	}
}
