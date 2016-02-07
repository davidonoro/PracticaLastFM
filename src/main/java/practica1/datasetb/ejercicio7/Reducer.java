package practica1.datasetb.ejercicio7;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

public class Reducer extends org.apache.hadoop.mapreduce.Reducer<CustomKey, IntWritable, Text, NullWritable>{

	@Override
	protected void reduce(
			CustomKey arg0,
			Iterable<IntWritable> arg1,
			org.apache.hadoop.mapreduce.Reducer<CustomKey, IntWritable, Text, NullWritable>.Context arg2)
			throws IOException, InterruptedException {
		
		// sum all appearances for each song on each age range (ie: wordcount)
		int total = 0;
		for (IntWritable intWritable : arg1) {
			total += intWritable.get();
		}
		
		arg2.write(new Text(arg0.getCancion()+"\t"+arg0.getRango()+"\t"+total), NullWritable.get());
	}

}
