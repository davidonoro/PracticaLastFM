package practica1.datasetb.ejercicio6;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class Combiner extends org.apache.hadoop.mapreduce.Reducer<CustomKey,IntWritable,CustomKey,IntWritable>{

	@Override
	protected void reduce(CustomKey arg0, Iterable<IntWritable> arg1,
			Reducer<CustomKey, IntWritable, CustomKey, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		int total = 0;
		for (IntWritable intWritable : arg1) {
			total = total + intWritable.get();
		}
		
		context.write(arg0, new IntWritable(total));
	}

}
