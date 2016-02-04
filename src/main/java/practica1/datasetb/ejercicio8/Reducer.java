package practica1.datasetb.ejercicio8;

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
		

		int total = 0;
		for (IntWritable intWritable : arg1) {
			total = intWritable.get()+total;
		}
		
		arg2.write(new Text(arg0.getUsuario()+"\t"+arg0.getMes()+"\t"+arg0.getCancion()+"\t"+total), NullWritable.get());
	}

}
