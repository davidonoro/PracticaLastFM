package practica1.datasetb.ejercicio8;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, CustomKey, IntWritable>{
	
	

	@Override
	protected void map(
			LongWritable key,
			Text value,
			org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, CustomKey, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		
		String[] campos = value.toString().split("\t");
		
		String usuario = campos[0];
		String grupo = campos[4];
		String cancion = campos[5];
		String mes = campos[9];
		String year = campos[10];
		
		context.write(new CustomKey(grupo, cancion,	year, mes,usuario),new IntWritable(1));
	}

	

}
