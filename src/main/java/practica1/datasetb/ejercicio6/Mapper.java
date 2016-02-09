package practica1.datasetb.ejercicio6;

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
		
		String sexo = campos[1];
		if(!sexo.contains("f") && !sexo.contains("m")){
			sexo = "NI";
		}
		
		String grupo = campos[4];
		String cancion = campos[5];
		String semana = campos[8];
		String year = campos[10];
		
		context.write(new CustomKey(grupo,cancion, year,semana,sexo),new IntWritable(1));
	}

	

}
