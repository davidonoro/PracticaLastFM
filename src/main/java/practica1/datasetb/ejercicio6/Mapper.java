package practica1.datasetb.ejercicio6;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
		
		String cancion = campos[5];
		String semana = campos[8];
		
		context.write(new CustomKey(cancion, semana,sexo),new IntWritable(1));
	}

	

}
