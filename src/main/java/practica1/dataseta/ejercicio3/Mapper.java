package practica1.dataseta.ejercicio3;

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
	
	
	
	int q1;
	int q2;
	int q3;
	
	@Override
	protected void setup(
			org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, CustomKey, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration(); 
		String pathRangos = conf.get("quartlies");
		
		if (pathRangos != ""){
			BufferedReader br;
			FileSystem fs = FileSystem.get(conf);
            FSDataInputStream is = fs.open(new Path(pathRangos));
            br = new BufferedReader(new InputStreamReader(is));
            String linea = br.readLine();
            while (linea != null) {
                String campos[] = linea.split(",");
                if(campos.length == 3){
                	q1 = (int)Float.parseFloat(campos[0]);
                	q2 = (int)Float.parseFloat(campos[1]);
                	q3 = (int)Float.parseFloat(campos[2]);
                }
                linea = br.readLine();
            }
		}
	}
	

	@Override
	protected void map(
			LongWritable key,
			Text value,
			org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, CustomKey, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		
		String[] campos = value.toString().split("\t");
		if(campos.length == 6){
			String age = getRangoEdad(campos[2]);
			String sex = campos[1];
			
			if(!sex.contains("f") && !sex.contains("m")){
				sex = "NI";
			}
			
			String grupo = campos[4];
			String plays = campos[5];
			
			try {
				int numplays = Integer.parseInt(plays);
				context.write(new CustomKey(grupo, age, sex),new IntWritable(numplays));
			} catch (NumberFormatException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}

	
	/**
	 * 
	 * @param edad
	 * @return
	 */
	public String getRangoEdad(String age){
		// Rangos
		// "1. <q1"
		// "2. q1-q2"
		// "3. q2-q3"
		// "4. >q3"
		
		try {
			int edad = Integer.parseInt(age);
			
			if(edad < q1){
				return "1. <"+q1+"  ";
			}
			
			if(edad >= q1 && edad < q2 ){
				return "2. "+q1+"-"+q2;
			}
			
			if (edad >= q2 && edad < q3 ){
				return "3. "+q2+"-"+q3;
			}
			
			return "4. >"+q3+"  ";
			
		} catch (NumberFormatException e) {
			return "5. NI";
		}
	}
	
	

}
