package practica1.datasetb.join;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

public class Reducer extends org.apache.hadoop.mapreduce.Reducer<CustomKey, CustomValue, Text, NullWritable>{

	@Override
	protected void reduce(
			CustomKey arg0,
			Iterable<CustomValue> arg1,
			org.apache.hadoop.mapreduce.Reducer<CustomKey, CustomValue, Text, NullWritable>.Context arg2)
			throws IOException, InterruptedException {
		
		String gender = "";
		String age = "";
		String country = "";
	
		//System.out.println(arg0.user);
		for (CustomValue customValue : arg1) {
			
			if(customValue.isUser){
				gender = customValue.getGender();
				age = customValue.getAge();
				country = customValue.getCountry();
			}else{
				String texto = arg0.getUser()+"\t"+gender+"\t"+age+"\t"+country+"\t"+customValue.getArtname()+"\t"+
							   customValue.getTraname()+"\t"+customValue.getHora()+"\t"+customValue.getDiaSemana()+"\t"+
							   customValue.getSemanaAnyo()+"\t"+customValue.getMes()+"\t"+customValue.getAnyo();
				arg2.write(new Text(texto), NullWritable.get());
			}
		}
	}
	
	

}
