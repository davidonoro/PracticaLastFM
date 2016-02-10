package practica1.datasetb.join;

import java.io.IOException;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperTSV extends Mapper<LongWritable, Text, CustomKey, CustomValue>{
	
	CustomKey clave = new CustomKey();
	CustomValue valor = new CustomValue();
	
	HashMap<Integer, String> days = new HashMap<Integer, String>();
	
	

	@Override
	protected void setup(
			Mapper<LongWritable, Text, CustomKey, CustomValue>.Context context)
			throws IOException, InterruptedException {
	
		days.put(1, "L");
		days.put(2, "M");
		days.put(3, "X");
		days.put(4, "J");
		days.put(4, "V");
		days.put(6, "S");
		days.put(7, "D");
	}



	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, CustomKey, CustomValue>.Context context)
			throws IOException, InterruptedException {
			
		String[] campos = value.toString().split("\t");
		if(campos.length == 6){
			clave.setUser(campos[0]);
			clave.setUsuario(false);
			
			//recuperamos la fecha y la dividimos en hora (0-23), diasemana (L-D), semanaAnyo (0-48), mes (1-12)
			//formato origen 2008-11-01T22:39:18Z
			Pattern p = Pattern.compile("(.{4})-(.{2})-(.{2})T(.{2}):(.{2}):(.{2})Z");
	        Matcher matcher = p.matcher(campos[1]);
	        
	        if (matcher.find()) {
	            String anyo = matcher.group(1);
	            String mes = matcher.group(2);
	            String dia = matcher.group(3);
	            String hora = matcher.group(4);
	           
	            String diaSemana = "";
	            String semanaAnyo = "";
	                        
	           
	            GregorianCalendar gc = new GregorianCalendar();
	            gc.setFirstDayOfWeek(Calendar.MONDAY);
	            gc.setMinimalDaysInFirstWeek(4);
	            gc.set(Integer.parseInt(anyo), Integer.parseInt(mes)-1, Integer.parseInt(dia)-1);
	            semanaAnyo = Integer.toString(gc.get(Calendar.WEEK_OF_YEAR));
	            String dia_aux = days.get(gc.get(Calendar.DAY_OF_WEEK));
	            if(dia_aux != null){
	            	diaSemana = dia_aux;
	            }

				valor.setHora(hora);
				valor.setDiaSemana(diaSemana);
				valor.setSemanaAnyo(semanaAnyo);
				valor.setMes(mes);
				valor.setAnyo(anyo);
				valor.setArtname(campos[3]);
				valor.setTraname(campos[5]);
				
				valor.setUser(false);
				
				context.write(clave, valor);
	        } 	
	        
		}
	}
}
