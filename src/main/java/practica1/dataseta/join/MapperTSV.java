package practica1.dataseta.join;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperTSV extends Mapper<LongWritable, Text, CustomKey, CustomValue>{
	
	CustomKey clave = new CustomKey();
	CustomValue valor = new CustomValue();

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, CustomKey, CustomValue>.Context context)
			throws IOException, InterruptedException {
		

		String[] campos = value.toString().split("\t");
		if(campos.length == 4){
			clave.setUser(campos[0]);
			clave.setUsuario(false);
			
			valor.setArtname(campos[2]);
			valor.setPlays(campos[3]);
			valor.setUser(false);
			
			context.write(clave, valor);
		}
	}
	
	

}
