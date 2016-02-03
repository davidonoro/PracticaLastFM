package practica1.dataseta.join;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperTable extends Mapper<LongWritable, Text, CustomKey, CustomValue>{
	
	CustomKey clave = new CustomKey();
	CustomValue valor = new CustomValue();

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, CustomKey, CustomValue>.Context context)
			throws IOException, InterruptedException {
		

		String[] campos = value.toString().split(",");
		if(campos.length == 6){
			clave.setUser(campos[1]);
			clave.setUsuario(true);
			
			valor.setGender(campos[2]);
			valor.setAge(campos[3]);
			valor.setCountry(campos[4]);
			valor.setUser(true);
			
			context.write(clave, valor);
		}
	}

}
