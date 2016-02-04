package practica1.datasetb.ejercicio6;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CustomKey implements WritableComparable<CustomKey>{
	
	String cancion;
	String semana;
	String sexo;
	
	
	public CustomKey(){};


	public CustomKey(String cancion, String semana, String sexo) {
		super();
		this.cancion = cancion;
		this.semana = semana;
		this.sexo = sexo;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(cancion);
		out.writeUTF(semana);
		out.writeUTF(sexo);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.cancion = in.readUTF();
		this.semana = in.readUTF();
		this.sexo = in.readUTF();
	}

	@Override
	public int compareTo(CustomKey o) {
		int z = cancion.compareTo(o.cancion);
		if (z == 0){
			int a = semana.compareTo(o.semana);
			if(a == 0){
				return sexo.compareTo(o.sexo);
			}else{
				return a;
			}
		}else{
			return z;
		}
	}
	

	public String getCancion() {
		return cancion;
	}


	public void setCancion(String cancion) {
		this.cancion = cancion;
	}


	public String getSemana() {
		return semana;
	}


	public void setSemana(String semana) {
		this.semana = semana;
	}


	public String getSexo() {
		return sexo;
	}


	public void setSexo(String sexo) {
		this.sexo = sexo;
	}


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((cancion == null) ? 0 : cancion.hashCode());
		result = prime * result + ((semana == null) ? 0 : semana.hashCode());
		result = prime * result + ((sexo == null) ? 0 : sexo.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CustomKey other = (CustomKey) obj;
		if (cancion == null) {
			if (other.cancion != null)
				return false;
		} else if (!cancion.equals(other.cancion))
			return false;
		if (semana == null) {
			if (other.semana != null)
				return false;
		} else if (!semana.equals(other.semana))
			return false;
		if (sexo == null) {
			if (other.sexo != null)
				return false;
		} else if (!sexo.equals(other.sexo))
			return false;
		return true;
	}
	
}
