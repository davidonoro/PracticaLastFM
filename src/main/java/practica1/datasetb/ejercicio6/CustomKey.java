package practica1.datasetb.ejercicio6;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CustomKey implements WritableComparable<CustomKey>{
	
	String grupo;
	String cancion;
	String semana;
	String year;
	String sexo;
	
	
	public CustomKey(){};


	public CustomKey(String grupo, String cancion,String year, String semana, String sexo) {
		super();
		this.grupo = grupo;
		this.cancion = cancion;
		this.semana = semana;
		this.year = year;
		this.sexo = sexo;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(grupo);
		out.writeUTF(cancion);
		out.writeUTF(semana);
		out.writeUTF(year);
		out.writeUTF(sexo);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.grupo = in.readUTF();
		this.cancion = in.readUTF();
		this.semana = in.readUTF();
		this.year = in.readUTF();
		this.sexo = in.readUTF();
	}

	@Override
	public int compareTo(CustomKey o) {
		int y  = grupo.compareTo(o.grupo);
		if(y == 0){
			int z = cancion.compareTo(o.cancion);
			if (z == 0){
				int a = semana.compareTo(o.semana);
				if(a == 0){
					int b  = year.compareTo(o.year);
					if (b == 0){
						return sexo.compareTo(o.sexo);
					}else{
						return b;
					}
					
				}else{
					return a;
				}
			}else{
				return z;
			}
		}else{
			return y;
		}
		
	}


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((cancion == null) ? 0 : cancion.hashCode());
		result = prime * result + ((grupo == null) ? 0 : grupo.hashCode());
		result = prime * result + ((semana == null) ? 0 : semana.hashCode());
		result = prime * result + ((sexo == null) ? 0 : sexo.hashCode());
		result = prime * result + ((year == null) ? 0 : year.hashCode());
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
		if (grupo == null) {
			if (other.grupo != null)
				return false;
		} else if (!grupo.equals(other.grupo))
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
		if (year == null) {
			if (other.year != null)
				return false;
		} else if (!year.equals(other.year))
			return false;
		return true;
	}


	public String getGrupo() {
		return grupo;
	}


	public void setGrupo(String grupo) {
		this.grupo = grupo;
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


	public String getYear() {
		return year;
	}


	public void setYear(String year) {
		this.year = year;
	}


	public String getSexo() {
		return sexo;
	}


	public void setSexo(String sexo) {
		this.sexo = sexo;
	}

	
}
