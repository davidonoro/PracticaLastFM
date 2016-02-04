package practica1.dataseta.ejercicio3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CustomKey implements WritableComparable<CustomKey>{
	
	String grupo;
	String rango;
	String sexo;
	
	
	public CustomKey(){};


	public CustomKey(String grupo, String rango, String sexo) {
		super();
		this.grupo = grupo;
		this.rango = rango;
		this.sexo = sexo;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(grupo);
		out.writeUTF(rango);
		out.writeUTF(sexo);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.grupo = in.readUTF();
		this.rango = in.readUTF();
		this.sexo = in.readUTF();
	}

	@Override
	public int compareTo(CustomKey o) {
		int z = grupo.compareTo(o.grupo);
		if (z == 0){
			int a = rango.compareTo(o.rango);
			if(a == 0){
				return sexo.compareTo(o.sexo);
			}else{
				return a;
			}
		}else{
			return z;
		}
	}

	

	public String getGrupo() {
		return grupo;
	}

	public void setGrupo(String grupo) {
		this.grupo = grupo;
	}

	public String getRango() {
		return rango;
	}

	public void setRango(String rango) {
		this.rango = rango;
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
		result = prime * result + ((grupo == null) ? 0 : grupo.hashCode());
		result = prime * result + ((rango == null) ? 0 : rango.hashCode());
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
		if (grupo == null) {
			if (other.grupo != null)
				return false;
		} else if (!grupo.equals(other.grupo))
			return false;
		if (rango == null) {
			if (other.rango != null)
				return false;
		} else if (!rango.equals(other.rango))
			return false;
		if (sexo == null) {
			if (other.sexo != null)
				return false;
		} else if (!sexo.equals(other.sexo))
			return false;
		return true;
	}
	
}
