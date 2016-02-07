package practica1.datasetb.ejercicio7;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CustomKey implements WritableComparable<CustomKey>{
	
	String cancion;
	String rango;
	
	
	public CustomKey(){};


	public CustomKey(String cancion, String rango) {
		super();
		this.cancion = cancion;
		this.rango = rango;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(cancion);
		out.writeUTF(rango);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.cancion = in.readUTF();
		this.rango = in.readUTF();
	}

	@Override
	public int compareTo(CustomKey o) {
		int z = cancion.compareTo(o.cancion);
		if (z == 0){
			int a = rango.compareTo(o.rango);
			if(a == 0){
				return cancion.compareTo(cancion);
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

	public void setCancion(String grupo) {
		this.cancion = grupo;
	}

	public String getRango() {
		return rango;
	}

	public void setRango(String rango) {
		this.rango = rango;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((cancion == null) ? 0 : cancion.hashCode());
		result = prime * result + ((rango == null) ? 0 : rango.hashCode());
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
		if (rango == null) {
			if (other.rango != null)
				return false;
		} else if (!rango.equals(other.rango))
			return false;
		return true;
	}
	
}
