package practica1.datasetb.ejercicio8;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CustomKey implements WritableComparable<CustomKey>{

	String usuario;
	String year;
	String mes;
	String grupo;
	String cancion;


	public CustomKey(){};


	public CustomKey(String grupo, String cancion, String year,String mes, String usuario) {
		super();
		this.grupo = grupo;
		this.cancion = cancion;
		this.mes = mes;
		this.year = year;
		this.usuario = usuario;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(grupo);
		out.writeUTF(cancion);
		out.writeUTF(year);
		out.writeUTF(mes);
		out.writeUTF(usuario);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.grupo = in.readUTF();
		this.cancion = in.readUTF();
		this.year = in.readUTF();
		this.mes = in.readUTF();
		this.usuario = in.readUTF();
	}



	@Override
	public int compareTo(CustomKey o) {
		int y  = grupo.compareTo(o.grupo);
		if (y==0){
			int z = cancion.compareTo(o.cancion);
			if (z == 0){
				int a = mes.compareTo(o.mes);
				if(a == 0){
					int b = year.compareTo(o.year);
					if (b == 0){
						return usuario.compareTo(o.usuario);
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


	public String getUsuario() {
		return usuario;
	}


	public void setUsuario(String usuario) {
		this.usuario = usuario;
	}


	public String getYear() {
		return year;
	}


	public void setYear(String year) {
		this.year = year;
	}


	public String getMes() {
		return mes;
	}


	public void setMes(String mes) {
		this.mes = mes;
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


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((cancion == null) ? 0 : cancion.hashCode());
		result = prime * result + ((grupo == null) ? 0 : grupo.hashCode());
		result = prime * result + ((mes == null) ? 0 : mes.hashCode());
		result = prime * result + ((usuario == null) ? 0 : usuario.hashCode());
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
		if (mes == null) {
			if (other.mes != null)
				return false;
		} else if (!mes.equals(other.mes))
			return false;
		if (usuario == null) {
			if (other.usuario != null)
				return false;
		} else if (!usuario.equals(other.usuario))
			return false;
		if (year == null) {
			if (other.year != null)
				return false;
		} else if (!year.equals(other.year))
			return false;
		return true;
	}

	
}
