package practica1.datasetb.join;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CustomKey implements WritableComparable<CustomKey>{
	
	String user;
	boolean isUsuario;
	
	

	@Override
	public void write(DataOutput out) throws IOException {		
		out.writeUTF(user);
		out.writeBoolean(isUsuario);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.user = in.readUTF();
		this.isUsuario = in.readBoolean();
	}

	@Override
	public int compareTo(CustomKey o) {
		int a = user.compareTo(o.user);
		if(a == 0){
			return -Boolean.compare(isUsuario, o.isUsuario);
		}else{
			return a;
		}
	}

	

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((user == null) ? 0 : user.hashCode());
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
		if (user == null) {
			if (other.user != null)
				return false;
		} else if (!user.equals(other.user))
			return false;
		return true;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public boolean isUsuario() {
		return isUsuario;
	}

	public void setUsuario(boolean isUsuario) {
		this.isUsuario = isUsuario;
	}

	
	
	

}
