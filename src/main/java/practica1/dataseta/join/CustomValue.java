package practica1.dataseta.join;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class CustomValue implements Writable{
	
	String gender = "";
	String age = "";
	String country = "";
	
	String artname = "";
	String plays = "";
	
	boolean isUser = true;
	
	public CustomValue(){};

	public CustomValue(String gender, String age, String country) {
		super();
		this.gender = gender;
		this.age = age;
		this.country = country;
		this.isUser = true; 
	}
	
	

	public CustomValue(String artname, String plays) {
		super();
		this.artname = artname;
		this.plays = plays;
		this.isUser = false; 
	}



	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(gender);
		out.writeUTF(age);
		out.writeUTF(country);
		out.writeUTF(artname);
		out.writeUTF(plays);
		out.writeBoolean(isUser);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.gender = in.readUTF();
		this.age = in.readUTF();
		this.country = in.readUTF();
		this.artname = in.readUTF();
		this.plays = in.readUTF();
		this.isUser = in.readBoolean();
	}

	public String getGender() {
		return gender;
	}

	public void setGender(String gender) {
		this.gender = gender;
	}

	public String getAge() {
		return age;
	}

	public void setAge(String age) {
		this.age = age;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public String getArtname() {
		return artname;
	}

	public void setArtname(String artname) {
		this.artname = artname;
	}

	public String getPlays() {
		return plays;
	}

	public void setPlays(String plays) {
		this.plays = plays;
	}

	public boolean isUser() {
		return isUser;
	}

	public void setUser(boolean isUser) {
		this.isUser = isUser;
	}

	
	
}
