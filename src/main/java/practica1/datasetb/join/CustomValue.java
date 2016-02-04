package practica1.datasetb.join;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Timestamp;

import org.apache.hadoop.io.Writable;

public class CustomValue implements Writable{
	
	String gender = "";
	String age = "";
	String country = "";
	
	String hora = "";
	String diaSemana = "";
	String semanaAnyo = "";	
	String mes = "";
	String anyo = "";
	String artname = "";
	String traname = "";
	
	boolean isUser = true;
	
	public CustomValue(){};

	public CustomValue(String gender, String age, String country) {
		super();
		this.gender = gender;
		this.age = age;
		this.country = country;
		this.isUser = true; 
	}
	
	

	public CustomValue(String hora,String diaSemana,String semanaAnyo,String mes, String artname, String traname) {
		super();
		this.hora = hora;
		this.diaSemana = diaSemana;
		this.semanaAnyo = semanaAnyo;
		this.mes = mes;
		this.anyo = anyo;
		this.artname = artname;
		this.traname = traname;
		this.isUser = false; 
	}



	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(gender);
		out.writeUTF(age);
		out.writeUTF(country);		
		out.writeUTF(hora);
		out.writeUTF(diaSemana);
		out.writeUTF(semanaAnyo);
		out.writeUTF(mes);
		out.writeUTF(anyo);
		out.writeUTF(artname);
		out.writeUTF(traname);		
		out.writeBoolean(isUser);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.gender = in.readUTF();
		this.age = in.readUTF();
		this.country = in.readUTF();		
		this.hora = in.readUTF();
		this.diaSemana = in.readUTF();
		this.semanaAnyo = in.readUTF();
		this.mes = in.readUTF();
		this.anyo = in.readUTF();
		this.artname = in.readUTF();
		this.traname = in.readUTF();
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

	public String getHora() {
		return hora;
	}

	public void setHora(String hora) {
		this.hora = hora;
	}

	public String getDiaSemana() {
		return diaSemana;
	}

	public void setDiaSemana(String diaSemana) {
		this.diaSemana = diaSemana;
	}

	public String getSemanaAnyo() {
		return semanaAnyo;
	}

	public void setSemanaAnyo(String semanaAnyo) {
		this.semanaAnyo = semanaAnyo;
	}

	public String getMes() {
		return mes;
	}

	public void setMes(String mes) {
		this.mes = mes;
	}
	
	public String getAnyo() {
		return anyo;
	}

	public void setAnyo(String anyo) {
		this.anyo = anyo;
	}

	public String getTraname() {
		return traname;
	}

	public void setTraname(String traname) {
		this.traname = traname;
	}

	public boolean isUser() {
		return isUser;
	}

	public void setUser(boolean isUser) {
		this.isUser = isUser;
	}

	
	
}
