package edu.boisestate.cs597.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class DateTypeValue implements WritableComparable<DateTypeValue>, Cloneable{
	
	public static final String top50Prefix = "T";
	public static final String weatherPrefix = "W";
	public static final String healthPrefix = "H";
	public static final String economyPrefix = "E";
	
	public LongWritable date;
	public Text type;
	public FloatWritable value;
	
	public LongWritable getDate() {
		return date;
	}
	public void setDate(LongWritable date) {
		this.date = date;
	}
	public Text getType() {
		return type;
	}
	public void setType(Text type) {
		this.type = type;
	}
	public FloatWritable getValue() {
		return value;
	}
	public void setValue(FloatWritable value) {
		this.value = value;
	}
	
	public boolean isWeather(){
		return this.type.toString().startsWith(weatherPrefix);
	}
	
	public boolean isCrimeFrequency(){
		return this.type.toString().startsWith(top50Prefix);
	}
	
	public DateTypeValue() {
		this.date = new LongWritable();
		this.type = new Text();
		this.value = new FloatWritable();
	}
	public DateTypeValue(long date, String type, float value) {
		this.date = new LongWritable(date);
		this.type = new Text(type);
		this.value = new FloatWritable(value);
	}
	
	public void readFields(DataInput dataInput) throws IOException {
		date = new LongWritable();
		type = new Text();
		value = new FloatWritable();
		
		date.readFields(dataInput);
		type.readFields(dataInput);
		value.readFields(dataInput);
	}

	public void write(DataOutput dataOutput) throws IOException {
		date.write(dataOutput);
		type.write(dataOutput);
		value.write(dataOutput);
	}
	
	// Useful for sorting
	public int compareTo(DateTypeValue pw) {
		int cmp = this.date.compareTo(pw.date);
		return cmp;
	}

	@Override
	protected DateTypeValue clone() throws CloneNotSupportedException{
		DateTypeValue pw = new DateTypeValue();
		pw.date = new LongWritable(this.date.get());
		pw.type = new Text(this.type.toString());
		pw.value = new FloatWritable(this.value.get());
		return pw;
	}
}
