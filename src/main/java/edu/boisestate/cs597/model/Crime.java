package edu.boisestate.cs597.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Crime implements WritableComparable<Crime>, Cloneable {
	public LongWritable date;
	public Text IUCR;
	public Text block;
	public Text locationDescription;
	public BooleanWritable arrest;
	public IntWritable communityArea;
	public DoubleWritable lon;
	public DoubleWritable lat;

	public Crime() {
		date = new LongWritable();
		IUCR = new Text();
		block = new Text();
		locationDescription = new Text();
		arrest = new BooleanWritable();
		communityArea = new IntWritable();
		lon = new DoubleWritable();
		lat = new DoubleWritable();
	}

	public void readFields(DataInput dataInput) throws IOException {
		date = new LongWritable();
		IUCR = new Text();
		block = new Text();
		locationDescription = new Text();
		arrest = new BooleanWritable();
		communityArea = new IntWritable();
		lon = new DoubleWritable();
		lat = new DoubleWritable();

		date.readFields(dataInput);
		IUCR.readFields(dataInput);
		block.readFields(dataInput);
		locationDescription.readFields(dataInput);
		arrest.readFields(dataInput);
		communityArea.readFields(dataInput);
		lon.readFields(dataInput);
		lat.readFields(dataInput);
	}

	public void write(DataOutput dataOutput) throws IOException {
		date.write(dataOutput);
		IUCR.write(dataOutput);
		block.write(dataOutput);
		locationDescription.write(dataOutput);
		arrest.write(dataOutput);
		communityArea.write(dataOutput);
		lon.write(dataOutput);
		lat.write(dataOutput);
	}

	// Useful for sorting
	public int compareTo(Crime pw) {
		int cmp = this.date.compareTo(pw.date);
		return cmp;
	}

	@Override
	public String toString() {
		return date + "," + IUCR + "," + block + "," + locationDescription + ","
				+ arrest + "," + communityArea + "," + lon+ "," + lat;
	}

	@Override
	public Crime clone() throws CloneNotSupportedException {
		Crime c = new Crime();

		c.date = new LongWritable(this.date.get());
		c.IUCR = new Text(this.IUCR.toString());
		c.block = new Text(this.block.toString());
		c.locationDescription = new Text(this.locationDescription.toString());
		c.arrest = new BooleanWritable(this.arrest.get());
		c.communityArea = new IntWritable(this.communityArea.get());
		c.lon = new DoubleWritable(this.lon.get());
		c.lat = new DoubleWritable(this.lat.get());

		return c;
	}
}
