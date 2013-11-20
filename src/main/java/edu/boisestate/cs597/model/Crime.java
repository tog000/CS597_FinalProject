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
		int cmp = this.getIUCR().compareTo(pw.getIUCR());
		return cmp;
	}

	@Override
	public String toString() {
		return date + "," + IUCR + "," + block + "," + locationDescription + ","
				+ arrest + "," + communityArea + "," + lon+ "," + lat + "\t";
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

    public LongWritable getDate()
    {
        return date;
    }

    public void setDate(LongWritable date)
    {
        this.date = date;
    }

    public Text getIUCR()
    {
        return IUCR;
    }

    public void setIUCR(Text IUCR)
    {
        this.IUCR = IUCR;
    }

    public Text getBlock()
    {
        return block;
    }

    public void setBlock(Text block)
    {
        this.block = block;
    }

    public Text getLocationDescription()
    {
        return locationDescription;
    }

    public void setLocationDescription(Text locationDescription)
    {
        this.locationDescription = locationDescription;
    }

    public BooleanWritable getArrest()
    {
        return arrest;
    }

    public void setArrest(BooleanWritable arrest)
    {
        this.arrest = arrest;
    }

    public IntWritable getCommunityArea()
    {
        return communityArea;
    }

    public void setCommunityArea(IntWritable communityArea)
    {
        this.communityArea = communityArea;
    }

    public DoubleWritable getLon()
    {
        return lon;
    }

    public void setLon(DoubleWritable lon)
    {
        this.lon = lon;
    }

    public DoubleWritable getLat()
    {
        return lat;
    }

    public void setLat(DoubleWritable lat)
    {
        this.lat = lat;
    }
        
        
}
