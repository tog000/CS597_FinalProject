package edu.boisestate.cs597.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Crime implements WritableComparable<Crime>, Cloneable {
	public LongWritable date;
	public Text IUCR;
	public Text crimeDescription;
	public Text block;
	public Text locationDescription;
	public BooleanWritable arrest;
	public IntWritable communityArea;
	public IntWritable frequency;
	public DoubleWritable lon;
	public DoubleWritable lat;
	public IntWritable crimeRanking; 

	public Crime() {
		date = new LongWritable();
		IUCR = new Text();
		crimeDescription = new Text();
		block = new Text();
		locationDescription = new Text();
		arrest = new BooleanWritable();
		communityArea = new IntWritable();
		frequency = new IntWritable();
		lon = new DoubleWritable();
		lat = new DoubleWritable();
		crimeRanking = new IntWritable();
	}
        
	public void readFields(DataInput dataInput) throws IOException {
		date = new LongWritable();
		IUCR = new Text();
		crimeDescription = new Text();
		block = new Text();
		locationDescription = new Text();
		arrest = new BooleanWritable();
		communityArea = new IntWritable();
		frequency = new IntWritable();
		lon = new DoubleWritable();
		lat = new DoubleWritable();
		crimeRanking = new IntWritable();

		date.readFields(dataInput);
		IUCR.readFields(dataInput);
		crimeDescription.readFields(dataInput);
		block.readFields(dataInput);
		locationDescription.readFields(dataInput);
		arrest.readFields(dataInput);
		communityArea.readFields(dataInput);
		frequency.readFields(dataInput);
		lon.readFields(dataInput);
		lat.readFields(dataInput);
		crimeRanking.readFields(dataInput);
	}

	public void write(DataOutput dataOutput) throws IOException {
		date.write(dataOutput);
		IUCR.write(dataOutput);
		crimeDescription.write(dataOutput);
		block.write(dataOutput);
		locationDescription.write(dataOutput);
		arrest.write(dataOutput);
		communityArea.write(dataOutput);
		frequency.write(dataOutput);
		lon.write(dataOutput);
		lat.write(dataOutput);
		crimeRanking.write(dataOutput);
	}

	// Useful for sorting
	public int compareTo(Crime pw) {
		int cmp = this.getIUCR().compareTo(pw.getIUCR());
		return cmp;
	}

	@Override
	public String toString() {
		return date + "," + IUCR + "," + crimeDescription + "," + block + "," + locationDescription + ","
				+ arrest + "," + communityArea + "," + frequency + "," + lon + "," + lat + "," + crimeRanking + "\t";
	}

	@Override
	public Crime clone() throws CloneNotSupportedException {
		Crime c = new Crime();

		c.date = new LongWritable(this.date.get());
		c.IUCR = new Text(this.IUCR.toString());
		c.crimeDescription = new Text(this.crimeDescription.toString());
		c.block = new Text(this.block.toString());
		c.locationDescription = new Text(this.locationDescription.toString());
		c.arrest = new BooleanWritable(this.arrest.get());
		c.communityArea = new IntWritable(this.communityArea.get());
		c.frequency = new IntWritable(this.frequency.get());
		c.lon = new DoubleWritable(this.lon.get());
		c.lat = new DoubleWritable(this.lat.get());
		c.crimeRanking = new IntWritable(this.crimeRanking.get());

		return c;
	}
	
    public Text getCrimeDescription() {
		return crimeDescription;
	}

	public void setCrimeDescription(String crimeDescription) {
		this.crimeDescription = new Text(crimeDescription);
	}
	
	public void setCrimeDescription(Text crimeDescription) {
		this.crimeDescription = crimeDescription;
	}

	public IntWritable getFrequency() {
		return frequency;
	}

	public void setFrequency(IntWritable frequency) {
		this.frequency = frequency;
	}
	
	public void setFrequency(int frequency) {
		this.frequency = new IntWritable(frequency);
	}

	public LongWritable getDate()
    {
        return date;
    }

    public void setDate(LongWritable date)
    {
        this.date = date;
    }

    public void setDate(Date date)
    {
        this.date = new LongWritable(date.getTime());
    }

    public Text getIUCR()
    {
        return IUCR;
    }

    public void setIUCR(Text IUCR)
    {
        this.IUCR = IUCR;
    }
    
    public void setIUCR(String IUCR)
    {
        this.IUCR = new Text(IUCR);
    }

    public IntWritable getCrimeRanking() {
		return crimeRanking;
	}

	public void setCrimeRanking(IntWritable crimeRanking) {
		this.crimeRanking = crimeRanking;
	}
	
	public void setCrimeRanking(int crimeRanking) {
		this.crimeRanking = new IntWritable(crimeRanking);
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
