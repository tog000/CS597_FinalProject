package edu.boisestate.cs597.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class CrimeFrequency implements WritableComparable<CrimeFrequency>, Cloneable {

    private LongWritable date;
    private Text iucr;          //most specific
    private Text crimeType;     //less specific
    private IntWritable frequency;

    public CrimeFrequency(Long date, String iucr, String crimeType)
    {
        this.date = new LongWritable(date);
        this.crimeType = new Text(crimeType);
        this.iucr = new Text(iucr);
        this.frequency = new IntWritable();
    }

    public CrimeFrequency()
    {
        this.date = new LongWritable();
        this.crimeType = new Text();
        this.iucr = new Text();
        this.frequency = new IntWritable();
    }

    private LongWritable getDate()
    {
        return date;
    }

    public void setDate(LongWritable date)
    {
        this.date = date;
    }

    public Text getIucr()
    {
        return iucr;
    }

    public void setIucr(Text iucr)
    {
        this.iucr = iucr;
    }

    public Text getCrimeType()
    {
        return crimeType;
    }

    public void setCrimeType(Text crimeType)
    {
        this.crimeType = crimeType;
    }

    public IntWritable getFrequency()
    {
        return frequency;
    }

    public void setFrequency(IntWritable frequency)
    {
        this.frequency = frequency;
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException
    {
        this.date = new LongWritable();
        this.iucr = new Text();
        this.crimeType = new Text();
        this.frequency = new IntWritable();

        this.date.readFields(dataInput);
        this.iucr.readFields(dataInput);
        this.crimeType.readFields(dataInput);
        this.frequency.readFields(dataInput);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException
    {
        this.date.write(dataOutput);
        this.iucr.write(dataOutput);
        this.crimeType.write(dataOutput);
        this.frequency.write(dataOutput);
    }

    @Override
    public int compareTo(CrimeFrequency cf)
    {
        return this.iucr.compareTo(cf.getIucr());
    }

    @Override
    public CrimeFrequency clone() throws CloneNotSupportedException
    {
        CrimeFrequency cf = new CrimeFrequency();
        cf.date = new LongWritable(this.date.get());
        cf.iucr = new Text(this.iucr.toString());
        cf.crimeType = new Text(this.crimeType.toString());
        cf.frequency = new IntWritable(this.frequency.get());
        return cf;
    }

    @Override
    public String toString()
    {
//        return "[iucr:"+this.iucr+", date:" + this.date+", crimeType:"+this.crimeType+", frequency:"+this.frequency+"]";
        return this.iucr + "\t" + this.date + "\t" + this.crimeType + "\t" + this.frequency;
    }
}
