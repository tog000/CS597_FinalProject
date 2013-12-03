package edu.boisestate.cs597.util;

import edu.boisestate.cs597.model.Crime;
import edu.boisestate.cs597.model.CrimeFrequency;

import java.awt.geom.Path2D;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class GlobalFunctions {

    private static final Pattern csvPat = Pattern.compile(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
    private static final Pattern tsvPat = Pattern.compile("\\t");
    //private static final SimpleDateFormat chiCsvDate = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss a");
    private static final SimpleDateFormat chiCsvDate = new SimpleDateFormat("MM/dd/yyyy");
    private static Calendar cal = Calendar.getInstance();

    //returns crimes in descending order
    public static LinkedList<String> parseTop50(String file) throws IOException
    {
        LinkedList<String> iucrs = new LinkedList<>();
        Path path = new Path(file);
        FileSystem fs = FileSystem.get(path.toUri(), new Configuration());
        InputStream is = (fs.open(new Path(path.toUri().toString())));
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, Charset.defaultCharset())))
        {
            String line;
            while ((line = reader.readLine()) != null)
            {
                String[] split = tsvPat.split(line);
                iucrs.addLast(GlobalFunctions.parseCrime(split[0]).getIUCR().toString());
                System.out.println(GlobalFunctions.parseCrime(split[0]));
            }
        }
        return iucrs;
    }

    public static void subsampleDataOnHdfs(String inputFile, String outputFile)
    {
        StringBuilder builder = new StringBuilder();
        Path path = new Path(inputFile);
        FileSystem fs = null;
        InputStream is = null;
        try
        {
            fs = FileSystem.get(path.toUri(), new Configuration());
            is = (fs.open(new Path(path.toUri().toString())));
        }
        catch (IOException e)
        {
            System.err.println("Error subsampling file");
            System.exit(1);
        }

        String line;
        int count = 0;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, Charset.defaultCharset())))
        {
            while ((line = reader.readLine()) != null)
            {
                if (count % 10000 == 0)
                {
                    builder.append(line);
                }
                ++count;
            }
        }
        catch (IOException ex)
        {
            System.err.println("Error subsampling file");
        }
    }

    public static void subsampleDataToFile(String inputFile, String outputFile)
    {
        StringBuilder builder = new StringBuilder();
        String line;
        int count = 0;
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(inputFile), Charset.defaultCharset()))
        {
            while ((line = reader.readLine()) != null)
            {
                if (count % 10000 == 0)
                {
                    builder.append(line).append("\n");
                }
                ++count;
            }

            BufferedWriter wr = Files.newBufferedWriter(Paths.get(outputFile), Charset.defaultCharset());
            wr.append(builder.toString());
        }
        catch (IOException ex)
        {
            System.err.println("Error subsampling file");
        }
    }

    public static Crime parseInitialCrime(String line, HashMap<Integer, Path2D.Double> communityPolygons)
    {
        String[] parts = csvPat.split(line, -1);
        Crime c = new Crime();
        Date date;
        if (parts.length >= 21)
        {
            try
            {
                date = chiCsvDate.parse(parts[2]);
                Long millis = date.getTime();
                c.date = new LongWritable(millis);

                c.block = new Text(parts[3]);

                c.IUCR = new Text(parts[4]);
                
                c.crimeDescription = new Text(parts[5]+" "+parts[6]);

                c.locationDescription = new Text(parts[7]);

                c.arrest = new BooleanWritable(Boolean.parseBoolean(parts[8]));

                c.communityArea = new IntWritable(-1);
                if (!parts[13].isEmpty())
                {
                    c.communityArea = new IntWritable(Integer.valueOf(parts[13]));
                }
                
                c.setFrequency(1);

                if (!parts[19].isEmpty())
                {
                    c.lat = new DoubleWritable(Double.valueOf(parts[19]));
                    c.lon = new DoubleWritable(Double.valueOf(parts[20]));

                    if (c.communityArea.get() != -1)
                    {
                        for (Map.Entry<Integer, Path2D.Double> entry : communityPolygons.entrySet())
                        {
                            if (entry.getValue().contains(c.lon.get(), c.lat.get()))
                            {
                                c.communityArea = new IntWritable(entry.getKey());
                            }
                        }
                    }
                }
                
                c.setCrimeRanking(-1);

                return c;
            }
            catch (ParseException | NumberFormatException e)
            {
                System.err.println("Error while parsing initial crime");
                e.printStackTrace();
                return null;
            }
        }
        else
        {
            return null;
        }
    }

    public static HashMap<Integer, Path2D.Double> extractPolygons(String pathToKmlOnHdfs)
    {
        HashMap<Integer, Path2D.Double> communityPolygons = new HashMap<>();
        if (!pathToKmlOnHdfs.isEmpty())
        {
            org.apache.hadoop.fs.Path pt = new Path(pathToKmlOnHdfs);

            try
            {
                FileSystem fs = FileSystem.get(new Configuration());
                DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
                DocumentBuilder dBuilder;
                dBuilder = dbFactory.newDocumentBuilder();
                Document document = dBuilder.parse(fs.open(pt));
                document.getDocumentElement().normalize();

                NodeList nodeList = document.getElementsByTagName("Placemark");
                Path2D.Double poly;

                for (int i = 0; i < nodeList.getLength(); i++)
                {
                    Node node = nodeList.item(i);

                    if (node.getNodeType() == Node.ELEMENT_NODE)
                    {
                        Element element = (Element) node;
                        String areaNumber = element.getElementsByTagName("name").item(0).getTextContent();
                        String coordinates = element.getElementsByTagName("coordinates").item(0).getTextContent();

                        poly = new Path2D.Double();

                        String parts1[] = coordinates.split(" ");

                        String parts2[];
                        boolean moved = false;
                        for (String coordinate : parts1)
                        {
                            if (!coordinate.isEmpty())
                            {
                                parts2 = coordinate.split(",");
                                if (moved)
                                {
                                    poly.lineTo(Math.abs(new Double(parts2[0])), Math.abs(new Double(parts2[1])));
                                }
                                else
                                {
                                    poly.moveTo(Math.abs(new Double(parts2[0])), Math.abs(new Double(parts2[1])));
                                    moved = true;
                                }
                            }
                        }
                        poly.closePath();

                        try
                        {
                            communityPolygons.put(Integer.valueOf(areaNumber), poly);
                        }
                        catch (Exception e)
                        {
                            System.err.println("Error while getting community area in extract polygons");
                            e.printStackTrace();
                            continue;
                        }
                    }
                }
            }
            catch (ParserConfigurationException | SAXException | IOException e)
            {
                System.err.println("Error while extracting polygon from xml");
                e.printStackTrace();
            }
        }
        return communityPolygons.isEmpty() ? null : communityPolygons;
    }

    public static CrimeFrequency parseCrimeFrequency(String line)
    {
        String[] split = csvPat.split(line, -1);
        try
        {
            return new CrimeFrequency(chiCsvDate.parse(split[2]).getTime(), split[4], split[5]);
        }
        catch (ArrayIndexOutOfBoundsException | ParseException | NumberFormatException e)
        {
            System.err.println("Error while parsing crime frequency");
            e.printStackTrace();
            return null;
        }
    }

    public static Crime parseCrime(String line)
    {
        String[] split = csvPat.split(line);
        Crime c = new Crime();
        try
        {
            c.setDate(new LongWritable(Long.parseLong(split[0])));
            c.setIUCR(new Text(split[1]));
            c.setCrimeDescription(new Text(split[2]));
            c.setBlock(new Text(split[3]));
            c.setLocationDescription(new Text(split[4]));
            c.setArrest(new BooleanWritable(Boolean.parseBoolean(split[5])));
            c.setCommunityArea(new IntWritable(Integer.parseInt(split[6])));
            c.setFrequency(new IntWritable(Integer.parseInt(split[7])));
            c.setLon(new DoubleWritable(Double.parseDouble(split[8])));
            c.setLat(new DoubleWritable(Double.parseDouble(split[9])));
            // Crime now also has a rating
            c.setCrimeRanking(Integer.parseInt(split[10].trim()));
            
            return c;
        }
        catch (NumberFormatException e)
        {
            System.err.println("Error while parsing crime.\nLINE="+line);
            e.printStackTrace();
            return null;
        }
    }
    
    public static String getMDYfromMillis(long millis)
    {
        cal.setTime(new Date(millis));
        int month = cal.get(Calendar.MONTH); //returns january as 0
        ++month;
        int day = cal.get(Calendar.DAY_OF_MONTH);
        int year = cal.get(Calendar.YEAR);
        return String.format("%s/%s/%d", month < 10 ? "0"+month : month+"" , day < 10 ? "0"+day : day+"" ,year);
    }
    
    
    public static class HashMapValueComparator implements Comparator<String>{
    	Map<String, Integer> base;
        public HashMapValueComparator(Map<String, Integer> base) {
            this.base = base;
        }
		@Override
		public int compare(String key1, String key2) {
			Integer freq1 = base.get(key1);
			Integer freq2 = base.get(key1);
			return freq2.compareTo(freq1);
		}
    }
    
    public static void main(String args[]){
    	String test = "1077562800000,"
    			+ "2825,019XX S HALSTED ST,"
    			+ "SMALL RETAIL STORE,"
    			+ "false,"
    			+ "31,"
    			+ "1,"
    			+ "-87.64673365029928,"
    			+ "41.855730164469,"
    			+ "1              ";
    	Crime c = GlobalFunctions.parseCrime(test);
    	System.out.println(c);
    }
    
}

    