/**
 * 
 */
package edu.brown.cs.mapreduce.benchmarks.benchmark3.phase1;

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.brown.cs.mapreduce.BenchmarkBase;
import edu.brown.cs.mapreduce.benchmarks.CompositeKey;

/**
 * @author pavlo
 *
 */
public class TextMap extends BaseMap implements Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key,
			Text value, 
			OutputCollector<Text, Text> output, 
			Reporter reporter) throws IOException {
		//
		// Split the value using VALUE_DELIMITER into separate fields
		// The last field should be our revenue
		//
		String fields[] = value.toString().split("\\" + BenchmarkBase.VALUE_DELIMITER);
		CompositeKey ckey = null;
		
		//
		// HACK:
		// We have to figure out what the input type is based on the number of fields
		//
		if (fields.length == BenchmarkBase.RANKINGS_FIELDS.length) {
		 
		 ckey = new CompositeKey(fields[1], BenchmarkBase.RANKINGS_KEY);
		 value.set(fields[0] + BenchmarkBase.VALUE_DELIMITER + fields[2]);
		 
		} else if (fields.length == BenchmarkBase.USERVISITS_FIELDS.length) {
		 try {
		    //
		    // Grab the visitDate field and check whether it is in the range we are looking for
		    //
		    Date date = BaseMap.dateParser.parse(fields[2]);
		    if (date.compareTo(this.dates[BaseMap.START_DATE]) >= 0 &&
		        date.compareTo(this.dates[BaseMap.STOP_DATE]) <= 0) {
		       ckey = new CompositeKey(fields[1], BenchmarkBase.USERVISITS_KEY);
		        //
		        // Since we are now going to use the URL as they composite key, we 
		        // need to swap the source IP and URL from the values
		        //
		        String new_value = "";
		        
		        for (int ctr = 0; ctr < fields.length; ctr++) {
		           if (ctr == 0) {
		              new_value += fields[0].toString();
		           } else if(ctr > 1){
		              new_value += BenchmarkBase.VALUE_DELIMITER + fields[ctr];
		           }
		        }
		        value = new Text(new_value);
		        
		        if (this.DEBUG) System.out.println("Match: " + ckey.toString() + " -> " + new_value);
		    }
		 } catch (ParseException ex) {
		    ex.printStackTrace();
		    System.exit(1);
		 }
		} else {
		 System.err.println("ERROR: The input value on key '" + key + "' to Phase1Map has '" + fields.length + "' fields! Skipping...");
		 for (int i = 0; i < fields.length; i++) {
		    System.err.println("[" + i + "] " + fields[i]);
		 }
		 //System.exit(1);
		}
		//if (ckey != null) output.collect(ckey.toText(), value);
		if (ckey != null) output.collect(new Text(ckey.key), value);
		return;
	}
} // END CLASS