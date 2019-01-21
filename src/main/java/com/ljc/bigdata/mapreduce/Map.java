package com.ljc.bigdata.mapreduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.ljc.bigdata.measures.Counter.ANALYTICS;
import com.ljc.bigdata.model.LandTemperatures;

public class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	/* The key is nothing but the offset of each line in the text file: LongWritable
	 * The value is each individual linew: Text
	 */
	private static final Log LOG = LogFactory.getLog(Map.class);
	
	@Override
	public void map(LongWritable key, Text value, Context context) {
		context.getCounter(ANALYTICS.NUM_MAPPERS).increment(1);
		try {
			String linea = value.toString();
			String[] campos = linea.split(LandTemperatures.VALUE_SEPARATOR);

			String average_temperature = campos[LandTemperatures.COLUMNS.AVERAGE_TEMPERATURE
					.ordinal()];

			if (!average_temperature.isEmpty()) {
				context.write(
						new Text(campos[LandTemperatures.COLUMNS.CITY.ordinal()]),
						new DoubleWritable(new Double(average_temperature)));
				context.getCounter(ANALYTICS.LINES_MAP).increment(1);
			}
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			LOG.error("Error en el mapeo :NumberFormatException: " + e.getMessage());
			e.printStackTrace();			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			LOG.error("Error en el mapeo :IOException: " + e.getMessage());
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			LOG.error("Error en el mapeo :InterruptedException: " + e.getMessage());
			e.printStackTrace();
		}
	}
}