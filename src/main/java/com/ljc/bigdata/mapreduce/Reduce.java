package com.ljc.bigdata.mapreduce;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.ljc.bigdata.measures.Counter.ANALYTICS;

import static com.ljc.bigdata.mapreduce.Driver.PROCESS_OUTPUT_FILE;

public class Reduce extends Reducer<Text, DoubleWritable, Text, Text>{
	
	private static final Log LOG = LogFactory.getLog(Reduce.class);
	private MultipleOutputs<Text, Text> multipleOutputs; // Archivo de salida
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		multipleOutputs = new MultipleOutputs<Text, Text>(context);
		context.getCounter(ANALYTICS.NUM_REDUCERS).increment(1);
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		multipleOutputs.close();
	}
	
	@Override
	public void reduce(final Text key, 
			final Iterable<DoubleWritable> values, Context context){
		/*
		Text key: Each City
		Iterable<DoubleWritable> values: Each Temperature record, one by one
		*/
		context.getCounter(ANALYTICS.NUM_GRUPOS).increment(1);
		
		LOG.info("Ciudad: " + key.toString());
		BigDecimal total = BigDecimal.ZERO;
		Long contador = 0l;
		
		for(final DoubleWritable temp: values){
			total = total.add(BigDecimal.valueOf(temp.get()));
			contador++;
		}
		
		BigDecimal media = total.divide(BigDecimal.valueOf(contador), RoundingMode.HALF_UP);
		
		String salida = "{\"city\":\"" + key.toString() + "\",\"average_temperature\":\"" + media.toString() + "ºC\"}";
		LOG.info(salida);
		
		try {
			multipleOutputs.write(PROCESS_OUTPUT_FILE, null, new Text(salida));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			LOG.error("Error en el mapeo :IOException: " + e.getMessage());
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Autoca-generated catch block
			LOG.error("Error en el mapeo :InterruptedException: " + e.getMessage());
			e.printStackTrace();
		}
		context.getCounter(ANALYTICS.LINES_WRITTEN).increment(1);
	}
}