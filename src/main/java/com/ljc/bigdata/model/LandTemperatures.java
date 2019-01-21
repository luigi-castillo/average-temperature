package com.ljc.bigdata.model;

public class LandTemperatures {
	public static final String VALUE_SEPARATOR = ",";
	
	public static enum COLUMNS {
		DATE,
		AVERAGE_TEMPERATURE,
		AVERAGE_TEMPERATURE_UNCERTAINLY,
		CITY,
		COUNTRY,
		LATITUDE,
		LONGITUDE
	}
}
