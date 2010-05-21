package com.njtransit;

import java.io.IOException;
import java.util.List;

import au.com.bytecode.opencsv.CSVReader;

public interface ContentValuesProvider {
	List<List<Object>> getContentValues(CSVReader reader) throws IOException;
	String getInsertString();
}