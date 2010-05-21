package com.njtransit;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;

import au.com.bytecode.opencsv.CSVReader;

/**
 * Hello world!
 * 
 */
public class App {
	
	private static DateFormat local = DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.FULL);
	private static DateFormat gmt = DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.FULL);
	
	static {
		gmt.setTimeZone(TimeZone.getTimeZone("GMT"));
	}
	
	public static void main(String[] args) throws Exception {
		HttpClient c = new HttpClient();
		PostMethod m = new PostMethod(
				"https://www.njtransit.com/mt/mt_servlet.srv?hdnPageAction=MTDevLoginSubmitTo");
		m.setRequestHeader("Content-Type", "application/x-www-form-urlencoded");
		m.setRequestEntity(new StringRequestEntity(
				"userName="+ System.getProperty("username")+"&password="+System.getProperty("password"),
				"application/x-www-form-urlencoded", "utf-8"));
		c.executeMethod(m);
		m.releaseConnection();
		GetMethod g = new GetMethod(
				"https://www.njtransit.com/mt/mt_servlet.srv?hdnPageAction=MTDevResourceDownloadTo&Category=rail");
		File railData = new File(System.getProperty("zip_destination"));
		if(railData.exists()) {			
			Date d = new Date(railData.lastModified());
			System.out.println(local.format(d));
			System.out.println(gmt.format(d));
			g.addRequestHeader("If-Modified-Since", gmt.format(d));
		}
		c.executeMethod(g);
		FileOutputStream fos = new FileOutputStream(
				railData);
		BufferedOutputStream bos = new BufferedOutputStream(fos);
		byte[] mybites = new byte[1024];
		while (g.getResponseBodyAsStream().read(mybites) != -1) {
			bos.write(mybites);
		}
		bos.close();
		
		if(g.getResponseHeader("Last-Modified")!=null) {
			railData.setLastModified(local.parse(g.getResponseHeader("Last-Modified").getValue()).getTime());
		}

		InputStream orig = App.class
				.getResourceAsStream("../../njtransit.sqlite");
		FileOutputStream out = new FileOutputStream(
				System.getProperty("destination"));
		while (orig.read(mybites) != -1) {
			out.write(mybites);
		}
		out.close();
		
		FileInputStream in = new FileInputStream(System.getProperty("destination"));
		
		Class.forName("org.sqlite.JDBC");
		Connection conn = DriverManager
				.getConnection("jdbc:sqlite:"+System.getProperty("destination"));
		Statement stat = conn.createStatement();
		String[] creates = new String[] {
				"create table if not exists trips(id int primary key, route_id int, service_id int, headsign varchar(255), direction int, block_id varchar(255))",
				"create table if not exists stops(id int primary key, name varchar(255), desc varchar(255), lat real, lon real, zone_id)",
				"create table if not exists stop_times(trip_id int, arrival int, departure int, stop_id int, sequence int, pickup_type int, drop_off_type int)",
				"create table if not exists routes(id int primary key, agency_id int, short_name varchar(255), long_name varchar(255), route_type int)",
				"create table if not exists calendar(service_id int, monday int, tuesday int, wednesday int, thursday int, friday int, saturday int, sunday int, start date, end date)",
				// agency_id,agency_name,agency_url,agency_timezone
				"create table if not exists calendar_dates(service_id int, calendar_date int, exception_type int)",
				"create table if not exists agency(id int primary key, name varchar(255), url varchar(255))" };
		for (String createTable : creates) {
			stat.executeUpdate(createTable);
		}

		final SimpleDateFormat df = new SimpleDateFormat("dd/MM/yyyy kk:mm:ss");
		final TransactionManager manager = new TransactionManager(conn);

		loadPartitioned(manager, "stop_times", new ContentValuesProvider() {

			@Override
			public List<List<Object>> getContentValues(CSVReader reader)
					throws IOException {
				List<List<Object>> values = new ArrayList<List<Object>>();
				String[] nextLine;
				while ((nextLine = reader.readNext()) != null) {
					// trip_id,arrival_time,departure_time,stop_id,stop_sequence,pickup_type,drop_off_type
					List<Object> o = new ArrayList<Object>();
					o.add(nextLine[0]);
					try {
						if (nextLine[1].trim().length() != 0) {
							o.add(df.parse("01/01/1970 " + nextLine[1])
									.getTime());
						}
						if (nextLine[2].trim().length() != 0) {
							o.add(df.parse("01/01/1970 " + nextLine[2])
									.getTime());
						}
					} catch (Exception e) {
						e.printStackTrace();
						throw new RuntimeException(e);
					}

					o.add(nextLine[3]);
					o.add(nextLine[4]);
					o.add(nextLine[5]);
					o.add(nextLine[6]);
					values.add(o);
				}
				return values;
			}

			@Override
			public String getInsertString() {
				return "insert into stop_times (trip_id,arrival,departure,stop_id,sequence,pickup_type,drop_off_type) values (?,?,?,?,?,?,?)";
			}

		});

		loadPartitioned(manager, "trips", new ContentValuesProvider() {

			@Override
			public List<List<Object>> getContentValues(CSVReader reader)
					throws IOException {
				List<List<Object>> values = new ArrayList<List<Object>>();
				String[] nextLine;
				while ((nextLine = reader.readNext()) != null) {
					// trip_id,arrival_time,departure_time,stop_id,stop_sequence,pickup_type,drop_off_type
					List<Object> o = new ArrayList<Object>();
					o.add(nextLine[0]);
					o.add(nextLine[1]);
					o.add(nextLine[2]);
					o.add(nextLine[3]);
					o.add(nextLine[4]);
					o.add(nextLine[5]);
					values.add(o);
				}
				return values;
			}

			@Override
			public String getInsertString() {
				return "insert into trips (route_id,service_id,id,direction,headsign,block_id) values (?,?,?,?,?,?)";
			}

		});

		loadPartitioned(manager, "stops", new ContentValuesProvider() {

			@Override
			public List<List<Object>> getContentValues(CSVReader reader)
					throws IOException {
				List<List<Object>> values = new ArrayList<List<Object>>();
				String[] nextLine;
				while ((nextLine = reader.readNext()) != null) {
					// trip_id,arrival_time,departure_time,stop_id,stop_sequence,pickup_type,drop_off_type
					List<Object> o = new ArrayList<Object>();
					o.add(nextLine[0]);
					o.add(nextLine[1]);
					o.add(nextLine[2]);
					o.add(nextLine[3]);
					o.add(nextLine[4]);
					o.add(nextLine[5]);
					values.add(o);
				}
				return values;
			}

			@Override
			public String getInsertString() {
				// stop_id,stop_name,stop_desc,stop_lat,stop_lon,zone_id
				return "insert into stops (id,name,desc,lat,lon,zone_id) values (?,?,?,?,?,?)";
			}

		});

		loadPartitioned(manager, "calendar", new ContentValuesProvider() {

			@Override
			public List<List<Object>> getContentValues(CSVReader reader)
					throws IOException {
				List<List<Object>> values = new ArrayList<List<Object>>();
				String[] nextLine;
				while ((nextLine = reader.readNext()) != null) {
					// trip_id,arrival_time,departure_time,stop_id,stop_sequence,pickup_type,drop_off_type
					List<Object> o = new ArrayList<Object>();
					o.add(nextLine[0]);
					o.add(nextLine[1]);
					o.add(nextLine[2]);
					o.add(nextLine[3]);
					o.add(nextLine[4]);
					o.add(nextLine[5]);
					o.add(nextLine[6]);
					o.add(nextLine[7]);
					String start = nextLine[8];
					String end = nextLine[9];
					String year = start.substring(0, 4);
					String month = start.substring(4, 6);
					String day = start.substring(6, 8);
					o.add(year + "-" + month + "-" + day);
					year = end.substring(0, 4);
					month = end.substring(4, 6);
					day = end.substring(6, 8);
					o.add(year + "-" + month + "-" + day);
					values.add(o);
				}
				return values;
			}

			@Override
			public String getInsertString() {
				// service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date
				return "insert into calendar (service_id,monday,tuesday,wednesday,thursday,friday, saturday, sunday, start, end) values (?,?,?,?,?,?,?,?,?,?)";
			}

		});

		loadPartitioned(manager, "calendar_dates", new ContentValuesProvider() {

			@Override
			public List<List<Object>> getContentValues(CSVReader reader)
					throws IOException {
				reader.readNext();
				List<List<Object>> values = new ArrayList<List<Object>>();
				String[] nextLine;
				while ((nextLine = reader.readNext()) != null) {
					// trip_id,arrival_time,departure_time,stop_id,stop_sequence,pickup_type,drop_off_type
					List<Object> o = new ArrayList<Object>();
					o.add(nextLine[0]);
					String start = nextLine[1];
					String year = start.substring(0, 4);
					String month = start.substring(4, 6);
					String day = start.substring(6, 8);
					o.add(year + "-" + month + "-" + day);
					o.add(nextLine[2]);
					values.add(o);
				}
				return values;
			}

			@Override
			public String getInsertString() {
				// service_id,date,exception_type
				return "insert into calendar_dates (service_id,calendar_date,exception_type) values (?,?,?)";
			}

		});

		loadPartitioned(manager, "routes", new ContentValuesProvider() {

			@Override
			public List<List<Object>> getContentValues(CSVReader reader)
					throws IOException {
				List<List<Object>> values = new ArrayList<List<Object>>();
				String[] nextLine;
				while ((nextLine = reader.readNext()) != null) {
					// trip_id,arrival_time,departure_time,stop_id,stop_sequence,pickup_type,drop_off_type
					List<Object> o = new ArrayList<Object>();
					o.add(nextLine[0]);
					o.add(nextLine[1]);
					o.add(nextLine[2]);
					o.add(nextLine[3]);
					o.add(nextLine[4]);
					values.add(o);
				}
				return values;
			}

			@Override
			public String getInsertString() {
				// route_id,agency_id,route_short_name,route_long_name,route_type
				return "insert into routes (id,agency_id,short_name,long_name,route_type) values (?,?,?,?,?)";
			}

		});

		loadPartitioned(manager, "agency", new ContentValuesProvider() {

			@Override
			public List<List<Object>> getContentValues(CSVReader reader)
					throws IOException {
				List<List<Object>> values = new ArrayList<List<Object>>();
				String[] nextLine;
				while ((nextLine = reader.readNext()) != null) {
					// trip_id,arrival_time,departure_time,stop_id,stop_sequence,pickup_type,drop_off_type
					List<Object> o = new ArrayList<Object>();
					o.add(nextLine[0]);
					o.add(nextLine[1]);
					o.add(nextLine[2]);
					values.add(o);
				}
				return values;
			}

			@Override
			public String getInsertString() {
				// agency_id,agency_name,agency_url
				return "insert into agency (id,name,url) values (?,?,?)";
			}

		});

		MessageDigest d = MessageDigest.getInstance("sha");
				
		while(in.read(mybites)!=-1) {
			d.update(mybites);
		}
		out = new FileOutputStream(System.getProperty("destination")+".sha");
		out.write(d.digest());
	}

	private static void loadPartitioned(TransactionManager tm,
			final String tableName, final ContentValuesProvider valuesProvider)
			throws SQLException {
		load(tm, tableName, valuesProvider);
	}

	private static void load(TransactionManager tm, final String tableName,
			final ContentValuesProvider valuesProvider) throws SQLException {
		tm.exec(new Transactional() {
			@Override
			public void work(Connection conn) {
				InputStream input = null;
				CSVReader reader = null;
				try {
					final String fileName = tableName + ".txt";
					input = App.class.getResourceAsStream("../../" + fileName);
					reader = new CSVReader(new InputStreamReader(input));
					reader.readNext(); // read in the header line
					List<List<Object>> values = valuesProvider
							.getContentValues(reader);
					PreparedStatement s = conn.prepareStatement(valuesProvider
							.getInsertString());
					for (List<Object> cv : values) {
						for (int i = 0; i < cv.size(); i++) {
							s.setObject(i + 1, cv.get(i));
						}
						s.addBatch();
					}
					System.out.println(s.executeBatch().length);
				} catch (Throwable t) {
					t.printStackTrace();
					throw new RuntimeException(t);
				} finally {
					try {
						input.close();
						reader.close();
					} catch (Throwable t) {
						t.printStackTrace();
						throw new RuntimeException(t);
					}
				}
			}
		});
	}

}
