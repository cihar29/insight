package postgres;

import elasticsearch.YouTubeService;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.ResultSet;

import java.io.*;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class Postgres {

    private Connection c;
    private static final String POSTGRES_HOST_VAR = "POSTGRES_HOST";
    private static String TABLE = "channel_ids";
    private static String filePath = "channels.txt";
    //get from your stack 'output'
    private static String callbackUrl = "https://xx.execute-api.us-east-1.amazonaws.com/Prod/notify";

    public static void main(String[] args) {
        Postgres pg = new Postgres();

        if (args.length != 0) {
            if      (args[0].equals("print"))       pg.printTable(TABLE);
            else if (args[0].equals("addChannels")) pg.addChannelsToDB(TABLE, filePath);
            else if (args[0].equals("subscribe"))   pg.subscribeToYoutube(TABLE, callbackUrl);
        }

        pg.close();
    }

    public Postgres() {
        try {
            c = DriverManager.getConnection(POSTGRES_HOST_VAR, "postgres", "nopass");
            System.out.println("Opened database successfully");
        } catch (Exception e) {
           e.printStackTrace();
           System.err.println(e.getClass().getName()+": "+e.getMessage());
        }
    }

    public void createTable(String table_name) {
        System.out.println("Creating table " + table_name);
        try {
            ResultSet result = c.getMetaData()
                .getTables(null, null, table_name, new String[] {"TABLE"});
            if (result.next()) {
                System.out.println("Table already exists!");
                return;
            }

            Statement s = c.createStatement();
            s.executeUpdate("CREATE TABLE " + table_name + " " +
                "(CHANNEL CHAR(30) PRIMARY KEY NOT NULL," +
                " UPDATED CHAR(50));");
            s.close();
            System.out.println("Success");

        } catch (Exception e) {
           e.printStackTrace();
           System.err.println(e.getClass().getName()+": "+e.getMessage());
        }
    }

    public void insertChannel(String table_name, Statement s, String channel_id) {
        try {
            //check if channel_id already exists
            ResultSet result = s.executeQuery("SELECT * from " + table_name + " where CHANNEL=\'" + channel_id + "\';");
            if (result.next()) {
                System.out.println("channel id " + channel_id + " already exists!");
                return;
            }

            String date = ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT);
            s.executeUpdate("INSERT INTO " + table_name + " " +
                "(CHANNEL,UPDATED)" +
                " VALUES (\'" + channel_id + "\', \'" + date + "\');");
        } catch (SQLException e) {
            e.printStackTrace();
            System.err.println(e.getClass().getName()+": "+e.getMessage());
        }
    }

    public void close() {
        try {
            c.close();
            System.out.println("Closed database successfully");
        } catch (Exception e) {
           e.printStackTrace();
           System.err.println(e.getClass().getName()+": "+e.getMessage());
        }
    }

    public void addChannelsToDB(String table_name, String fname) {
        try {
            c.setAutoCommit(false);
            Statement s = c.createStatement();

            File file = new File(fname);
            BufferedReader br = new BufferedReader(new FileReader(file));

            String st;
            while ((st = br.readLine()) != null) {
                String channelId = YouTubeService.getChannelIdFromString(st);
                if (channelId == null) continue;

                try {
                    insertChannel(table_name, s, channelId);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            s.close();
            c.commit();
        } catch (Exception e) {
           e.printStackTrace();
           System.err.println(e.getClass().getName()+": "+e.getMessage());
        }
    }

    public void subscribeToYoutube(String table_name, String url) {
        try {
            c.setAutoCommit(false);
            Statement s = c.createStatement();

            ResultSet result = s.executeQuery( "SELECT * FROM " + table_name + ";" );
            while ( result.next() ) {
                String id = result.getString("CHANNEL");
                YouTubeService.subscribeToChannelVideosPushNotifications(id, url);
//                System.out.println("Channel subscription request sent for channel " + id);
            }
            System.out.println("Channel subscription requests submitted");
            result.close();
            s.close();
        } catch (Exception e) {
           e.printStackTrace();
           System.err.println(e.getClass().getName()+": "+e.getMessage());
        }
    }

    public void printTable(String table_name) {
        try {
            c.setAutoCommit(false);
            Statement s = c.createStatement();

            ResultSet result = s.executeQuery( "SELECT * FROM " + table_name + ";" );
            System.out.println("CHANNEL\tUPDATED");
            while ( result.next() ) {
                System.out.println( result.getString("CHANNEL") + "\t" + result.getString("UPDATED") );
            }
            result.close();
            s.close();
        } catch (Exception e) {
           e.printStackTrace();
           System.err.println(e.getClass().getName()+": "+e.getMessage());
        }
    }
}
