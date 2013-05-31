package edu.washington.cs.cse490h.lib;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Random;

/**
 * Provides some useful static methods.
 */
public class Utility {

    private static final String CHARSET = "US-ASCII";
    static Random randNumGen;

    public static Random getRNG() {
        return randNumGen;
    }

    /**
     * Convert a string to a byte[]
     * 
     * @param msg
     *            The string to convert
     * @return The byte[] that the string was converted to
     */
    public static byte[] stringToByteArray(String msg) {
        try {
            return msg.getBytes(CHARSET);
        } catch (UnsupportedEncodingException e) {
            System.err
                    .println("Exception occured while converting string to byte array. String: "
                            + msg + " Exception: " + e);
        }
        return null;
    }

    /**
     * Convert a byte[] to a string
     * 
     * @param msg
     *            The byte[] to convert
     * @return The converted String
     */
    public static String byteArrayToString(byte[] msg) {
        try {
            return new String(msg, CHARSET);
        } catch (UnsupportedEncodingException e) {
            System.err
                    .println("Exception occured while converting byte array to string. Exception: "
                            + e);
        }
        return null;
    }
    
    public static byte[] hexStringToByteArray(String s) {
	    int len = s.length();
	    byte[] data = new byte[len / 2];
	    for (int i = 0; i < len; i += 2) {
	        data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
	                             + Character.digit(s.charAt(i+1), 16));
	    }
	    return data;
	}

    /**
     * Escapes a string to be suitable for inclusion on a synoptic log event
     * line.
     * 
     * @param s
     *            the string to escape
     * @return the escaped string
     */
    public static String logEscape(String s) {
        if (s != null) {
            s = s.replace(" ", "_");
            s = s.replace("\n", "|");
            return "'" + s + "'";
        }
        return "''";
    }

    static String realFilename(int nodeAddr, String filename) {
        return "storage/" + nodeAddr + "/" + filename;
    }

    static void mkdirs(int nodeAddr) {
        File f = new File("storage/" + nodeAddr + "");
        if (!f.exists()) {
            f.mkdirs();
        }
        
        //then this node gets a copy of the secret key
        //just as with f.mkdirs above, we are bypassing all logging/possibility of failure because
        //	we are assuming this stuff if already in place at the start of the simulation.
        if(ServerList.in(nodeAddr)){
        	File skf = getFileHandle(nodeAddr, "secretKey");
        	BufferedWriter writer;
			try {
				writer = new BufferedWriter(new FileWriter(skf,false));
				StringBuilder sb = new StringBuilder();
	        	for(byte b : Simulator.serverKey){
	        		sb.append(String.format("%02x", b&0xff));
	        	}
	        	writer.write(sb.toString());
	            
	            writer.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}        	
        }
    }

    public static File getFileHandle(int n, String filename) {
        File file = new File(realFilename(n, filename));
        //file.getParentFile().mkdirs();
        return file;
    }

    public static boolean fileExists(int n, String filename) {
        File file = new File(realFilename(n, filename));
        return file.exists();
    }
}
