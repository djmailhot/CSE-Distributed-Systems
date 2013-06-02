package edu.washington.cs.cse490h.lib;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Random;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.Cipher;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.SecretKeySpec;

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
    
    public static String bytesToHexString(byte[] b){
    	StringBuilder sb = new StringBuilder();
		  for(byte current: b) sb.append(String.format("%02x", current&0xff));
		return sb.toString();
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
    
    /**
     * Returns a cryptographically-secure hashing of the bytes in s.
     * Can append a salt to the end, or optionally leave salt null for no salt.
     */
    public static byte[] hashBytes(String s, byte[] salt){
    	byte[] bytesToHash;
    	byte[] b = s.getBytes();
    	if(salt != null){
    		bytesToHash = new byte[b.length + salt.length];
    		System.arraycopy(b, 0, bytesToHash, 0, b.length);
    		System.arraycopy(salt, 0, bytesToHash, b.length, salt.length);
    	}
    	else{
    		bytesToHash = b;
    	}
    	
    	MessageDigest md;
		try {
			md = MessageDigest.getInstance("SHA-256");
			return md.digest(bytesToHash);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
			return null;
		}
    }
    
    public static byte[] AESEncrypt(byte[] input, byte[] key){
    	try{
	    	Cipher c = Cipher.getInstance("AES/ECB/PKCS5Padding");
	    	SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("AES");
	    	c.init(Cipher.ENCRYPT_MODE, keyFactory.generateSecret(new SecretKeySpec(key,"AES")));
	    	
	    	return c.doFinal(input);
    	} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchPaddingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidKeyException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidKeySpecException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalBlockSizeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (BadPaddingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	return null;
    }
    
    public static byte[] AESDecrypt(byte[] input, byte[] key){
    	try{
	    	Cipher c = Cipher.getInstance("AES/ECB/PKCS5Padding");
	    	SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("AES");
	    	c.init(Cipher.DECRYPT_MODE, keyFactory.generateSecret(new SecretKeySpec(key,"AES")));
	    	
	    	return c.doFinal(input);
    	} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchPaddingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidKeyException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidKeySpecException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalBlockSizeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (BadPaddingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	return null;
    	
    }

    static void mkdirs(int nodeAddr) {
        File f = new File("storage/" + nodeAddr + "");
        if (!f.exists()) {
            f.mkdirs();
        }
        
        //then this node gets a copy of the secret key
        //just as with f.mkdirs above, we are bypassing all logging/possibility of failure because
        //	we are assuming this stuff is already in place at the start of the simulation.
        if(ServerList.in(nodeAddr)){
        	File skf = getFileHandle(nodeAddr, "secretKey");
        	File ksf = getFileHandle(nodeAddr, "keyStore");
        	BufferedWriter writer;
        	BufferedWriter writer2;
			try {
				writer = new BufferedWriter(new FileWriter(skf,false));
				String s = bytesToHexString(Simulator.serverKey);
				
	        	writer.write(s);
	            
	            writer.close();
	            
	            writer2 = new BufferedWriter(new FileWriter(ksf,false));
	            writer2.write("");
	            writer2.close();
	            
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
