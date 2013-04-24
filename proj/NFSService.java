import edu.washington.cs.cse490h.lib.Node;
import edu.washington.cs.cse490h.lib.PersistentStorageWriter;
import edu.washington.cs.cse490h.lib.PersistentStorageReader;
import edu.washington.cs.cse490h.lib.Utility;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * The NFSService module provides a handy API to access the Named File Storage
 * of a particular node.
 */
public class NFSService {
  private static final String TEMP_FILE_PREFIX = "_";

  private final Node node;

  /**
   * Constructs an NFSService module for the specified node.
   *
   * @param node the Node that this storage belongs to.
   */
  public NFSService(Node node) {
    this.node = node;
  }


  /**
   * Create the specified file.
   *
   * If the file already exists, nothing happens.
   */
  public void create(String filename) throws IOException {
    PersistentStorageWriter writer;
    writer = node.getWriter(filename, false);
    writer.close();
  }

  /**
   * Read the specified file.
   *
   * If the file does not exist, returns null;
   */
  public List<String> read(String filename) throws IOException {
	  System.out.println("read: " + filename);
    if(!exists(filename)) {
    	System.out.println("read1: " + filename);
      return null;
    }
    PersistentStorageReader reader;
    reader = node.getReader(filename);
    System.out.println("read2: " + reader);

    List<String> lines = new ArrayList<String>();
    String line = "";
    System.out.println("read2.5");
    try{
    	line = reader.readLine();	
    } catch (Exception e){
    	e.printStackTrace();
    }
    
    System.out.println("read3: " + line);
    while(line != null) {
      lines.add(line);
      System.out.println("read4");
      line = reader.readLine();
      
    }
    reader.close();
    System.out.println("read5");
    return lines;
  }

  /**
   * Write to the specified file.
   *
   * If the file does not exist, creates a new one and writes;
   * If the file does exist, overwrites the contents;
   *
   * @return true if the write was successful
   */
  public boolean write(String filename, String data) throws IOException {
    String tempfile = newTempFile(filename).getName();
    PersistentStorageWriter writer;
    writer = node.getWriter(tempfile, false);
    writer.write(data);
    writer.close();
    return commitTempFile(tempfile, filename);
  }

  /**
   * Rename the specified file to a new name.
   *
   * If the oldfile does not exist, nothing happens;
   * If the newfile already exists, overwrites the contents;
   *
   * @return true if file was renamed, false if not
   */
  public boolean rename(String oldname, String newname) throws IOException {
    File oldFile = Utility.getFileHandle(node, oldname);
    if(!oldFile.exists()) {
      return false;
    }
    File newFile = Utility.getFileHandle(node, newname);
    return oldFile.renameTo(newFile);
  }

  /**
   * Returns a list of filenames and directory names in this node's NFS root.
   * Note, this is not recursive on directories.
   *
   * All returned filenames are rooted at this node's file system root.
   * That is, the filenames can be safely passed to other NFSService methods.
   */
  public List<String> getFileList() throws IOException {
    File file = Utility.getFileHandle(node, ".");
    if(!file.exists()) {
      return new ArrayList<String>();
    }
    return Arrays.asList(file.list());
  }

  /**
   * Append the specified string to the specified file.

   * If the file does not exist, the file will be created and appended to.
   *
   * The string will be followed by a newline, such that repeated append
   * calls will be written to separate lines.
   *
   * @return true if the append was successful
   */
  public boolean append(String filename, String data) throws IOException {
    String tempfile = copyTempFile(filename);
    PersistentStorageWriter writer;
    writer = node.getWriter(tempfile, true);
    writer.append(data);
    writer.newLine();
    writer.close();
    return commitTempFile(tempfile, filename);
  }

  /**
   * Check that the version of the specified file is not newer than the
   * specified date.
   *
   * If the file does not exist, returns true.
   * 
   * @return true if the specified file's last modified date is no more recent
   *         than the specified date.
   */
  public boolean check(String filename, Date date) throws IOException {
    if(!exists(filename)) {
      return true;
    }
    File file = Utility.getFileHandle(node, filename);
    return Long.compare(file.lastModified(), date.getTime()) >= 0;
  }

  /**
   * Delete the specified file.
   *
   * If the file does not exist, nothing happens, returns false.
   *
   * @return true if deletion was successful.
   */
  public boolean delete(String filename) throws IOException {
    if(!exists(filename)) {
      return false;
    }
    PersistentStorageWriter writer;
    writer = node.getWriter(filename, false);
    boolean success = writer.delete();
    writer.close();
    return success;
  }

  /**
   * @return true if the specified file exists.
   */
  public boolean exists(String filename) throws IOException {
    return Utility.fileExists(node, filename);
  }


  /**
   * Delete all lines matching the specified line in the specified file.
   *
   * If the file does not exist, nothing happens, returns false.
   *
   * @return true if deletion was successful.
   */
  public boolean deleteLine(String filename, String line) throws IOException {
    if(!exists(filename)) {
      return false;
    }
    String tempfile = newTempFile(filename).getName();

    PersistentStorageWriter writer;
    writer = node.getWriter(tempfile, true);
    List<String> allLines = read(filename);
    for(String l : allLines) {
      if(!l.equals(line)) {
        writer.append(l);
        writer.newLine();
      }
    }

    return commitTempFile(tempfile, filename);
  }

  /**
   * Prep a temp file for writing.
   *
   * @return the name of the temp file
   */
  private File newTempFile(String filename) throws IOException {
    File root = Utility.getFileHandle(node, ".");
    return File.createTempFile(TEMP_FILE_PREFIX, filename, root);
  }

  /**
   * Prep a temp file for writing.
   *
   * @return the name of the temp file
   */
  private String copyTempFile(String filename) throws IOException {
    File tempfile = newTempFile(filename);
    List<String> lines = read(filename);
    for(String line : lines) {
      append(tempfile.getName(), line);
    }
    return tempfile.getName();
  }

  /**
   * Commit a temp file as an update to an original file.
   *
   * @return true if the commit was successful
   */
  private boolean commitTempFile(String tempfile, String origfile) 
      throws IOException {
    return rename(tempfile, origfile);
  }

}
