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
    if(!exists(filename)) {
      return null;
    }
    PersistentStorageReader reader;
    reader = node.getReader(filename);

    List<String> lines = new ArrayList<String>();
    String line = reader.readLine();
    while(line != null) {
      lines.add(line);
      line = reader.readLine();
    }
    reader.close();
    return lines;
  }

  /**
   * Write to the specified file.
   *
   * If the file does not exist, creates a new one and writes;
   * If the file does exist, overwrites the contents;
   */
  public void write(String filename, String data) throws IOException {
    PersistentStorageWriter writer;
    writer = node.getWriter(filename, false);
    writer.delete();
    writer = node.getWriter(filename, false);
    writer.write(data);
    writer.close();
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
    return Arrays.asList(Utility.getFileHandle(node, ".").list());
  }

  /**
   * Append the specified string to the specified file.

   * If the file does not exist, the file will be created and appended to.
   *
   * The string will be followed by a newline, such that repeated append
   * calls will be written to separate lines.
   */
  public void append(String filename, String data) throws IOException {
    PersistentStorageWriter writer;
    writer = node.getWriter(filename, true);
    writer.append(data);
    writer.newLine();
    writer.close();
  }

  /**
   * @return true if the specified file exists.
   */
  public boolean exists(String filename) throws IOException {
    return Utility.fileExists(node, filename);
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
   * Prep a temp file for writing.
   *
   * @return the name of the temp file
   */
  private String prepWrite(String filename) {
    return null;
  }

  /**
   * Commit a temp file as written.
   *
   * @return the name of the temp file
   */
  private String commitWrite(String filename) {
    return null;
  }

}
