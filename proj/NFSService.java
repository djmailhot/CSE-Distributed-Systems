import edu.washington.cs.cse490h.lib.Node;
import edu.washington.cs.cse490h.lib.PersistentStorageWriter;
import edu.washington.cs.cse490h.lib.PersistentStorageReader;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The NFSService module provides a handy API to access the Named File Storage
 * of a particular node.
 */
public class NFSService {

  private final Node node;
  private final Map<String, Date> fileVersionMap;

  /**
   * Constructs an NFSService module for the specified node.
   *
   * @param node the Node that this storage belongs to.
   */
  public NFSService(Node node) {
    this.node = node;
    this.fileVersionMap = new HashMap<String, Date>();
  }


  /**
   * Create the specified file.
   */
  public void create(String filename) throws IOException {
    PersistentStorageWriter writer;
    writer = new PersistentStorageWriter(node, filename, false);
    writer.close();
  }

  /**
   * Read the specified file.
   */
  public List<String> read(String filename) throws IOException {
    PersistentStorageReader reader;
    reader = new PersistentStorageReader(node, filename);

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
   * Append the specified string to the specified file.
   * The string will be followed by a newline, such that repeated append
   * calls will be written to separate lines.
   */
  public void append(String filename, String data) throws IOException {
    PersistentStorageWriter writer;
    writer = new PersistentStorageWriter(node, filename, true);
    writer.append(data);
    writer.newLine();
    writer.close();
  }

  /**
   * Check that the version of the specified file is not newer than the
   * specified date.
   */
  public boolean check(String filename, Date date) throws IOException {
    // TODO: check the file version map?  Manually check the file version?
    return true;
  }

  /**
   * Delete the specified file.
   */
  public void delete(String filename) throws IOException {
    PersistentStorageWriter writer;
    writer = new PersistentStorageWriter(node, filename, false);
    writer.delete();
    writer.close();
  }
}
