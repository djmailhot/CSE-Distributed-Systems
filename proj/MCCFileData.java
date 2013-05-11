public class MCCFileData {
  public static final char DELIMITER = '-';

  public final int versionNum;
  public final String filename;
  public final String contents;

  public MCCFileData(int versionNum, String filename, String contents) {
    this.versionNum = this.versionNum;
    this.filename = filename;
    this.contents = contents;
  }

  public String getVersionedFilename() {
    return String.format("%s%c%s", versionNum, DELIMITER, filename);
  }
}
