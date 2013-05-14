import java.io.Serializable;

public class MCCFileData implements Serializable {
  public final int versionNum;
  public final String filename;
  public final String contents;

  public MCCFileData(int versionNum, String filename, String contents) {
    this.versionNum = this.versionNum;
    this.filename = filename;
    this.contents = contents;
  }

  public String toString() {
    return String.format("MCCFileData{%s, %d}", filename, versionNum);
  }
}
