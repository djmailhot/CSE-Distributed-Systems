import java.io.Serializable;

public class MCCFileData implements Serializable {
  public static final long serialVersionUID = 0L;

  public final int versionNum;
  public final String filename;
  public final String contents;
  public final boolean deleted;

  public MCCFileData(int versionNum, String filename, String contents, boolean deleted) {
    this.versionNum = this.versionNum;
    this.filename = filename;
    this.contents = contents;
    this.deleted = deleted;
  }

  public String toString() {
    return String.format("MCCFileData{%s, %d, %s}", filename, versionNum, deleted);
  }
}
