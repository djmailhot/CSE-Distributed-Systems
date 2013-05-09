import java.util.*;

import org.json.JSONException;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Represents an atomic filesystem transaction.
 *
 * This is an immutable container object holding a list of filesystem 
 * operations that make up this transaction.  Filesystem operations
 * contained in transactions are always write operations.
 *
 * @specfield tid : int  // the unique transaction id
 * @specfield ops : List<NFSOperation>  // a list of filesystem operations
 *
 * The unique transaction id should be the id of the twitter command
 * that produced this transaction.  To generate a unique id, use
 * edu.washington.cs.cse490h.lib.Utility.getRNG()
 *
 * New NFSTransaction objects are constructed using a builder pattern.
 * For example:
 * NFSTransaction.Builder(tid)
 *               .createFile(filename)
 *               .appendLine(filename,dataline)
 *               .build();
 * would return a NFSTransaction object representing one create file and one 
 * append data filesystem operation.
 */
public class NFSTransaction {

    /** The unique id for this transaction. */
    public final int tid; 
    /** An unmodifiable list of filesystem operations. */
    public final List<NFSOperation> ops;


    private NFSTransaction(Builder b) {
        this.tid = tid;
        this.ops = Collections.unmodifiableList(b.ops);
    }

    // TODO: NOT FINISHED
    public static byte[] serialize(NFSTransaction t) {
        JSONObject jsonObj = new JSONObject();
        jsonObj.put("tid", t.tid);

        JSONArray opsArray = new JSONArray();
        for(NFSOperation op : t.ops) {

        }
        return null;
    }

    // TODO: NOT FINISHED
    public static NFSTransaction deserialize(byte[] data) {
        return null;
    }
    

    /**
     * A NFSOperation object represents a single filesystem operation.
     */
    public static class NFSOperation {
        /** The filesystem operation type. */
        public final NFSOpType opType;
        /** The name of the file to target. */
        public final String filename;
        /** The line of data to include, or null if not applicable. */
        public final String dataline;

        public NFSOperation(NFSOpType opType, String filename, String dataline) {
            this.opType = opType;
            this.filename = filename;
            this.dataline = dataline;
        }

        public NFSOperation(NFSOpType opType, String filename) {
            this(opType, filename, null);
        }
    }

    /**
     * Enum to specify the NFS operation type.
     */
    public static enum NFSOpType {
      CREATEFILE("CREATEFILE"),
      APPENDLINE("APPENDLINE"),
      DELETEFILE("DELETEFILE"),
      DELETELINE("DELETELINE");

      private String value;

      private NFSOpType(String value) {
        this.value = value;
      }

      public static NFSOpType fromString(String value) {
        if(value.equals(CREATEFILE.value)) { return CREATEFILE; }
        else if(value.equals(APPENDLINE.value)) { return APPENDLINE; }
        else if(value.equals(DELETEFILE.value)) { return DELETEFILE; }
        else if(value.equals(DELETELINE.value)) { return DELETELINE; }
        else { 
          throw new IllegalArgumentException(value + " is not a valid op type");
        }
      }
    }

    /**
     * Builder to generate a NFSTransaction.
     */
    public static class Builder {
        private final int tid;
        private final List<NFSOperation> ops;

        public Builder(int tid) {
            this.tid = tid;
            this.ops = new ArrayList<NFSOperation>();
        }

        public Builder createFile(String filename) {
            ops.add(new NFSOperation(NFSOpType.CREATEFILE, filename));
        }

        public Builder appendLine(String filename, String line) {
            ops.add(new NFSOperation(NFSOpType.APPENDLINE, filename, line));
        }

        public Builder deleteFile(String filename) {
            ops.add(new NFSOperation(NFSOpType.DELETEFILE, filename));
        }

        public Builder deleteLine(String filename, String line) {
            ops.add(new NFSOperation(NFSOpType.DELETELINE, filename, line));
        }

        public NFSTransaction build() {
            return new NFSTransaction(this);
        }
    }

}
