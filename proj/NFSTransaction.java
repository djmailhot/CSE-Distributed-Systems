import java.util.*;

/**
 * Represents an atomic filesystem transaction.
 *
 * This is an immutable container object holding a list of filesystem 
 * operations that make up this transaction.  Filesystem operations
 * contained in transactions are always write operations.
 *
 * @specfield tid : int  // the unique transaction id.  Must be positive
 * @specfield ops : List<NFSOperation>  // a list of filesystem operations
 *
 * Abstraction Invariant:
 *  AI(r):  r.tid > 0 && r.ops != null && !r.ops.contains(null)
 *
 * The unique transaction id should be the id of the twitter command
 * that produced this transaction.  To generate a unique id, use
 * edu.washington.cs.cse490h.lib.Utility.getRNG()
 *
 * New NFSTransaction objects are constructed using a builder pattern.
 * For example:
 * NFSTransaction.Builder(tid)
 *               .createFile(filename1)
 *               .appendLine(filename2,dataline)
 *               .touchFile(filename3)
 *               .build();
 * would return a NFSTransaction object representing a create file and an 
 * append data filesystem operation as well as a file read dependency.
 */
public class NFSTransaction {

    /** The unique id for this transaction. */
    public final int tid; 
    /** An unmodifiable list of filesystem operations. */
    public final List<NFSOperation> ops;


    private NFSTransaction(Builder b) {
        this.tid = b.tid;
        this.ops = Collections.unmodifiableList(b.ops);
        assert(checkRep());
    }

    private boolean checkRep() {
      return tid > 0 && ops != null && !ops.contains(null);
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
      TOUCHFILE("TOUCHFILE"),
      CREATEFILE("CREATEFILE"),
      APPENDLINE("APPENDLINE"),
      DELETEFILE("DELETEFILE"),
      DELETELINE("DELETELINE");

      private String value;

      private NFSOpType(String value) {
        this.value = value;
      }

      public static NFSOpType fromString(String value) {
        if(value.equals(TOUCHFILE.value)) { return TOUCHFILE; }
        else if(value.equals(CREATEFILE.value)) { return CREATEFILE; }
        else if(value.equals(APPENDLINE.value)) { return APPENDLINE; }
        else if(value.equals(DELETEFILE.value)) { return DELETEFILE; }
        else if(value.equals(DELETELINE.value)) { return DELETELINE; }
        else { 
          throw new IllegalArgumentException(value + " is not a valid op type");
        }
      }
    }

    /**
     * Builder methods to generate a NFSTransaction.
     */
    public static class Builder {
        private final int tid;
        private final List<NFSOperation> ops;

        public Builder(int tid) {
            this.tid = tid;
            this.ops = new ArrayList<NFSOperation>();
        }

        public Builder touchFile(String filename) {
            ops.add(new NFSOperation(NFSOpType.TOUCHFILE, filename));
            return this;
        }

        public Builder createFile(String filename) {
            ops.add(new NFSOperation(NFSOpType.CREATEFILE, filename));
            return this;
        }

        public Builder appendLine(String filename, String line) {
            ops.add(new NFSOperation(NFSOpType.APPENDLINE, filename, line));
            return this;
        }

        public Builder deleteFile(String filename) {
            ops.add(new NFSOperation(NFSOpType.DELETEFILE, filename));
            return this;
        }

        public Builder deleteLine(String filename, String line) {
            ops.add(new NFSOperation(NFSOpType.DELETELINE, filename, line));
            return this;
        }

        public NFSTransaction build() {
            return new NFSTransaction(this);
        }
    }
}
