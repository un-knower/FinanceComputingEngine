// ORM class for table 'LSETCSSYSJJ'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Tue Sep 18 17:02:11 CST 2018
// For connector: org.apache.sqoop.manager.OracleManager
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;
import com.cloudera.sqoop.lib.JdbcWritableBridge;
import com.cloudera.sqoop.lib.DelimiterSet;
import com.cloudera.sqoop.lib.FieldFormatter;
import com.cloudera.sqoop.lib.RecordParser;
import com.cloudera.sqoop.lib.BooleanParser;
import com.cloudera.sqoop.lib.BlobRef;
import com.cloudera.sqoop.lib.ClobRef;
import com.cloudera.sqoop.lib.LargeObjectLoader;
import com.cloudera.sqoop.lib.SqoopRecord;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class LSETCSSYSJJ extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("FSETCODE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        FSETCODE = (java.math.BigDecimal)value;
      }
    });
    setters.put("FJJLX", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        FJJLX = (java.math.BigDecimal)value;
      }
    });
    setters.put("FJJZL", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        FJJZL = (java.math.BigDecimal)value;
      }
    });
    setters.put("FJJLB", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        FJJLB = (java.math.BigDecimal)value;
      }
    });
    setters.put("FJJWTREN", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        FJJWTREN = (String)value;
      }
    });
    setters.put("FJJGLREN", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        FJJGLREN = (String)value;
      }
    });
    setters.put("FJJTGREN", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        FJJTGREN = (String)value;
      }
    });
    setters.put("FJJCLDATE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        FJJCLDATE = (java.sql.Date)value;
      }
    });
    setters.put("FJJDQDATE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        FJJDQDATE = (java.sql.Date)value;
      }
    });
    setters.put("FQCJZ", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        FQCJZ = (java.math.BigDecimal)value;
      }
    });
    setters.put("FPJE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        FPJE = (java.math.BigDecimal)value;
      }
    });
    setters.put("FPJELJ", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        FPJELJ = (java.math.BigDecimal)value;
      }
    });
    setters.put("FZSBZZHXS", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        FZSBZZHXS = (java.math.BigDecimal)value;
      }
    });
    setters.put("FJJGLRFV", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        FJJGLRFV = (java.math.BigDecimal)value;
      }
    });
    setters.put("FJJTGRFV", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        FJJTGRFV = (java.math.BigDecimal)value;
      }
    });
    setters.put("FJJXSRFV", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        FJJXSRFV = (java.math.BigDecimal)value;
      }
    });
    setters.put("FJFRI", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        FJFRI = (java.math.BigDecimal)value;
      }
    });
    setters.put("FTAMS", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        FTAMS = (String)value;
      }
    });
    setters.put("FZHGLR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        FZHGLR = (String)value;
      }
    });
    setters.put("FNJLX", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        FNJLX = (String)value;
      }
    });
    setters.put("FSSSF", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        FSSSF = (String)value;
      }
    });
    setters.put("FTGZH", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        FTGZH = (String)value;
      }
    });
    setters.put("FJZCYTJ", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        FJZCYTJ = (java.math.BigDecimal)value;
      }
    });
    setters.put("HSTABB", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        HSTABB = (java.math.BigDecimal)value;
      }
    });
    setters.put("FTGRJC", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        FTGRJC = (String)value;
      }
    });
    setters.put("FSH", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        FSH = (java.math.BigDecimal)value;
      }
    });
    setters.put("FZZR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        FZZR = (String)value;
      }
    });
    setters.put("FCHK", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        FCHK = (String)value;
      }
    });
    setters.put("FSTARTDATE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        FSTARTDATE = (java.sql.Date)value;
      }
    });
    setters.put("FKHH", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        FKHH = (String)value;
      }
    });
    setters.put("FQSDATE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        FQSDATE = (java.sql.Date)value;
      }
    });
    setters.put("FZCZT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        FZCZT = (java.math.BigDecimal)value;
      }
    });
    setters.put("FPLANLICID", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        FPLANLICID = (String)value;
      }
    });
    setters.put("FYYJG", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        FYYJG = (String)value;
      }
    });
    setters.put("FZYNJ", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        FZYNJ = (java.math.BigDecimal)value;
      }
    });
    setters.put("FSTRJHBM", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        FSTRJHBM = (String)value;
      }
    });
    setters.put("FDLRJHBM", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        FDLRJHBM = (String)value;
      }
    });
    setters.put("FTZZHDM", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        FTZZHDM = (String)value;
      }
    });
  }
  public LSETCSSYSJJ() {
    init0();
  }
  private java.math.BigDecimal FSETCODE;
  public java.math.BigDecimal get_FSETCODE() {
    return FSETCODE;
  }
  public void set_FSETCODE(java.math.BigDecimal FSETCODE) {
    this.FSETCODE = FSETCODE;
  }
  public LSETCSSYSJJ with_FSETCODE(java.math.BigDecimal FSETCODE) {
    this.FSETCODE = FSETCODE;
    return this;
  }
  private java.math.BigDecimal FJJLX;
  public java.math.BigDecimal get_FJJLX() {
    return FJJLX;
  }
  public void set_FJJLX(java.math.BigDecimal FJJLX) {
    this.FJJLX = FJJLX;
  }
  public LSETCSSYSJJ with_FJJLX(java.math.BigDecimal FJJLX) {
    this.FJJLX = FJJLX;
    return this;
  }
  private java.math.BigDecimal FJJZL;
  public java.math.BigDecimal get_FJJZL() {
    return FJJZL;
  }
  public void set_FJJZL(java.math.BigDecimal FJJZL) {
    this.FJJZL = FJJZL;
  }
  public LSETCSSYSJJ with_FJJZL(java.math.BigDecimal FJJZL) {
    this.FJJZL = FJJZL;
    return this;
  }
  private java.math.BigDecimal FJJLB;
  public java.math.BigDecimal get_FJJLB() {
    return FJJLB;
  }
  public void set_FJJLB(java.math.BigDecimal FJJLB) {
    this.FJJLB = FJJLB;
  }
  public LSETCSSYSJJ with_FJJLB(java.math.BigDecimal FJJLB) {
    this.FJJLB = FJJLB;
    return this;
  }
  private String FJJWTREN;
  public String get_FJJWTREN() {
    return FJJWTREN;
  }
  public void set_FJJWTREN(String FJJWTREN) {
    this.FJJWTREN = FJJWTREN;
  }
  public LSETCSSYSJJ with_FJJWTREN(String FJJWTREN) {
    this.FJJWTREN = FJJWTREN;
    return this;
  }
  private String FJJGLREN;
  public String get_FJJGLREN() {
    return FJJGLREN;
  }
  public void set_FJJGLREN(String FJJGLREN) {
    this.FJJGLREN = FJJGLREN;
  }
  public LSETCSSYSJJ with_FJJGLREN(String FJJGLREN) {
    this.FJJGLREN = FJJGLREN;
    return this;
  }
  private String FJJTGREN;
  public String get_FJJTGREN() {
    return FJJTGREN;
  }
  public void set_FJJTGREN(String FJJTGREN) {
    this.FJJTGREN = FJJTGREN;
  }
  public LSETCSSYSJJ with_FJJTGREN(String FJJTGREN) {
    this.FJJTGREN = FJJTGREN;
    return this;
  }
  private java.sql.Date FJJCLDATE;
  public java.sql.Date get_FJJCLDATE() {
    return FJJCLDATE;
  }
  public void set_FJJCLDATE(java.sql.Date FJJCLDATE) {
    this.FJJCLDATE = FJJCLDATE;
  }
  public LSETCSSYSJJ with_FJJCLDATE(java.sql.Date FJJCLDATE) {
    this.FJJCLDATE = FJJCLDATE;
    return this;
  }
  private java.sql.Date FJJDQDATE;
  public java.sql.Date get_FJJDQDATE() {
    return FJJDQDATE;
  }
  public void set_FJJDQDATE(java.sql.Date FJJDQDATE) {
    this.FJJDQDATE = FJJDQDATE;
  }
  public LSETCSSYSJJ with_FJJDQDATE(java.sql.Date FJJDQDATE) {
    this.FJJDQDATE = FJJDQDATE;
    return this;
  }
  private java.math.BigDecimal FQCJZ;
  public java.math.BigDecimal get_FQCJZ() {
    return FQCJZ;
  }
  public void set_FQCJZ(java.math.BigDecimal FQCJZ) {
    this.FQCJZ = FQCJZ;
  }
  public LSETCSSYSJJ with_FQCJZ(java.math.BigDecimal FQCJZ) {
    this.FQCJZ = FQCJZ;
    return this;
  }
  private java.math.BigDecimal FPJE;
  public java.math.BigDecimal get_FPJE() {
    return FPJE;
  }
  public void set_FPJE(java.math.BigDecimal FPJE) {
    this.FPJE = FPJE;
  }
  public LSETCSSYSJJ with_FPJE(java.math.BigDecimal FPJE) {
    this.FPJE = FPJE;
    return this;
  }
  private java.math.BigDecimal FPJELJ;
  public java.math.BigDecimal get_FPJELJ() {
    return FPJELJ;
  }
  public void set_FPJELJ(java.math.BigDecimal FPJELJ) {
    this.FPJELJ = FPJELJ;
  }
  public LSETCSSYSJJ with_FPJELJ(java.math.BigDecimal FPJELJ) {
    this.FPJELJ = FPJELJ;
    return this;
  }
  private java.math.BigDecimal FZSBZZHXS;
  public java.math.BigDecimal get_FZSBZZHXS() {
    return FZSBZZHXS;
  }
  public void set_FZSBZZHXS(java.math.BigDecimal FZSBZZHXS) {
    this.FZSBZZHXS = FZSBZZHXS;
  }
  public LSETCSSYSJJ with_FZSBZZHXS(java.math.BigDecimal FZSBZZHXS) {
    this.FZSBZZHXS = FZSBZZHXS;
    return this;
  }
  private java.math.BigDecimal FJJGLRFV;
  public java.math.BigDecimal get_FJJGLRFV() {
    return FJJGLRFV;
  }
  public void set_FJJGLRFV(java.math.BigDecimal FJJGLRFV) {
    this.FJJGLRFV = FJJGLRFV;
  }
  public LSETCSSYSJJ with_FJJGLRFV(java.math.BigDecimal FJJGLRFV) {
    this.FJJGLRFV = FJJGLRFV;
    return this;
  }
  private java.math.BigDecimal FJJTGRFV;
  public java.math.BigDecimal get_FJJTGRFV() {
    return FJJTGRFV;
  }
  public void set_FJJTGRFV(java.math.BigDecimal FJJTGRFV) {
    this.FJJTGRFV = FJJTGRFV;
  }
  public LSETCSSYSJJ with_FJJTGRFV(java.math.BigDecimal FJJTGRFV) {
    this.FJJTGRFV = FJJTGRFV;
    return this;
  }
  private java.math.BigDecimal FJJXSRFV;
  public java.math.BigDecimal get_FJJXSRFV() {
    return FJJXSRFV;
  }
  public void set_FJJXSRFV(java.math.BigDecimal FJJXSRFV) {
    this.FJJXSRFV = FJJXSRFV;
  }
  public LSETCSSYSJJ with_FJJXSRFV(java.math.BigDecimal FJJXSRFV) {
    this.FJJXSRFV = FJJXSRFV;
    return this;
  }
  private java.math.BigDecimal FJFRI;
  public java.math.BigDecimal get_FJFRI() {
    return FJFRI;
  }
  public void set_FJFRI(java.math.BigDecimal FJFRI) {
    this.FJFRI = FJFRI;
  }
  public LSETCSSYSJJ with_FJFRI(java.math.BigDecimal FJFRI) {
    this.FJFRI = FJFRI;
    return this;
  }
  private String FTAMS;
  public String get_FTAMS() {
    return FTAMS;
  }
  public void set_FTAMS(String FTAMS) {
    this.FTAMS = FTAMS;
  }
  public LSETCSSYSJJ with_FTAMS(String FTAMS) {
    this.FTAMS = FTAMS;
    return this;
  }
  private String FZHGLR;
  public String get_FZHGLR() {
    return FZHGLR;
  }
  public void set_FZHGLR(String FZHGLR) {
    this.FZHGLR = FZHGLR;
  }
  public LSETCSSYSJJ with_FZHGLR(String FZHGLR) {
    this.FZHGLR = FZHGLR;
    return this;
  }
  private String FNJLX;
  public String get_FNJLX() {
    return FNJLX;
  }
  public void set_FNJLX(String FNJLX) {
    this.FNJLX = FNJLX;
  }
  public LSETCSSYSJJ with_FNJLX(String FNJLX) {
    this.FNJLX = FNJLX;
    return this;
  }
  private String FSSSF;
  public String get_FSSSF() {
    return FSSSF;
  }
  public void set_FSSSF(String FSSSF) {
    this.FSSSF = FSSSF;
  }
  public LSETCSSYSJJ with_FSSSF(String FSSSF) {
    this.FSSSF = FSSSF;
    return this;
  }
  private String FTGZH;
  public String get_FTGZH() {
    return FTGZH;
  }
  public void set_FTGZH(String FTGZH) {
    this.FTGZH = FTGZH;
  }
  public LSETCSSYSJJ with_FTGZH(String FTGZH) {
    this.FTGZH = FTGZH;
    return this;
  }
  private java.math.BigDecimal FJZCYTJ;
  public java.math.BigDecimal get_FJZCYTJ() {
    return FJZCYTJ;
  }
  public void set_FJZCYTJ(java.math.BigDecimal FJZCYTJ) {
    this.FJZCYTJ = FJZCYTJ;
  }
  public LSETCSSYSJJ with_FJZCYTJ(java.math.BigDecimal FJZCYTJ) {
    this.FJZCYTJ = FJZCYTJ;
    return this;
  }
  private java.math.BigDecimal HSTABB;
  public java.math.BigDecimal get_HSTABB() {
    return HSTABB;
  }
  public void set_HSTABB(java.math.BigDecimal HSTABB) {
    this.HSTABB = HSTABB;
  }
  public LSETCSSYSJJ with_HSTABB(java.math.BigDecimal HSTABB) {
    this.HSTABB = HSTABB;
    return this;
  }
  private String FTGRJC;
  public String get_FTGRJC() {
    return FTGRJC;
  }
  public void set_FTGRJC(String FTGRJC) {
    this.FTGRJC = FTGRJC;
  }
  public LSETCSSYSJJ with_FTGRJC(String FTGRJC) {
    this.FTGRJC = FTGRJC;
    return this;
  }
  private java.math.BigDecimal FSH;
  public java.math.BigDecimal get_FSH() {
    return FSH;
  }
  public void set_FSH(java.math.BigDecimal FSH) {
    this.FSH = FSH;
  }
  public LSETCSSYSJJ with_FSH(java.math.BigDecimal FSH) {
    this.FSH = FSH;
    return this;
  }
  private String FZZR;
  public String get_FZZR() {
    return FZZR;
  }
  public void set_FZZR(String FZZR) {
    this.FZZR = FZZR;
  }
  public LSETCSSYSJJ with_FZZR(String FZZR) {
    this.FZZR = FZZR;
    return this;
  }
  private String FCHK;
  public String get_FCHK() {
    return FCHK;
  }
  public void set_FCHK(String FCHK) {
    this.FCHK = FCHK;
  }
  public LSETCSSYSJJ with_FCHK(String FCHK) {
    this.FCHK = FCHK;
    return this;
  }
  private java.sql.Date FSTARTDATE;
  public java.sql.Date get_FSTARTDATE() {
    return FSTARTDATE;
  }
  public void set_FSTARTDATE(java.sql.Date FSTARTDATE) {
    this.FSTARTDATE = FSTARTDATE;
  }
  public LSETCSSYSJJ with_FSTARTDATE(java.sql.Date FSTARTDATE) {
    this.FSTARTDATE = FSTARTDATE;
    return this;
  }
  private String FKHH;
  public String get_FKHH() {
    return FKHH;
  }
  public void set_FKHH(String FKHH) {
    this.FKHH = FKHH;
  }
  public LSETCSSYSJJ with_FKHH(String FKHH) {
    this.FKHH = FKHH;
    return this;
  }
  private java.sql.Date FQSDATE;
  public java.sql.Date get_FQSDATE() {
    return FQSDATE;
  }
  public void set_FQSDATE(java.sql.Date FQSDATE) {
    this.FQSDATE = FQSDATE;
  }
  public LSETCSSYSJJ with_FQSDATE(java.sql.Date FQSDATE) {
    this.FQSDATE = FQSDATE;
    return this;
  }
  private java.math.BigDecimal FZCZT;
  public java.math.BigDecimal get_FZCZT() {
    return FZCZT;
  }
  public void set_FZCZT(java.math.BigDecimal FZCZT) {
    this.FZCZT = FZCZT;
  }
  public LSETCSSYSJJ with_FZCZT(java.math.BigDecimal FZCZT) {
    this.FZCZT = FZCZT;
    return this;
  }
  private String FPLANLICID;
  public String get_FPLANLICID() {
    return FPLANLICID;
  }
  public void set_FPLANLICID(String FPLANLICID) {
    this.FPLANLICID = FPLANLICID;
  }
  public LSETCSSYSJJ with_FPLANLICID(String FPLANLICID) {
    this.FPLANLICID = FPLANLICID;
    return this;
  }
  private String FYYJG;
  public String get_FYYJG() {
    return FYYJG;
  }
  public void set_FYYJG(String FYYJG) {
    this.FYYJG = FYYJG;
  }
  public LSETCSSYSJJ with_FYYJG(String FYYJG) {
    this.FYYJG = FYYJG;
    return this;
  }
  private java.math.BigDecimal FZYNJ;
  public java.math.BigDecimal get_FZYNJ() {
    return FZYNJ;
  }
  public void set_FZYNJ(java.math.BigDecimal FZYNJ) {
    this.FZYNJ = FZYNJ;
  }
  public LSETCSSYSJJ with_FZYNJ(java.math.BigDecimal FZYNJ) {
    this.FZYNJ = FZYNJ;
    return this;
  }
  private String FSTRJHBM;
  public String get_FSTRJHBM() {
    return FSTRJHBM;
  }
  public void set_FSTRJHBM(String FSTRJHBM) {
    this.FSTRJHBM = FSTRJHBM;
  }
  public LSETCSSYSJJ with_FSTRJHBM(String FSTRJHBM) {
    this.FSTRJHBM = FSTRJHBM;
    return this;
  }
  private String FDLRJHBM;
  public String get_FDLRJHBM() {
    return FDLRJHBM;
  }
  public void set_FDLRJHBM(String FDLRJHBM) {
    this.FDLRJHBM = FDLRJHBM;
  }
  public LSETCSSYSJJ with_FDLRJHBM(String FDLRJHBM) {
    this.FDLRJHBM = FDLRJHBM;
    return this;
  }
  private String FTZZHDM;
  public String get_FTZZHDM() {
    return FTZZHDM;
  }
  public void set_FTZZHDM(String FTZZHDM) {
    this.FTZZHDM = FTZZHDM;
  }
  public LSETCSSYSJJ with_FTZZHDM(String FTZZHDM) {
    this.FTZZHDM = FTZZHDM;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof LSETCSSYSJJ)) {
      return false;
    }
    LSETCSSYSJJ that = (LSETCSSYSJJ) o;
    boolean equal = true;
    equal = equal && (this.FSETCODE == null ? that.FSETCODE == null : this.FSETCODE.equals(that.FSETCODE));
    equal = equal && (this.FJJLX == null ? that.FJJLX == null : this.FJJLX.equals(that.FJJLX));
    equal = equal && (this.FJJZL == null ? that.FJJZL == null : this.FJJZL.equals(that.FJJZL));
    equal = equal && (this.FJJLB == null ? that.FJJLB == null : this.FJJLB.equals(that.FJJLB));
    equal = equal && (this.FJJWTREN == null ? that.FJJWTREN == null : this.FJJWTREN.equals(that.FJJWTREN));
    equal = equal && (this.FJJGLREN == null ? that.FJJGLREN == null : this.FJJGLREN.equals(that.FJJGLREN));
    equal = equal && (this.FJJTGREN == null ? that.FJJTGREN == null : this.FJJTGREN.equals(that.FJJTGREN));
    equal = equal && (this.FJJCLDATE == null ? that.FJJCLDATE == null : this.FJJCLDATE.equals(that.FJJCLDATE));
    equal = equal && (this.FJJDQDATE == null ? that.FJJDQDATE == null : this.FJJDQDATE.equals(that.FJJDQDATE));
    equal = equal && (this.FQCJZ == null ? that.FQCJZ == null : this.FQCJZ.equals(that.FQCJZ));
    equal = equal && (this.FPJE == null ? that.FPJE == null : this.FPJE.equals(that.FPJE));
    equal = equal && (this.FPJELJ == null ? that.FPJELJ == null : this.FPJELJ.equals(that.FPJELJ));
    equal = equal && (this.FZSBZZHXS == null ? that.FZSBZZHXS == null : this.FZSBZZHXS.equals(that.FZSBZZHXS));
    equal = equal && (this.FJJGLRFV == null ? that.FJJGLRFV == null : this.FJJGLRFV.equals(that.FJJGLRFV));
    equal = equal && (this.FJJTGRFV == null ? that.FJJTGRFV == null : this.FJJTGRFV.equals(that.FJJTGRFV));
    equal = equal && (this.FJJXSRFV == null ? that.FJJXSRFV == null : this.FJJXSRFV.equals(that.FJJXSRFV));
    equal = equal && (this.FJFRI == null ? that.FJFRI == null : this.FJFRI.equals(that.FJFRI));
    equal = equal && (this.FTAMS == null ? that.FTAMS == null : this.FTAMS.equals(that.FTAMS));
    equal = equal && (this.FZHGLR == null ? that.FZHGLR == null : this.FZHGLR.equals(that.FZHGLR));
    equal = equal && (this.FNJLX == null ? that.FNJLX == null : this.FNJLX.equals(that.FNJLX));
    equal = equal && (this.FSSSF == null ? that.FSSSF == null : this.FSSSF.equals(that.FSSSF));
    equal = equal && (this.FTGZH == null ? that.FTGZH == null : this.FTGZH.equals(that.FTGZH));
    equal = equal && (this.FJZCYTJ == null ? that.FJZCYTJ == null : this.FJZCYTJ.equals(that.FJZCYTJ));
    equal = equal && (this.HSTABB == null ? that.HSTABB == null : this.HSTABB.equals(that.HSTABB));
    equal = equal && (this.FTGRJC == null ? that.FTGRJC == null : this.FTGRJC.equals(that.FTGRJC));
    equal = equal && (this.FSH == null ? that.FSH == null : this.FSH.equals(that.FSH));
    equal = equal && (this.FZZR == null ? that.FZZR == null : this.FZZR.equals(that.FZZR));
    equal = equal && (this.FCHK == null ? that.FCHK == null : this.FCHK.equals(that.FCHK));
    equal = equal && (this.FSTARTDATE == null ? that.FSTARTDATE == null : this.FSTARTDATE.equals(that.FSTARTDATE));
    equal = equal && (this.FKHH == null ? that.FKHH == null : this.FKHH.equals(that.FKHH));
    equal = equal && (this.FQSDATE == null ? that.FQSDATE == null : this.FQSDATE.equals(that.FQSDATE));
    equal = equal && (this.FZCZT == null ? that.FZCZT == null : this.FZCZT.equals(that.FZCZT));
    equal = equal && (this.FPLANLICID == null ? that.FPLANLICID == null : this.FPLANLICID.equals(that.FPLANLICID));
    equal = equal && (this.FYYJG == null ? that.FYYJG == null : this.FYYJG.equals(that.FYYJG));
    equal = equal && (this.FZYNJ == null ? that.FZYNJ == null : this.FZYNJ.equals(that.FZYNJ));
    equal = equal && (this.FSTRJHBM == null ? that.FSTRJHBM == null : this.FSTRJHBM.equals(that.FSTRJHBM));
    equal = equal && (this.FDLRJHBM == null ? that.FDLRJHBM == null : this.FDLRJHBM.equals(that.FDLRJHBM));
    equal = equal && (this.FTZZHDM == null ? that.FTZZHDM == null : this.FTZZHDM.equals(that.FTZZHDM));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof LSETCSSYSJJ)) {
      return false;
    }
    LSETCSSYSJJ that = (LSETCSSYSJJ) o;
    boolean equal = true;
    equal = equal && (this.FSETCODE == null ? that.FSETCODE == null : this.FSETCODE.equals(that.FSETCODE));
    equal = equal && (this.FJJLX == null ? that.FJJLX == null : this.FJJLX.equals(that.FJJLX));
    equal = equal && (this.FJJZL == null ? that.FJJZL == null : this.FJJZL.equals(that.FJJZL));
    equal = equal && (this.FJJLB == null ? that.FJJLB == null : this.FJJLB.equals(that.FJJLB));
    equal = equal && (this.FJJWTREN == null ? that.FJJWTREN == null : this.FJJWTREN.equals(that.FJJWTREN));
    equal = equal && (this.FJJGLREN == null ? that.FJJGLREN == null : this.FJJGLREN.equals(that.FJJGLREN));
    equal = equal && (this.FJJTGREN == null ? that.FJJTGREN == null : this.FJJTGREN.equals(that.FJJTGREN));
    equal = equal && (this.FJJCLDATE == null ? that.FJJCLDATE == null : this.FJJCLDATE.equals(that.FJJCLDATE));
    equal = equal && (this.FJJDQDATE == null ? that.FJJDQDATE == null : this.FJJDQDATE.equals(that.FJJDQDATE));
    equal = equal && (this.FQCJZ == null ? that.FQCJZ == null : this.FQCJZ.equals(that.FQCJZ));
    equal = equal && (this.FPJE == null ? that.FPJE == null : this.FPJE.equals(that.FPJE));
    equal = equal && (this.FPJELJ == null ? that.FPJELJ == null : this.FPJELJ.equals(that.FPJELJ));
    equal = equal && (this.FZSBZZHXS == null ? that.FZSBZZHXS == null : this.FZSBZZHXS.equals(that.FZSBZZHXS));
    equal = equal && (this.FJJGLRFV == null ? that.FJJGLRFV == null : this.FJJGLRFV.equals(that.FJJGLRFV));
    equal = equal && (this.FJJTGRFV == null ? that.FJJTGRFV == null : this.FJJTGRFV.equals(that.FJJTGRFV));
    equal = equal && (this.FJJXSRFV == null ? that.FJJXSRFV == null : this.FJJXSRFV.equals(that.FJJXSRFV));
    equal = equal && (this.FJFRI == null ? that.FJFRI == null : this.FJFRI.equals(that.FJFRI));
    equal = equal && (this.FTAMS == null ? that.FTAMS == null : this.FTAMS.equals(that.FTAMS));
    equal = equal && (this.FZHGLR == null ? that.FZHGLR == null : this.FZHGLR.equals(that.FZHGLR));
    equal = equal && (this.FNJLX == null ? that.FNJLX == null : this.FNJLX.equals(that.FNJLX));
    equal = equal && (this.FSSSF == null ? that.FSSSF == null : this.FSSSF.equals(that.FSSSF));
    equal = equal && (this.FTGZH == null ? that.FTGZH == null : this.FTGZH.equals(that.FTGZH));
    equal = equal && (this.FJZCYTJ == null ? that.FJZCYTJ == null : this.FJZCYTJ.equals(that.FJZCYTJ));
    equal = equal && (this.HSTABB == null ? that.HSTABB == null : this.HSTABB.equals(that.HSTABB));
    equal = equal && (this.FTGRJC == null ? that.FTGRJC == null : this.FTGRJC.equals(that.FTGRJC));
    equal = equal && (this.FSH == null ? that.FSH == null : this.FSH.equals(that.FSH));
    equal = equal && (this.FZZR == null ? that.FZZR == null : this.FZZR.equals(that.FZZR));
    equal = equal && (this.FCHK == null ? that.FCHK == null : this.FCHK.equals(that.FCHK));
    equal = equal && (this.FSTARTDATE == null ? that.FSTARTDATE == null : this.FSTARTDATE.equals(that.FSTARTDATE));
    equal = equal && (this.FKHH == null ? that.FKHH == null : this.FKHH.equals(that.FKHH));
    equal = equal && (this.FQSDATE == null ? that.FQSDATE == null : this.FQSDATE.equals(that.FQSDATE));
    equal = equal && (this.FZCZT == null ? that.FZCZT == null : this.FZCZT.equals(that.FZCZT));
    equal = equal && (this.FPLANLICID == null ? that.FPLANLICID == null : this.FPLANLICID.equals(that.FPLANLICID));
    equal = equal && (this.FYYJG == null ? that.FYYJG == null : this.FYYJG.equals(that.FYYJG));
    equal = equal && (this.FZYNJ == null ? that.FZYNJ == null : this.FZYNJ.equals(that.FZYNJ));
    equal = equal && (this.FSTRJHBM == null ? that.FSTRJHBM == null : this.FSTRJHBM.equals(that.FSTRJHBM));
    equal = equal && (this.FDLRJHBM == null ? that.FDLRJHBM == null : this.FDLRJHBM.equals(that.FDLRJHBM));
    equal = equal && (this.FTZZHDM == null ? that.FTZZHDM == null : this.FTZZHDM.equals(that.FTZZHDM));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.FSETCODE = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.FJJLX = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.FJJZL = JdbcWritableBridge.readBigDecimal(3, __dbResults);
    this.FJJLB = JdbcWritableBridge.readBigDecimal(4, __dbResults);
    this.FJJWTREN = JdbcWritableBridge.readString(5, __dbResults);
    this.FJJGLREN = JdbcWritableBridge.readString(6, __dbResults);
    this.FJJTGREN = JdbcWritableBridge.readString(7, __dbResults);
    this.FJJCLDATE = JdbcWritableBridge.readDate(8, __dbResults);
    this.FJJDQDATE = JdbcWritableBridge.readDate(9, __dbResults);
    this.FQCJZ = JdbcWritableBridge.readBigDecimal(10, __dbResults);
    this.FPJE = JdbcWritableBridge.readBigDecimal(11, __dbResults);
    this.FPJELJ = JdbcWritableBridge.readBigDecimal(12, __dbResults);
    this.FZSBZZHXS = JdbcWritableBridge.readBigDecimal(13, __dbResults);
    this.FJJGLRFV = JdbcWritableBridge.readBigDecimal(14, __dbResults);
    this.FJJTGRFV = JdbcWritableBridge.readBigDecimal(15, __dbResults);
    this.FJJXSRFV = JdbcWritableBridge.readBigDecimal(16, __dbResults);
    this.FJFRI = JdbcWritableBridge.readBigDecimal(17, __dbResults);
    this.FTAMS = JdbcWritableBridge.readString(18, __dbResults);
    this.FZHGLR = JdbcWritableBridge.readString(19, __dbResults);
    this.FNJLX = JdbcWritableBridge.readString(20, __dbResults);
    this.FSSSF = JdbcWritableBridge.readString(21, __dbResults);
    this.FTGZH = JdbcWritableBridge.readString(22, __dbResults);
    this.FJZCYTJ = JdbcWritableBridge.readBigDecimal(23, __dbResults);
    this.HSTABB = JdbcWritableBridge.readBigDecimal(24, __dbResults);
    this.FTGRJC = JdbcWritableBridge.readString(25, __dbResults);
    this.FSH = JdbcWritableBridge.readBigDecimal(26, __dbResults);
    this.FZZR = JdbcWritableBridge.readString(27, __dbResults);
    this.FCHK = JdbcWritableBridge.readString(28, __dbResults);
    this.FSTARTDATE = JdbcWritableBridge.readDate(29, __dbResults);
    this.FKHH = JdbcWritableBridge.readString(30, __dbResults);
    this.FQSDATE = JdbcWritableBridge.readDate(31, __dbResults);
    this.FZCZT = JdbcWritableBridge.readBigDecimal(32, __dbResults);
    this.FPLANLICID = JdbcWritableBridge.readString(33, __dbResults);
    this.FYYJG = JdbcWritableBridge.readString(34, __dbResults);
    this.FZYNJ = JdbcWritableBridge.readBigDecimal(35, __dbResults);
    this.FSTRJHBM = JdbcWritableBridge.readString(36, __dbResults);
    this.FDLRJHBM = JdbcWritableBridge.readString(37, __dbResults);
    this.FTZZHDM = JdbcWritableBridge.readString(38, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.FSETCODE = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.FJJLX = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.FJJZL = JdbcWritableBridge.readBigDecimal(3, __dbResults);
    this.FJJLB = JdbcWritableBridge.readBigDecimal(4, __dbResults);
    this.FJJWTREN = JdbcWritableBridge.readString(5, __dbResults);
    this.FJJGLREN = JdbcWritableBridge.readString(6, __dbResults);
    this.FJJTGREN = JdbcWritableBridge.readString(7, __dbResults);
    this.FJJCLDATE = JdbcWritableBridge.readDate(8, __dbResults);
    this.FJJDQDATE = JdbcWritableBridge.readDate(9, __dbResults);
    this.FQCJZ = JdbcWritableBridge.readBigDecimal(10, __dbResults);
    this.FPJE = JdbcWritableBridge.readBigDecimal(11, __dbResults);
    this.FPJELJ = JdbcWritableBridge.readBigDecimal(12, __dbResults);
    this.FZSBZZHXS = JdbcWritableBridge.readBigDecimal(13, __dbResults);
    this.FJJGLRFV = JdbcWritableBridge.readBigDecimal(14, __dbResults);
    this.FJJTGRFV = JdbcWritableBridge.readBigDecimal(15, __dbResults);
    this.FJJXSRFV = JdbcWritableBridge.readBigDecimal(16, __dbResults);
    this.FJFRI = JdbcWritableBridge.readBigDecimal(17, __dbResults);
    this.FTAMS = JdbcWritableBridge.readString(18, __dbResults);
    this.FZHGLR = JdbcWritableBridge.readString(19, __dbResults);
    this.FNJLX = JdbcWritableBridge.readString(20, __dbResults);
    this.FSSSF = JdbcWritableBridge.readString(21, __dbResults);
    this.FTGZH = JdbcWritableBridge.readString(22, __dbResults);
    this.FJZCYTJ = JdbcWritableBridge.readBigDecimal(23, __dbResults);
    this.HSTABB = JdbcWritableBridge.readBigDecimal(24, __dbResults);
    this.FTGRJC = JdbcWritableBridge.readString(25, __dbResults);
    this.FSH = JdbcWritableBridge.readBigDecimal(26, __dbResults);
    this.FZZR = JdbcWritableBridge.readString(27, __dbResults);
    this.FCHK = JdbcWritableBridge.readString(28, __dbResults);
    this.FSTARTDATE = JdbcWritableBridge.readDate(29, __dbResults);
    this.FKHH = JdbcWritableBridge.readString(30, __dbResults);
    this.FQSDATE = JdbcWritableBridge.readDate(31, __dbResults);
    this.FZCZT = JdbcWritableBridge.readBigDecimal(32, __dbResults);
    this.FPLANLICID = JdbcWritableBridge.readString(33, __dbResults);
    this.FYYJG = JdbcWritableBridge.readString(34, __dbResults);
    this.FZYNJ = JdbcWritableBridge.readBigDecimal(35, __dbResults);
    this.FSTRJHBM = JdbcWritableBridge.readString(36, __dbResults);
    this.FDLRJHBM = JdbcWritableBridge.readString(37, __dbResults);
    this.FTZZHDM = JdbcWritableBridge.readString(38, __dbResults);
  }
  public void loadLargeObjects(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void loadLargeObjects0(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void write(PreparedStatement __dbStmt) throws SQLException {
    write(__dbStmt, 0);
  }

  public int write(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeBigDecimal(FSETCODE, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(FJJLX, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(FJJZL, 3 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(FJJLB, 4 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(FJJWTREN, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(FJJGLREN, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(FJJTGREN, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeDate(FJJCLDATE, 8 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeDate(FJJDQDATE, 9 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(FQCJZ, 10 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(FPJE, 11 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(FPJELJ, 12 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(FZSBZZHXS, 13 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(FJJGLRFV, 14 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(FJJTGRFV, 15 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(FJJXSRFV, 16 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(FJFRI, 17 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(FTAMS, 18 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(FZHGLR, 19 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(FNJLX, 20 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(FSSSF, 21 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(FTGZH, 22 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(FJZCYTJ, 23 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(HSTABB, 24 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(FTGRJC, 25 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(FSH, 26 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(FZZR, 27 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(FCHK, 28 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeDate(FSTARTDATE, 29 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeString(FKHH, 30 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeDate(FQSDATE, 31 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(FZCZT, 32 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(FPLANLICID, 33 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(FYYJG, 34 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(FZYNJ, 35 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(FSTRJHBM, 36 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(FDLRJHBM, 37 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(FTZZHDM, 38 + __off, 12, __dbStmt);
    return 38;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeBigDecimal(FSETCODE, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(FJJLX, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(FJJZL, 3 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(FJJLB, 4 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(FJJWTREN, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(FJJGLREN, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(FJJTGREN, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeDate(FJJCLDATE, 8 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeDate(FJJDQDATE, 9 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(FQCJZ, 10 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(FPJE, 11 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(FPJELJ, 12 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(FZSBZZHXS, 13 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(FJJGLRFV, 14 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(FJJTGRFV, 15 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(FJJXSRFV, 16 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(FJFRI, 17 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(FTAMS, 18 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(FZHGLR, 19 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(FNJLX, 20 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(FSSSF, 21 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(FTGZH, 22 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(FJZCYTJ, 23 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(HSTABB, 24 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(FTGRJC, 25 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(FSH, 26 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(FZZR, 27 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(FCHK, 28 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeDate(FSTARTDATE, 29 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeString(FKHH, 30 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeDate(FQSDATE, 31 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(FZCZT, 32 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(FPLANLICID, 33 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(FYYJG, 34 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(FZYNJ, 35 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(FSTRJHBM, 36 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(FDLRJHBM, 37 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(FTZZHDM, 38 + __off, 12, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.FSETCODE = null;
    } else {
    this.FSETCODE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.FJJLX = null;
    } else {
    this.FJJLX = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.FJJZL = null;
    } else {
    this.FJJZL = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.FJJLB = null;
    } else {
    this.FJJLB = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.FJJWTREN = null;
    } else {
    this.FJJWTREN = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.FJJGLREN = null;
    } else {
    this.FJJGLREN = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.FJJTGREN = null;
    } else {
    this.FJJTGREN = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.FJJCLDATE = null;
    } else {
    this.FJJCLDATE = new Date(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.FJJDQDATE = null;
    } else {
    this.FJJDQDATE = new Date(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.FQCJZ = null;
    } else {
    this.FQCJZ = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.FPJE = null;
    } else {
    this.FPJE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.FPJELJ = null;
    } else {
    this.FPJELJ = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.FZSBZZHXS = null;
    } else {
    this.FZSBZZHXS = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.FJJGLRFV = null;
    } else {
    this.FJJGLRFV = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.FJJTGRFV = null;
    } else {
    this.FJJTGRFV = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.FJJXSRFV = null;
    } else {
    this.FJJXSRFV = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.FJFRI = null;
    } else {
    this.FJFRI = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.FTAMS = null;
    } else {
    this.FTAMS = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.FZHGLR = null;
    } else {
    this.FZHGLR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.FNJLX = null;
    } else {
    this.FNJLX = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.FSSSF = null;
    } else {
    this.FSSSF = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.FTGZH = null;
    } else {
    this.FTGZH = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.FJZCYTJ = null;
    } else {
    this.FJZCYTJ = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.HSTABB = null;
    } else {
    this.HSTABB = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.FTGRJC = null;
    } else {
    this.FTGRJC = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.FSH = null;
    } else {
    this.FSH = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.FZZR = null;
    } else {
    this.FZZR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.FCHK = null;
    } else {
    this.FCHK = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.FSTARTDATE = null;
    } else {
    this.FSTARTDATE = new Date(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.FKHH = null;
    } else {
    this.FKHH = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.FQSDATE = null;
    } else {
    this.FQSDATE = new Date(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.FZCZT = null;
    } else {
    this.FZCZT = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.FPLANLICID = null;
    } else {
    this.FPLANLICID = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.FYYJG = null;
    } else {
    this.FYYJG = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.FZYNJ = null;
    } else {
    this.FZYNJ = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.FSTRJHBM = null;
    } else {
    this.FSTRJHBM = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.FDLRJHBM = null;
    } else {
    this.FDLRJHBM = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.FTZZHDM = null;
    } else {
    this.FTZZHDM = Text.readString(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.FSETCODE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.FSETCODE, __dataOut);
    }
    if (null == this.FJJLX) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.FJJLX, __dataOut);
    }
    if (null == this.FJJZL) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.FJJZL, __dataOut);
    }
    if (null == this.FJJLB) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.FJJLB, __dataOut);
    }
    if (null == this.FJJWTREN) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, FJJWTREN);
    }
    if (null == this.FJJGLREN) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, FJJGLREN);
    }
    if (null == this.FJJTGREN) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, FJJTGREN);
    }
    if (null == this.FJJCLDATE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.FJJCLDATE.getTime());
    }
    if (null == this.FJJDQDATE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.FJJDQDATE.getTime());
    }
    if (null == this.FQCJZ) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.FQCJZ, __dataOut);
    }
    if (null == this.FPJE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.FPJE, __dataOut);
    }
    if (null == this.FPJELJ) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.FPJELJ, __dataOut);
    }
    if (null == this.FZSBZZHXS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.FZSBZZHXS, __dataOut);
    }
    if (null == this.FJJGLRFV) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.FJJGLRFV, __dataOut);
    }
    if (null == this.FJJTGRFV) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.FJJTGRFV, __dataOut);
    }
    if (null == this.FJJXSRFV) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.FJJXSRFV, __dataOut);
    }
    if (null == this.FJFRI) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.FJFRI, __dataOut);
    }
    if (null == this.FTAMS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, FTAMS);
    }
    if (null == this.FZHGLR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, FZHGLR);
    }
    if (null == this.FNJLX) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, FNJLX);
    }
    if (null == this.FSSSF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, FSSSF);
    }
    if (null == this.FTGZH) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, FTGZH);
    }
    if (null == this.FJZCYTJ) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.FJZCYTJ, __dataOut);
    }
    if (null == this.HSTABB) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.HSTABB, __dataOut);
    }
    if (null == this.FTGRJC) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, FTGRJC);
    }
    if (null == this.FSH) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.FSH, __dataOut);
    }
    if (null == this.FZZR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, FZZR);
    }
    if (null == this.FCHK) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, FCHK);
    }
    if (null == this.FSTARTDATE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.FSTARTDATE.getTime());
    }
    if (null == this.FKHH) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, FKHH);
    }
    if (null == this.FQSDATE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.FQSDATE.getTime());
    }
    if (null == this.FZCZT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.FZCZT, __dataOut);
    }
    if (null == this.FPLANLICID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, FPLANLICID);
    }
    if (null == this.FYYJG) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, FYYJG);
    }
    if (null == this.FZYNJ) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.FZYNJ, __dataOut);
    }
    if (null == this.FSTRJHBM) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, FSTRJHBM);
    }
    if (null == this.FDLRJHBM) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, FDLRJHBM);
    }
    if (null == this.FTZZHDM) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, FTZZHDM);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.FSETCODE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.FSETCODE, __dataOut);
    }
    if (null == this.FJJLX) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.FJJLX, __dataOut);
    }
    if (null == this.FJJZL) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.FJJZL, __dataOut);
    }
    if (null == this.FJJLB) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.FJJLB, __dataOut);
    }
    if (null == this.FJJWTREN) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, FJJWTREN);
    }
    if (null == this.FJJGLREN) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, FJJGLREN);
    }
    if (null == this.FJJTGREN) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, FJJTGREN);
    }
    if (null == this.FJJCLDATE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.FJJCLDATE.getTime());
    }
    if (null == this.FJJDQDATE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.FJJDQDATE.getTime());
    }
    if (null == this.FQCJZ) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.FQCJZ, __dataOut);
    }
    if (null == this.FPJE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.FPJE, __dataOut);
    }
    if (null == this.FPJELJ) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.FPJELJ, __dataOut);
    }
    if (null == this.FZSBZZHXS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.FZSBZZHXS, __dataOut);
    }
    if (null == this.FJJGLRFV) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.FJJGLRFV, __dataOut);
    }
    if (null == this.FJJTGRFV) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.FJJTGRFV, __dataOut);
    }
    if (null == this.FJJXSRFV) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.FJJXSRFV, __dataOut);
    }
    if (null == this.FJFRI) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.FJFRI, __dataOut);
    }
    if (null == this.FTAMS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, FTAMS);
    }
    if (null == this.FZHGLR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, FZHGLR);
    }
    if (null == this.FNJLX) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, FNJLX);
    }
    if (null == this.FSSSF) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, FSSSF);
    }
    if (null == this.FTGZH) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, FTGZH);
    }
    if (null == this.FJZCYTJ) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.FJZCYTJ, __dataOut);
    }
    if (null == this.HSTABB) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.HSTABB, __dataOut);
    }
    if (null == this.FTGRJC) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, FTGRJC);
    }
    if (null == this.FSH) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.FSH, __dataOut);
    }
    if (null == this.FZZR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, FZZR);
    }
    if (null == this.FCHK) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, FCHK);
    }
    if (null == this.FSTARTDATE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.FSTARTDATE.getTime());
    }
    if (null == this.FKHH) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, FKHH);
    }
    if (null == this.FQSDATE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.FQSDATE.getTime());
    }
    if (null == this.FZCZT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.FZCZT, __dataOut);
    }
    if (null == this.FPLANLICID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, FPLANLICID);
    }
    if (null == this.FYYJG) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, FYYJG);
    }
    if (null == this.FZYNJ) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.FZYNJ, __dataOut);
    }
    if (null == this.FSTRJHBM) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, FSTRJHBM);
    }
    if (null == this.FDLRJHBM) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, FDLRJHBM);
    }
    if (null == this.FTZZHDM) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, FTZZHDM);
    }
  }
  private static final DelimiterSet __outputDelimiters = new DelimiterSet((char) 44, (char) 10, (char) 0, (char) 0, false);
  public String toString() {
    return toString(__outputDelimiters, true);
  }
  public String toString(DelimiterSet delimiters) {
    return toString(delimiters, true);
  }
  public String toString(boolean useRecordDelim) {
    return toString(__outputDelimiters, useRecordDelim);
  }
  public String toString(DelimiterSet delimiters, boolean useRecordDelim) {
    StringBuilder __sb = new StringBuilder();
    char fieldDelim = delimiters.getFieldsTerminatedBy();
    __sb.append(FieldFormatter.escapeAndEnclose(FSETCODE==null?"null":FSETCODE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FJJLX==null?"null":FJJLX.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FJJZL==null?"null":FJJZL.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FJJLB==null?"null":FJJLB.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FJJWTREN==null?"null":FJJWTREN, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FJJGLREN==null?"null":FJJGLREN, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FJJTGREN==null?"null":FJJTGREN, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FJJCLDATE==null?"null":"" + FJJCLDATE, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FJJDQDATE==null?"null":"" + FJJDQDATE, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FQCJZ==null?"null":FQCJZ.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FPJE==null?"null":FPJE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FPJELJ==null?"null":FPJELJ.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FZSBZZHXS==null?"null":FZSBZZHXS.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FJJGLRFV==null?"null":FJJGLRFV.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FJJTGRFV==null?"null":FJJTGRFV.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FJJXSRFV==null?"null":FJJXSRFV.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FJFRI==null?"null":FJFRI.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FTAMS==null?"null":FTAMS, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FZHGLR==null?"null":FZHGLR, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FNJLX==null?"null":FNJLX, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FSSSF==null?"null":FSSSF, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FTGZH==null?"null":FTGZH, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FJZCYTJ==null?"null":FJZCYTJ.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(HSTABB==null?"null":HSTABB.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FTGRJC==null?"null":FTGRJC, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FSH==null?"null":FSH.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FZZR==null?"null":FZZR, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FCHK==null?"null":FCHK, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FSTARTDATE==null?"null":"" + FSTARTDATE, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FKHH==null?"null":FKHH, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FQSDATE==null?"null":"" + FQSDATE, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FZCZT==null?"null":FZCZT.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FPLANLICID==null?"null":FPLANLICID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FYYJG==null?"null":FYYJG, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FZYNJ==null?"null":FZYNJ.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FSTRJHBM==null?"null":FSTRJHBM, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FDLRJHBM==null?"null":FDLRJHBM, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FTZZHDM==null?"null":FTZZHDM, delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(FSETCODE==null?"null":FSETCODE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FJJLX==null?"null":FJJLX.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FJJZL==null?"null":FJJZL.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FJJLB==null?"null":FJJLB.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FJJWTREN==null?"null":FJJWTREN, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FJJGLREN==null?"null":FJJGLREN, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FJJTGREN==null?"null":FJJTGREN, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FJJCLDATE==null?"null":"" + FJJCLDATE, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FJJDQDATE==null?"null":"" + FJJDQDATE, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FQCJZ==null?"null":FQCJZ.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FPJE==null?"null":FPJE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FPJELJ==null?"null":FPJELJ.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FZSBZZHXS==null?"null":FZSBZZHXS.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FJJGLRFV==null?"null":FJJGLRFV.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FJJTGRFV==null?"null":FJJTGRFV.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FJJXSRFV==null?"null":FJJXSRFV.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FJFRI==null?"null":FJFRI.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FTAMS==null?"null":FTAMS, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FZHGLR==null?"null":FZHGLR, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FNJLX==null?"null":FNJLX, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FSSSF==null?"null":FSSSF, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FTGZH==null?"null":FTGZH, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FJZCYTJ==null?"null":FJZCYTJ.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(HSTABB==null?"null":HSTABB.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FTGRJC==null?"null":FTGRJC, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FSH==null?"null":FSH.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FZZR==null?"null":FZZR, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FCHK==null?"null":FCHK, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FSTARTDATE==null?"null":"" + FSTARTDATE, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FKHH==null?"null":FKHH, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FQSDATE==null?"null":"" + FQSDATE, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FZCZT==null?"null":FZCZT.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FPLANLICID==null?"null":FPLANLICID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FYYJG==null?"null":FYYJG, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FZYNJ==null?"null":FZYNJ.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FSTRJHBM==null?"null":FSTRJHBM, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FDLRJHBM==null?"null":FDLRJHBM, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FTZZHDM==null?"null":FTZZHDM, delimiters));
  }
  private static final DelimiterSet __inputDelimiters = new DelimiterSet((char) 44, (char) 10, (char) 0, (char) 0, false);
  private RecordParser __parser;
  public void parse(Text __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharSequence __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(byte [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(char [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(ByteBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  private void __loadFromFields(List<String> fields) {
    Iterator<String> __it = fields.listIterator();
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.FSETCODE = null; } else {
      this.FSETCODE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.FJJLX = null; } else {
      this.FJJLX = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.FJJZL = null; } else {
      this.FJJZL = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.FJJLB = null; } else {
      this.FJJLB = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.FJJWTREN = null; } else {
      this.FJJWTREN = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.FJJGLREN = null; } else {
      this.FJJGLREN = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.FJJTGREN = null; } else {
      this.FJJTGREN = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.FJJCLDATE = null; } else {
      this.FJJCLDATE = java.sql.Date.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.FJJDQDATE = null; } else {
      this.FJJDQDATE = java.sql.Date.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.FQCJZ = null; } else {
      this.FQCJZ = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.FPJE = null; } else {
      this.FPJE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.FPJELJ = null; } else {
      this.FPJELJ = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.FZSBZZHXS = null; } else {
      this.FZSBZZHXS = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.FJJGLRFV = null; } else {
      this.FJJGLRFV = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.FJJTGRFV = null; } else {
      this.FJJTGRFV = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.FJJXSRFV = null; } else {
      this.FJJXSRFV = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.FJFRI = null; } else {
      this.FJFRI = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.FTAMS = null; } else {
      this.FTAMS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.FZHGLR = null; } else {
      this.FZHGLR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.FNJLX = null; } else {
      this.FNJLX = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.FSSSF = null; } else {
      this.FSSSF = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.FTGZH = null; } else {
      this.FTGZH = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.FJZCYTJ = null; } else {
      this.FJZCYTJ = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.HSTABB = null; } else {
      this.HSTABB = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.FTGRJC = null; } else {
      this.FTGRJC = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.FSH = null; } else {
      this.FSH = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.FZZR = null; } else {
      this.FZZR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.FCHK = null; } else {
      this.FCHK = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.FSTARTDATE = null; } else {
      this.FSTARTDATE = java.sql.Date.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.FKHH = null; } else {
      this.FKHH = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.FQSDATE = null; } else {
      this.FQSDATE = java.sql.Date.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.FZCZT = null; } else {
      this.FZCZT = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.FPLANLICID = null; } else {
      this.FPLANLICID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.FYYJG = null; } else {
      this.FYYJG = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.FZYNJ = null; } else {
      this.FZYNJ = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.FSTRJHBM = null; } else {
      this.FSTRJHBM = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.FDLRJHBM = null; } else {
      this.FDLRJHBM = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.FTZZHDM = null; } else {
      this.FTZZHDM = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.FSETCODE = null; } else {
      this.FSETCODE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.FJJLX = null; } else {
      this.FJJLX = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.FJJZL = null; } else {
      this.FJJZL = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.FJJLB = null; } else {
      this.FJJLB = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.FJJWTREN = null; } else {
      this.FJJWTREN = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.FJJGLREN = null; } else {
      this.FJJGLREN = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.FJJTGREN = null; } else {
      this.FJJTGREN = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.FJJCLDATE = null; } else {
      this.FJJCLDATE = java.sql.Date.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.FJJDQDATE = null; } else {
      this.FJJDQDATE = java.sql.Date.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.FQCJZ = null; } else {
      this.FQCJZ = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.FPJE = null; } else {
      this.FPJE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.FPJELJ = null; } else {
      this.FPJELJ = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.FZSBZZHXS = null; } else {
      this.FZSBZZHXS = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.FJJGLRFV = null; } else {
      this.FJJGLRFV = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.FJJTGRFV = null; } else {
      this.FJJTGRFV = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.FJJXSRFV = null; } else {
      this.FJJXSRFV = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.FJFRI = null; } else {
      this.FJFRI = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.FTAMS = null; } else {
      this.FTAMS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.FZHGLR = null; } else {
      this.FZHGLR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.FNJLX = null; } else {
      this.FNJLX = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.FSSSF = null; } else {
      this.FSSSF = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.FTGZH = null; } else {
      this.FTGZH = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.FJZCYTJ = null; } else {
      this.FJZCYTJ = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.HSTABB = null; } else {
      this.HSTABB = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.FTGRJC = null; } else {
      this.FTGRJC = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.FSH = null; } else {
      this.FSH = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.FZZR = null; } else {
      this.FZZR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.FCHK = null; } else {
      this.FCHK = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.FSTARTDATE = null; } else {
      this.FSTARTDATE = java.sql.Date.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.FKHH = null; } else {
      this.FKHH = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.FQSDATE = null; } else {
      this.FQSDATE = java.sql.Date.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.FZCZT = null; } else {
      this.FZCZT = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.FPLANLICID = null; } else {
      this.FPLANLICID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.FYYJG = null; } else {
      this.FYYJG = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.FZYNJ = null; } else {
      this.FZYNJ = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.FSTRJHBM = null; } else {
      this.FSTRJHBM = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.FDLRJHBM = null; } else {
      this.FDLRJHBM = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.FTZZHDM = null; } else {
      this.FTZZHDM = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    LSETCSSYSJJ o = (LSETCSSYSJJ) super.clone();
    o.FJJCLDATE = (o.FJJCLDATE != null) ? (java.sql.Date) o.FJJCLDATE.clone() : null;
    o.FJJDQDATE = (o.FJJDQDATE != null) ? (java.sql.Date) o.FJJDQDATE.clone() : null;
    o.FSTARTDATE = (o.FSTARTDATE != null) ? (java.sql.Date) o.FSTARTDATE.clone() : null;
    o.FQSDATE = (o.FQSDATE != null) ? (java.sql.Date) o.FQSDATE.clone() : null;
    return o;
  }

  public void clone0(LSETCSSYSJJ o) throws CloneNotSupportedException {
    o.FJJCLDATE = (o.FJJCLDATE != null) ? (java.sql.Date) o.FJJCLDATE.clone() : null;
    o.FJJDQDATE = (o.FJJDQDATE != null) ? (java.sql.Date) o.FJJDQDATE.clone() : null;
    o.FSTARTDATE = (o.FSTARTDATE != null) ? (java.sql.Date) o.FSTARTDATE.clone() : null;
    o.FQSDATE = (o.FQSDATE != null) ? (java.sql.Date) o.FQSDATE.clone() : null;
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("FSETCODE", this.FSETCODE);
    __sqoop$field_map.put("FJJLX", this.FJJLX);
    __sqoop$field_map.put("FJJZL", this.FJJZL);
    __sqoop$field_map.put("FJJLB", this.FJJLB);
    __sqoop$field_map.put("FJJWTREN", this.FJJWTREN);
    __sqoop$field_map.put("FJJGLREN", this.FJJGLREN);
    __sqoop$field_map.put("FJJTGREN", this.FJJTGREN);
    __sqoop$field_map.put("FJJCLDATE", this.FJJCLDATE);
    __sqoop$field_map.put("FJJDQDATE", this.FJJDQDATE);
    __sqoop$field_map.put("FQCJZ", this.FQCJZ);
    __sqoop$field_map.put("FPJE", this.FPJE);
    __sqoop$field_map.put("FPJELJ", this.FPJELJ);
    __sqoop$field_map.put("FZSBZZHXS", this.FZSBZZHXS);
    __sqoop$field_map.put("FJJGLRFV", this.FJJGLRFV);
    __sqoop$field_map.put("FJJTGRFV", this.FJJTGRFV);
    __sqoop$field_map.put("FJJXSRFV", this.FJJXSRFV);
    __sqoop$field_map.put("FJFRI", this.FJFRI);
    __sqoop$field_map.put("FTAMS", this.FTAMS);
    __sqoop$field_map.put("FZHGLR", this.FZHGLR);
    __sqoop$field_map.put("FNJLX", this.FNJLX);
    __sqoop$field_map.put("FSSSF", this.FSSSF);
    __sqoop$field_map.put("FTGZH", this.FTGZH);
    __sqoop$field_map.put("FJZCYTJ", this.FJZCYTJ);
    __sqoop$field_map.put("HSTABB", this.HSTABB);
    __sqoop$field_map.put("FTGRJC", this.FTGRJC);
    __sqoop$field_map.put("FSH", this.FSH);
    __sqoop$field_map.put("FZZR", this.FZZR);
    __sqoop$field_map.put("FCHK", this.FCHK);
    __sqoop$field_map.put("FSTARTDATE", this.FSTARTDATE);
    __sqoop$field_map.put("FKHH", this.FKHH);
    __sqoop$field_map.put("FQSDATE", this.FQSDATE);
    __sqoop$field_map.put("FZCZT", this.FZCZT);
    __sqoop$field_map.put("FPLANLICID", this.FPLANLICID);
    __sqoop$field_map.put("FYYJG", this.FYYJG);
    __sqoop$field_map.put("FZYNJ", this.FZYNJ);
    __sqoop$field_map.put("FSTRJHBM", this.FSTRJHBM);
    __sqoop$field_map.put("FDLRJHBM", this.FDLRJHBM);
    __sqoop$field_map.put("FTZZHDM", this.FTZZHDM);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("FSETCODE", this.FSETCODE);
    __sqoop$field_map.put("FJJLX", this.FJJLX);
    __sqoop$field_map.put("FJJZL", this.FJJZL);
    __sqoop$field_map.put("FJJLB", this.FJJLB);
    __sqoop$field_map.put("FJJWTREN", this.FJJWTREN);
    __sqoop$field_map.put("FJJGLREN", this.FJJGLREN);
    __sqoop$field_map.put("FJJTGREN", this.FJJTGREN);
    __sqoop$field_map.put("FJJCLDATE", this.FJJCLDATE);
    __sqoop$field_map.put("FJJDQDATE", this.FJJDQDATE);
    __sqoop$field_map.put("FQCJZ", this.FQCJZ);
    __sqoop$field_map.put("FPJE", this.FPJE);
    __sqoop$field_map.put("FPJELJ", this.FPJELJ);
    __sqoop$field_map.put("FZSBZZHXS", this.FZSBZZHXS);
    __sqoop$field_map.put("FJJGLRFV", this.FJJGLRFV);
    __sqoop$field_map.put("FJJTGRFV", this.FJJTGRFV);
    __sqoop$field_map.put("FJJXSRFV", this.FJJXSRFV);
    __sqoop$field_map.put("FJFRI", this.FJFRI);
    __sqoop$field_map.put("FTAMS", this.FTAMS);
    __sqoop$field_map.put("FZHGLR", this.FZHGLR);
    __sqoop$field_map.put("FNJLX", this.FNJLX);
    __sqoop$field_map.put("FSSSF", this.FSSSF);
    __sqoop$field_map.put("FTGZH", this.FTGZH);
    __sqoop$field_map.put("FJZCYTJ", this.FJZCYTJ);
    __sqoop$field_map.put("HSTABB", this.HSTABB);
    __sqoop$field_map.put("FTGRJC", this.FTGRJC);
    __sqoop$field_map.put("FSH", this.FSH);
    __sqoop$field_map.put("FZZR", this.FZZR);
    __sqoop$field_map.put("FCHK", this.FCHK);
    __sqoop$field_map.put("FSTARTDATE", this.FSTARTDATE);
    __sqoop$field_map.put("FKHH", this.FKHH);
    __sqoop$field_map.put("FQSDATE", this.FQSDATE);
    __sqoop$field_map.put("FZCZT", this.FZCZT);
    __sqoop$field_map.put("FPLANLICID", this.FPLANLICID);
    __sqoop$field_map.put("FYYJG", this.FYYJG);
    __sqoop$field_map.put("FZYNJ", this.FZYNJ);
    __sqoop$field_map.put("FSTRJHBM", this.FSTRJHBM);
    __sqoop$field_map.put("FDLRJHBM", this.FDLRJHBM);
    __sqoop$field_map.put("FTZZHDM", this.FTZZHDM);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
