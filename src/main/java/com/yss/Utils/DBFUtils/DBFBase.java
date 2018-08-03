package main.java.com.yss.Utils.DBFUtils;

/**
 * @author yupan
 * 2018/7/31 16:29
 **/
public abstract  class DBFBase {
    protected String characterSetName = "8859_1";
    protected final int END_OF_DATA = 0x1A;

    /*
     If the library is used in a non-latin environment use this method to set
     corresponding character set. More information:
     http://www.iana.org/assignments/character-sets
     Also see the documentation of the class java.nio.charset.Charset
    */
    public String getCharactersetName() {

        return this.characterSetName;
    }

    public void setCharactersetName( String characterSetName) {

        this.characterSetName = characterSetName;
    }
}
