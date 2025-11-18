package csx55.dfs.util;

public class Converter {
    private static Converter converter;

    private Converter() {

    }

    public static Converter getConverter() {
        if(converter == null){
            converter = new Converter();
        }
        return converter;
    }

    /**
     * This method converts a set of bytes into a Hexadecimal representation.
     */
    public String convertBytesToHex(byte[] buf) {
        StringBuffer strBuf = new StringBuffer();
        for (int i = 0; i < buf.length; i++) {
            int byteValue = (int) buf[i] & 0xff;
            if (byteValue <= 15) {
                strBuf.append("0");
            }
            strBuf.append(Integer.toString(byteValue, 16));
        }
        return strBuf.toString();
    }
    /**
     * This method converts a specified hexadecimal String into a set of bytes.
     */
    public byte[] convertHexToBytes(String hexString) {
        int size = hexString.length();
        byte[] buf = new byte[size / 2];
        int j = 0;
        for (int i = 0; i < size; i++) {
            String a = hexString.substring(i, i + 2);
            int valA = Integer.parseInt(a, 16);
            i++;
            buf[j] = (byte) valA;
            j++;
        }
        return buf;
    }
}

