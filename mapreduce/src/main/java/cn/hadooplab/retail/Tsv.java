package cn.hadooplab.retail;

import java.io.IOException;
import java.util.*;

/** Frozen UTF-8 TSV codec; encoded tokens are also safe shuffle keys. */
public final class Tsv {
    private Tsv() {}
    public static String encode(Object value) {
        if (value == null) return "\\N";
        String s = value.toString();
        if (s.indexOf('\t') >= 0 || s.indexOf('\r') >= 0 || s.indexOf('\n') >= 0 || s.indexOf(0) >= 0)
            throw new IllegalArgumentException("Control character in TSV field");
        return s.replace("\\", "\\\\");
    }
    public static String decode(String s) throws IOException {
        if (s.equals("\\N")) return null;
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\') {
                if (++i >= s.length() || s.charAt(i) != '\\') throw new IOException("Invalid TSV escape");
            }
            if (c == 0 || c == '\t' || c == '\r' || c == '\n') throw new IOException("Invalid TSV control");
            b.append(s.charAt(i));
        }
        return b.toString();
    }
    public static String[] parse(String line, int count) throws IOException {
        String[] a = line.split("\t", -1);
        if (count >= 0 && a.length != count) throw new IOException("Invalid TSV column count");
        for (int i = 0; i < a.length; i++) a[i] = decode(a[i]);
        return a;
    }
    public static String line(Object... values) {
        StringJoiner j = new StringJoiner("\t");
        for (Object v : values) j.add(encode(v));
        return j.toString();
    }
    public static long number(String s) throws IOException {
        try { return Long.parseLong(s); }
        catch (RuntimeException e) { throw new IOException("Invalid int64", e); }
    }
    public static long add(long a, long b) throws IOException {
        try { return Math.addExact(a, b); }
        catch (ArithmeticException e) { throw new IOException("Aggregate int64 overflow", e); }
    }
    public static int bytes(String a, String b) {
        return new org.apache.hadoop.io.Text(a).compareTo(new org.apache.hadoop.io.Text(b));
    }
}
