package cn.hadooplab.retail;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.LocalDateTime;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/** J01 joins source warnings by row_id and checks uniqueness before classification. */
public final class Clean {
    private Clean() {}
    static final List<String> REASONS = Arrays.asList("PARSE_ERROR", "MISSING_INVOICE", "INVALID_INVOICE_LENGTH",
        "MISSING_STOCK_CODE", "INVALID_STOCK_LENGTH", "INVALID_QUANTITY", "ZERO_QUANTITY", "INVALID_DATETIME",
        "INVALID_PRICE", "NONPOSITIVE_PRICE", "INVALID_COUNTRY_LENGTH", "AMOUNT_OVERFLOW");
    static final Set<String> WARNINGS = new HashSet<>(Arrays.asList("MISSING_CUSTOMER_ID", "INVALID_CUSTOMER_ID",
        "MISSING_DESCRIPTION", "DESCRIPTION_TRUNCATED", "PRICE_ROUNDED", "CANCEL_POSITIVE_QUANTITY", "NEGATIVE_WITHOUT_CANCEL"));
    static final class Row {
        String reason, recordClass;
        String[] f;
        long quantity, price, amount, absolute;
        Row(String[] fields) throws IOException {
            f = fields;
            Set<String> errors = new HashSet<>();
            if (f[10] != null) errors.add("PARSE_ERROR");
            if (f[2] == null || f[2].isEmpty()) errors.add("MISSING_INVOICE");
            else if (f[2].codePointCount(0, f[2].length()) > 32) errors.add("INVALID_INVOICE_LENGTH");
            if (f[3] == null || f[3].isEmpty()) errors.add("MISSING_STOCK_CODE");
            else if (f[3].codePointCount(0, f[3].length()) > 32) errors.add("INVALID_STOCK_LENGTH");
            try { quantity = Tsv.number(f[5]); if (quantity == 0) errors.add("ZERO_QUANTITY"); }
            catch (IOException e) { errors.add("INVALID_QUANTITY"); }
            try { LocalDateTime.parse(f[6]); }
            catch (RuntimeException e) { errors.add("INVALID_DATETIME"); }
            try { price = Tsv.number(f[7]); if (price <= 0) errors.add("NONPOSITIVE_PRICE"); }
            catch (IOException e) { errors.add("INVALID_PRICE"); }
            if (f[9] == null || f[9].codePointCount(0, f[9].length()) > 64) errors.add("INVALID_COUNTRY_LENGTH");
            recordClass = (f[2] != null && f[2].startsWith("C")) || quantity < 0 ? "RETURN" : "SALE";
            try {
                if (quantity == Long.MIN_VALUE) throw new ArithmeticException();
                absolute = Math.abs(quantity);
                amount = Math.multiplyExact(recordClass.equals("RETURN") ? -absolute : quantity, price);
            } catch (ArithmeticException e) { errors.add("AMOUNT_OVERFLOW"); }
            for (String r : REASONS) if (errors.contains(r)) { reason = r; break; }
            if (reason != null) recordClass = "REJECTED";
        }
        String output() {
            return Tsv.line(f[0], f[1], f[2], f[3], f[4], quantity, absolute, f[6], f[6].substring(0,10),
                price, amount, f[8], f[9], recordClass, merchandise(f[3]) ? "true" : "false");
        }
    }
    static boolean merchandise(String stock) { return stock != null && stock.matches("[0-9]{5,6}[A-Z]{0,2}"); }
    public static class SourceMapper extends Mapper<LongWritable,Text,Text,Text> {
        protected void map(LongWritable key, Text value, Context c) throws IOException, InterruptedException {
            String[] f = Tsv.parse(value.toString(), 11);
            long row = Tsv.number(f[1]);
            if (row < 2 || f[0] == null || !f[0].matches("[a-f0-9]{64}")) throw new IOException("Invalid source identity");
            try {
                byte[] bytes = MessageDigest.getInstance("SHA-256").digest((c.getConfiguration().get("retail.source.sha")+":"+row).getBytes(StandardCharsets.UTF_8));
                StringBuilder hash = new StringBuilder();
                for (byte b : bytes) hash.append(String.format(Locale.ROOT,"%02x",b & 255));
                if (!hash.toString().equals(f[0])) throw new IOException("Cross-source row identity");
            } catch (java.security.NoSuchAlgorithmException e) { throw new IOException(e); }
            c.write(new Text(f[0]), new Text("R\t"+value));
        }
    }
    public static class WarningMapper extends Mapper<LongWritable,Text,Text,Text> {
        protected void map(LongWritable key, Text value, Context c) throws IOException, InterruptedException {
            String[] f = Tsv.parse(value.toString(), 2);
            if (!WARNINGS.contains(f[1])) throw new IOException("Unknown source warning");
            c.write(new Text(f[0]), new Text("W\t"+f[1]));
        }
    }
    public static class Rows extends Reducer<Text,Text,Text,NullWritable> {
        MultipleOutputs<Text,NullWritable> out;
        protected void setup(Context c) { out = new MultipleOutputs<>(c); }
        protected void reduce(Text key, Iterable<Text> values, Context c) throws IOException, InterruptedException {
            String source = null;
            Set<String> warnings = new TreeSet<>();
            for (Text value : values) {
                String s = value.toString();
                if (s.startsWith("R\t")) {
                    if (source != null) throw new IOException("Duplicate row_id");
                    source = s.substring(2);
                } else if (!warnings.add(s.substring(2))) throw new IOException("Duplicate source warning");
            }
            if (source == null) throw new IOException("Warning without source row");
            Row row = new Row(Tsv.parse(source, 11));
            c.getCounter("RETAIL", "RAW_ROWS").increment(1);
            c.getCounter("RETAIL", row.recordClass+"_ROWS").increment(1);
            if (row.reason != null) {
                emit("rejected", Tsv.line(row.f[0],row.f[1],row.reason));
                c.getCounter("RETAIL", row.reason+"_ROWS").increment(1);
            } else {
                emit("clean", row.output());
                if (row.recordClass.equals("SALE") && !merchandise(row.f[3]))
                    c.getCounter("RETAIL", "NON_MERCHANDISE_SALE_ROWS").increment(1);
                if (row.f[2].startsWith("C") && row.quantity > 0) warnings.add("CANCEL_POSITIVE_QUANTITY");
                if (!row.f[2].startsWith("C") && row.quantity < 0) warnings.add("NEGATIVE_WITHOUT_CANCEL");
            }
            for (String warning : warnings) {
                emit("warnings", Tsv.line(key.toString(),warning));
                c.getCounter("RETAIL", warning+"_ROWS").increment(1);
            }
        }
        void emit(String dataset, String line) throws IOException, InterruptedException {
            out.write("rows", new Text(line), NullWritable.get(), dataset+"/part");
        }
        protected void cleanup(Context c) throws IOException, InterruptedException { out.close(); }
    }
}
