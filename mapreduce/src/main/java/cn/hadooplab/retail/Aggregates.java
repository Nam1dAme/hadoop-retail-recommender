package cn.hadooplab.retail;

import java.io.IOException;
import java.time.LocalDate;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/** Streaming grouping and checked integer aggregation for J02-J06. */
public final class Aggregates {
    private Aggregates() {}
    static boolean sale(String[] f) { return f[13].equals("SALE"); }
    public static class Rows extends Mapper<LongWritable,Text,Text,Text> {
        protected void map(LongWritable k, Text v, Context c) throws IOException, InterruptedException {
            String stage = c.getConfiguration().get("retail.stage");
            String[] f;
            switch (stage) {
            case "J02":
                f=Tsv.parse(v.toString(),15);
                if (sale(f)) c.write(new Text(Tsv.encode(f[2])),new Text(Tsv.line(f[8],f[12],f[11],f[10])));
                break;
            case "J03":
                f=Tsv.parse(v.toString(),15);
                c.write(new Text(Tsv.line(f[8],f[12],f[3])), new Text(sale(f) ? Tsv.line(f[10],0,f[6],0) : Tsv.line(0,Math.negateExact(Tsv.number(f[10])),0,f[6])));
                break;
            case "J04":
                f=Tsv.parse(v.toString(),15);
                if (sale(f)) c.write(new Text(Tsv.line(f[2],f[3])),new Text(Tsv.line(f[8],f[12],f[10],f[5])));
                break;
            case "J05base":
                f=Tsv.parse(v.toString(),5);
                if (f[3]!=null) c.write(new Text(Tsv.encode(f[3])),new Text(Tsv.line(f[1],f[4])));
                break;
            case "J06interactions":
                f=Tsv.parse(v.toString(),15);
                if (sale(f) && f[11]!=null && f[14].equals("true"))
                    c.write(new Text(Tsv.line(f[11],f[3],f[2])),new Text(Tsv.line(f[2],f[8])));
                break;
            default: throw new IOException("Unknown aggregate stage");
            }
        }
    }
    public static class Groups extends Reducer<Text,Text,Text,NullWritable> {
        MultipleOutputs<Text,NullWritable> out;
        protected void setup(Context c) { out=new MultipleOutputs<>(c); }
        void emit(String dataset,String value) throws IOException,InterruptedException { out.write("rows",new Text(value),NullWritable.get(),dataset+"/part"); }
        protected void reduce(Text key, Iterable<Text> values, Context c) throws IOException, InterruptedException {
            String stage=c.getConfiguration().get("retail.stage");
            String[] identity=Tsv.parse(key.toString(),-1);
            String day=null,country=null,customer=null,last=null,invoice=null;
            long amount=0,quantity=0,count=0;
            long[] totals=new long[4];
            for (Text v:values) {
                String[] f=Tsv.parse(v.toString(),-1);
                switch(stage) {
                case "J02":
                    if (day!=null && (!day.equals(f[0]) || !country.equals(f[1]))) throw new IOException("Order identity conflict");
                    if (customer!=null && f[2]!=null && !customer.equals(f[2])) throw new IOException("Order customer conflict");
                    day=f[0];country=f[1];if(f[2]!=null)customer=f[2];amount=Tsv.add(amount,Tsv.number(f[3]));break;
                case "J03":
                    for(int i=0;i<4;i++) totals[i]=Tsv.add(totals[i],Tsv.number(f[i]));break;
                case "J04":
                    if(day!=null && (!day.equals(f[0]) || !country.equals(f[1])))throw new IOException("Product order identity conflict");
                    day=f[0];country=f[1];amount=Tsv.add(amount,Tsv.number(f[2]));quantity=Tsv.add(quantity,Tsv.number(f[3]));break;
                case "J05base":
                    if(last==null || f[0].compareTo(last)>0)last=f[0];
                    count=Tsv.add(count,1);amount=Tsv.add(amount,Tsv.number(f[1]));break;
                case "J06interactions":
                    if(!f[0].equals(invoice)){count=Tsv.add(count,1);invoice=f[0];}
                    if(last==null || f[1].compareTo(last)>0)last=f[1];break;
                default:throw new IOException("Unknown aggregate reducer");
                }
            }
            switch(stage) {
            case "J02": emit("order_fact",Tsv.line(identity[0],day,country,customer,amount));break;
            case "J03": emit("sales_cube",key+"\t"+Tsv.line(totals[0],totals[1],totals[2],totals[3]));break;
            case "J04": emit("product_order_fact",key+"\t"+Tsv.line(day,country,amount,quantity));break;
            case "J05base": emit("rfm_base",Tsv.line(identity[0],last,count,amount));break;
            case "J06interactions":
                emit("interactions",Tsv.line(identity[0],identity[1],count,last));
                emit("customer_seen",Tsv.line(identity[0],identity[1]));break;
            default:throw new IOException("Unknown aggregate output");
            }
        }
        protected void cleanup(Context c)throws IOException,InterruptedException{out.close();}
    }
    /** Group by first n tokens, while sorting the complete composite key. */
    public static class PrefixPartition extends Partitioner<Text,Text> {
        public int getPartition(Text key,Text value,int n){return (prefix(key.toString(),2).hashCode()&Integer.MAX_VALUE)%n;}
    }
    public static class PrefixGroup extends WritableComparator {
        public PrefixGroup(){super(Text.class,true);}
        @Override public int compare(WritableComparable a,WritableComparable b){return Tsv.bytes(prefix(a.toString(),2),prefix(b.toString(),2));}
    }
    static String prefix(String s,int n){int p=-1;for(int i=0;i<n;i++){p=s.indexOf('\t',p+1);if(p<0)return s;}return s.substring(0,p);}
}
