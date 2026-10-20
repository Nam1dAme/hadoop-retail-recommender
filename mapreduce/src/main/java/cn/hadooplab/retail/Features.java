package cn.hadooplab.retail;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/** RFM frequency scoring, bounded baskets and partitioned item similarity. */
public final class Features {
    private Features() {}
    public static class FirstPartition extends Partitioner<Text,Text> {
        public int getPartition(Text key,Text value,int n){return (Aggregates.prefix(key.toString(),1).hashCode()&Integer.MAX_VALUE)%n;}
    }
    public static class FirstGroup extends WritableComparator {
        public FirstGroup(){super(Text.class,true);}
        @Override public int compare(WritableComparable a,WritableComparable b){return Tsv.bytes(Aggregates.prefix(a.toString(),1),Aggregates.prefix(b.toString(),1));}
    }
    public static class Output extends Reducer<Text,Text,Text,NullWritable> {
        protected MultipleOutputs<Text,NullWritable> out;
        protected void setup(Context c){out=new MultipleOutputs<>(c);}
        void emit(String dataset,String line)throws IOException,InterruptedException{out.write("rows",new Text(line),NullWritable.get(),dataset+"/part");}
        protected void cleanup(Context c)throws IOException,InterruptedException{out.close();}
    }
    public static class DistributionMap extends Mapper<LongWritable,Text,Text,Text> {
        protected void map(LongWritable k,Text v,Context c)throws IOException,InterruptedException{
            String[] f=Tsv.parse(v.toString(),4);
            long[] values={LocalDate.parse(f[1]).toEpochDay(),Tsv.number(f[2]),Tsv.number(f[3])};
            String[] metrics={"R","F","M"};
            for(int i=0;i<3;i++)c.write(new Text(metrics[i]+"\t"+String.format(Locale.ROOT,"%016x",values[i]^Long.MIN_VALUE)),new Text("1"));
        }
    }
    public static class Distribution extends Output {
        String previous="";long less=0;
        protected void reduce(Text key,Iterable<Text> values,Context c)throws IOException,InterruptedException{
            String[] tokens=key.toString().split("\t");
            if(!tokens[0].equals(previous)){previous=tokens[0];less=0;}
            long count=0;for(Text v:values)count=Tsv.add(count,Tsv.number(v.toString()));
            long n=c.getConfiguration().getLong("retail.customers",0);
            if(n<=0 || less+count>n)throw new IOException("Invalid RFM population");
            long value=Long.parseUnsignedLong(tokens[1],16)^Long.MIN_VALUE;
            long smaller=tokens[0].equals("R")?n-less-count:less;
            int score=(int)Math.min(5,1+Math.multiplyExact(5,smaller)/n);
            if(tokens[0].equals("R"))score=6-score;
            emit("rfm_scores",Tsv.line(tokens[0],value,score));
            less=Tsv.add(less,count);
        }
    }
    static List<String[]> lookup(org.apache.hadoop.conf.Configuration conf,int columns)throws IOException{
        List<String[]> rows=new ArrayList<>();
        long bytes=0;
        for(int i=0;i<conf.getInt("retail.cache.count",0);i++){
            java.nio.file.Path p=Paths.get("lookup"+i);
            bytes+=Files.size(p);
            if(bytes>32L*1024*1024)throw new IOException("Broadcast lookup exceeds 32 MiB; repartition join required");
            try(BufferedReader in=Files.newBufferedReader(p,StandardCharsets.UTF_8)){
                String line;while((line=in.readLine())!=null)rows.add(Tsv.parse(line,columns));
            }
        }
        return rows;
    }
    public static class Score extends Mapper<LongWritable,Text,Text,NullWritable>{
        Map<String,Integer> scores=new HashMap<>();LocalDate reference;
        MultipleOutputs<Text,NullWritable> out;
        protected void setup(Context c)throws IOException{
            out=new MultipleOutputs<>(c);
            long max=Long.MIN_VALUE;
            for(String[] f:lookup(c.getConfiguration(),3)){
                scores.put(f[0]+":"+f[1],Integer.parseInt(f[2]));
                if(f[0].equals("R"))max=Math.max(max,Tsv.number(f[1]));
            }
            if(max==Long.MIN_VALUE && c.getConfiguration().getLong("retail.customers",0)>0)throw new IOException("Missing RFM dates");
            // The true reference is max SALE date, including anonymous orders.
            reference=LocalDate.parse(c.getConfiguration().get("retail.reference.date"));
        }
        protected void map(LongWritable k,Text v,Context c)throws IOException,InterruptedException{
            String[] f=Tsv.parse(v.toString(),4);
            Integer r=scores.get("R:"+LocalDate.parse(f[1]).toEpochDay()),fs=scores.get("F:"+f[2]),m=scores.get("M:"+f[3]);
            if(r==null||fs==null||m==null)throw new IOException("Missing RFM score");
            String segment=r>=4&&fs>=4&&m>=4?"CORE":r<=2&&fs>=4?"AT_RISK":r>=4&&fs<=2?"NEW":r>=3?"POTENTIAL":"OTHER";
            out.write("rows",new Text(Tsv.line(f[0],f[1],ChronoUnit.DAYS.between(LocalDate.parse(f[1]),reference),f[2],f[3],r,fs,m,segment)),NullWritable.get(),"rfm/part");
        }
        protected void cleanup(Context c)throws IOException,InterruptedException{out.close();}
    }
    public static class OrderMap extends Mapper<LongWritable,Text,Text,Text>{
        protected void map(LongWritable k,Text v,Context c)throws IOException,InterruptedException{
            String[] f=Tsv.parse(v.toString(),5);
            c.write(new Text(Tsv.line(f[0])+"\t0"),new Text("O\t"+Tsv.encode(f[3])));
        }
    }
    public static class BasketMap extends Mapper<LongWritable,Text,Text,Text>{
        protected void map(LongWritable k,Text v,Context c)throws IOException,InterruptedException{
            String[] f=Tsv.parse(v.toString(),15);
            if(Aggregates.sale(f)&&f[14].equals("true"))c.write(new Text(Tsv.line(f[2])+"\t1\t"+Tsv.encode(f[3])),new Text("S\t"+Tsv.line(f[3],f[7],f[1])));
        }
    }
    public static class Baskets extends Output{
        protected void reduce(Text key,Iterable<Text> values,Context c)throws IOException,InterruptedException{
            String invoice=Tsv.parse(key.toString(),-1)[0],customer=null,stock=null,time=null;
            long row=Long.MAX_VALUE;boolean order=false;
            for(Text value:values){
                String[] f=Tsv.parse(value.toString(),-1);
                if(f[0].equals("O")){if(order)throw new IOException("Duplicate order fact");order=true;customer=f[1];continue;}
                if(!order)throw new IOException("Basket has no order identity");
                if(stock!=null&&!stock.equals(f[1])){emit("baskets",Tsv.line(invoice,customer,time,stock));row=Long.MAX_VALUE;}
                stock=f[1];long current=Tsv.number(f[3]);if(current<row){row=current;time=f[2];}
            }
            if(stock!=null)emit("baskets",Tsv.line(invoice,customer,time,stock));
        }
    }
    public static class PairMap extends Mapper<LongWritable,Text,Text,Text>{
        protected void map(LongWritable k,Text v,Context c)throws IOException,InterruptedException{
            String[] f=Tsv.parse(v.toString(),4);c.write(new Text(Tsv.encode(f[0])),new Text(Tsv.encode(f[3])));
        }
    }
    public static class Pairs extends Output{
        protected void reduce(Text key,Iterable<Text> values,Context c)throws IOException,InterruptedException{
            TreeSet<String> items=new TreeSet<>(Tsv::bytes);long size=0;
            for(Text v:values){size=Tsv.add(size,1);items.add(Tsv.decode(v.toString()));if(items.size()>100)items.pollLast();}
            c.getCounter("RETAIL","ELIGIBLE_BASKET_COUNT").increment(1);
            if(size>100){c.getCounter("RETAIL","TRUNCATED_BASKETS").increment(1);c.getCounter("RETAIL","TRUNCATED_BASKET_ITEMS").increment(size-100);}
            List<String> list=new ArrayList<>(items);
            for(int i=0;i<list.size();i++){
                emit("occurrences",Tsv.line("F",list.get(i),null,1));
                for(int j=i+1;j<list.size();j++)emit("occurrences",Tsv.line("P",list.get(i),list.get(j),1));
            }
        }
    }
    public static class CountMap extends Mapper<LongWritable,Text,Text,Text>{
        protected void map(LongWritable k,Text v,Context c)throws IOException,InterruptedException{
            String[] f=Tsv.parse(v.toString(),4);c.write(new Text(Tsv.line(f[0],f[1],f[2])),new Text(f[3]));
        }
    }
    public static class Counts extends Output{
        protected void reduce(Text key,Iterable<Text> values,Context c)throws IOException,InterruptedException{
            long n=0;for(Text v:values)n=Tsv.add(n,Tsv.number(v.toString()));String[] f=Tsv.parse(key.toString(),3);
            emit(f[0].equals("P")?"item_pairs":"item_frequency",f[0].equals("P")?Tsv.line(f[1],f[2],n):Tsv.line(f[1],n));
        }
    }
    public static class SimilarityMap extends Mapper<LongWritable,Text,Text,Text>{
        Map<String,Long> frequencies=new HashMap<>();long baskets;
        protected void setup(Context c)throws IOException{
            for(String[] f:lookup(c.getConfiguration(),2))if(frequencies.put(f[0],Tsv.number(f[1]))!=null)throw new IOException("Duplicate item frequency");
            baskets=c.getConfiguration().getLong("retail.baskets",0);
        }
        protected void map(LongWritable k,Text v,Context c)throws IOException,InterruptedException{
            String[] f=Tsv.parse(v.toString(),3);long count=Tsv.number(f[2]);if(count<3)return;
            Long ni=frequencies.get(f[0]),nj=frequencies.get(f[1]);
            if(ni==null||nj==null||count>ni||count>nj||Math.max(ni,nj)>baskets||Tsv.bytes(f[0],f[1])>=0)throw new IOException("Inconsistent pair frequency");
            double sim=count/Math.sqrt((double)ni*nj),lift=(double)count*baskets/((double)ni*nj);
            c.write(new Text(Tsv.encode(f[0])),new Text(Tsv.line(f[1],format(sim),count,format((double)count/ni),format(lift))));
            c.write(new Text(Tsv.encode(f[1])),new Text(Tsv.line(f[0],format(sim),count,format((double)count/nj),format(lift))));
        }
    }
    static String format(double value)throws IOException{if(!Double.isFinite(value))throw new IOException("Nonfinite score");return String.format(Locale.ROOT,"%.17g",value);}
    static final Comparator<String[]> BEST=(a,b)->{
        int d=Double.compare(Double.parseDouble(b[1]),Double.parseDouble(a[1]));
        if(d==0)d=Long.compare(Long.parseLong(b[2]),Long.parseLong(a[2]));
        return d==0?Tsv.bytes(a[0],b[0]):d;
    };
    public static class Similarity extends Output{
        protected void reduce(Text key,Iterable<Text> values,Context c)throws IOException,InterruptedException{
            PriorityQueue<String[]> top=new PriorityQueue<>(11,BEST.reversed());
            for(Text v:values){top.add(Tsv.parse(v.toString(),5));if(top.size()>10)top.poll();}
            List<String[]> sorted=new ArrayList<>(top);sorted.sort(BEST);int rank=0;
            for(String[] f:sorted)emit("similarity",Tsv.line(Tsv.decode(key.toString()),f[0],f[1],f[2],f[3],f[4],++rank));
        }
    }
}
