package cn.hadooplab.retail;

import java.net.URI;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

/** One independently tracked YARN application per physical pipeline stage. */
public final class RetailJobs extends Configured implements Tool {
    @Override public int run(String[] arguments)throws Exception{
        Map<String,String> args=new HashMap<>();
        for(int i=0;i<arguments.length;i+=2){if(i+1>=arguments.length||!arguments[i].startsWith("--")||args.put(arguments[i].substring(2),arguments[i+1])!=null)throw new IllegalArgumentException("Invalid stage arguments");}
        String stage=Objects.requireNonNull(args.get("stage")),run=Objects.requireNonNull(args.get("run-id"));
        Configuration conf=getConf();conf.set("retail.stage",stage);
        for(String name:Arrays.asList("source-sha","customers","reference-date","baskets"))if(args.containsKey(name))conf.set("retail."+name.replace('-','.'),args.get(name));
        conf.set("mapreduce.output.textoutputformat.separator", "\t");
        Job job=Job.getInstance(conf,"retail-"+stage+"-"+run);
        job.setJarByClass(RetailJobs.class);
        job.setMapOutputKeyClass(Text.class);job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(Integer.parseInt(args.getOrDefault("reducers","2")));
        if(job.getNumReduceTasks()<1)throw new IllegalArgumentException("reducers must be positive");
        FileOutputFormat.setOutputPath(job,new Path(Objects.requireNonNull(args.get("output"))));
        MultipleOutputs.addNamedOutput(job,"rows",TextOutputFormat.class,Text.class,NullWritable.class);
        String input=Objects.requireNonNull(args.get("input"));
        if(stage.equals("J01")){
            MultipleInputs.addInputPath(job,new Path(input),TextInputFormat.class,Clean.SourceMapper.class);
            MultipleInputs.addInputPath(job,new Path(Objects.requireNonNull(args.get("warnings"))),TextInputFormat.class,Clean.WarningMapper.class);
            job.setReducerClass(Clean.Rows.class);
        }else if(stage.equals("J06baskets")){
            MultipleInputs.addInputPath(job,new Path(input),TextInputFormat.class,Features.BasketMap.class);
            MultipleInputs.addInputPath(job,new Path(Objects.requireNonNull(args.get("orders"))),TextInputFormat.class,Features.OrderMap.class);
            job.setPartitionerClass(Features.FirstPartition.class);job.setGroupingComparatorClass(Features.FirstGroup.class);job.setReducerClass(Features.Baskets.class);
        }else{
            FileInputFormat.addInputPath(job,new Path(input));
            switch(stage){
            case "J02":case "J03":case "J04":case "J05base":case "J06interactions":
                job.setMapperClass(Aggregates.Rows.class);job.setReducerClass(Aggregates.Groups.class);
                if(stage.equals("J06interactions")){job.setPartitionerClass(Aggregates.PrefixPartition.class);job.setGroupingComparatorClass(Aggregates.PrefixGroup.class);}break;
            case "J05distribution":job.setMapperClass(Features.DistributionMap.class);job.setPartitionerClass(Features.FirstPartition.class);job.setReducerClass(Features.Distribution.class);break;
            case "J05":job.setMapperClass(Features.Score.class);job.setNumReduceTasks(0);break;
            case "J07baskets":job.setMapperClass(Features.PairMap.class);job.setReducerClass(Features.Pairs.class);break;
            case "J07":job.setMapperClass(Features.CountMap.class);job.setReducerClass(Features.Counts.class);break;
            case "J08":job.setMapperClass(Features.SimilarityMap.class);job.setReducerClass(Features.Similarity.class);break;
            default:throw new IllegalArgumentException("Unknown pipeline stage");
            }
        }
        if(args.containsKey("cache")){
            String[] paths=args.get("cache").split(",");
            job.getConfiguration().setInt("retail.cache.count",paths.length);
            for(int i=0;i<paths.length;i++)job.addCacheFile(new URI(paths[i]+"#lookup"+i));
        }
        job.submit();
        System.out.println("RETAIL_APPLICATION\t"+job.getJobID().toString().replace("job_","application_"));System.out.flush();
        System.out.println("RETAIL_JOB_COUNTER\tINPUT_SPLITS\t"+job.getConfiguration().getInt("mapreduce.job.maps",-1));
        boolean success=job.waitForCompletion(true);
        for(Counter counter:job.getCounters().getGroup("RETAIL"))System.out.println("RETAIL_COUNTER\t"+counter.getName()+"\t"+counter.getValue());
        for(JobCounter counter: new JobCounter[]{JobCounter.TOTAL_LAUNCHED_MAPS,JobCounter.TOTAL_LAUNCHED_REDUCES})System.out.println("RETAIL_JOB_COUNTER\t"+counter.name()+"\t"+job.getCounters().findCounter(counter).getValue());
        for(TaskCounter counter: new TaskCounter[]{TaskCounter.MAP_INPUT_RECORDS,TaskCounter.MAP_OUTPUT_RECORDS,TaskCounter.REDUCE_INPUT_RECORDS,TaskCounter.REDUCE_SHUFFLE_BYTES,TaskCounter.SPILLED_RECORDS,TaskCounter.MAP_PHYSICAL_MEMORY_BYTES_MAX,TaskCounter.REDUCE_PHYSICAL_MEMORY_BYTES_MAX})System.out.println("RETAIL_JOB_COUNTER\t"+counter.name()+"\t"+job.getCounters().findCounter(counter).getValue());
        System.out.println("RETAIL_RESOURCE\t"+job.getCounters().findCounter("org.apache.hadoop.mapreduce.TaskCounter","CPU_MILLISECONDS").getValue()+"\t"+job.getCounters().findCounter("org.apache.hadoop.mapreduce.TaskCounter","GC_TIME_MILLIS").getValue());
        return success?0:4;
    }
    public static void main(String[] args)throws Exception{System.exit(ToolRunner.run(new Configuration(),new RetailJobs(),args));}
}
