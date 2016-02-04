package practica1.datasetb.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import practica1.dataseta.join.NameComparator;

public class DriverJoin extends Configured implements Tool{

	static Configuration conf = new Configuration();

	public static void main(String args[]) throws Exception{
		int exitcode = ToolRunner.run(conf, new DriverJoin(), args);
		System.exit(exitcode);
	}

	@Override
	public int run(String[] args) throws Exception {
		
		if (args.length != 3) {
			System.out.printf("Usage: " + this.getClass().getName() + "<input dir1> <input dir2> <output dir>\n");
			System.exit(-1);
		}
		
		// Delete outopput folder if exits
		FileSystem fs = FileSystem.get(conf);
		Path output = new Path(args[2]);
		if(fs.exists(output)){
			fs.delete(output,true);
		}
		
		Job job = Job.getInstance(getConf(), "Join Dataset A");
        job.setJarByClass(DriverJoin.class);
        
        MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class,MapperTSV.class);
        MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class,MapperTable.class);
        
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        
        job.setMapOutputKeyClass(CustomKey.class);
        job.setMapOutputValueClass(CustomValue.class);
        
        job.setGroupingComparatorClass(NameComparator.class);
        
        job.setReducerClass(Reducer.class);
     
        
        boolean success = job.waitForCompletion(true);
        return (success ? 0 : 1);
	}

}
