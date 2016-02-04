package practica1.datasetb.ejercicio8;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DriverEjercicio6 extends Configured implements Tool{

	static Configuration conf = new Configuration();

	public static void main(String args[]) throws Exception{
		int exitcode = ToolRunner.run(conf, new DriverEjercicio6(), args);
		System.exit(exitcode);
	}

	@Override
	public int run(String[] args) throws Exception {
		
		if (args.length != 2) {
			System.out.printf("Usage: " + this.getClass().getName() + "<input dir1> <output dir>\n");
			System.exit(-1);
		}
		
		// Delete outopput folder if exits
		FileSystem fs = FileSystem.get(conf);
		Path output = new Path(args[1]);
		if(fs.exists(output)){
			fs.delete(output,true);
		}
		
		
		Job job = Job.getInstance(conf, "Ejercicio 6");
        job.setJarByClass(DriverEjercicio6.class);
        
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.setMapperClass(Mapper.class);
        job.setMapOutputKeyClass(CustomKey.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        
        job.setReducerClass(Reducer.class);
     
        
        boolean success = job.waitForCompletion(true);
        return (success ? 0 : 1);
	}

}
