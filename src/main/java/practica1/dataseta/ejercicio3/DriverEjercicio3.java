package practica1.dataseta.ejercicio3;

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

public class DriverEjercicio3 extends Configured implements Tool{

	static Configuration conf = new Configuration();

	public static void main(String args[]) throws Exception{
		int exitcode = ToolRunner.run(conf, new DriverEjercicio3(), args);
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
		
		conf.set("quartlies", args[1]);
		
		Job job = Job.getInstance(conf, "Ejercicio 3");
        job.setJarByClass(DriverEjercicio3.class);
        
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        
        job.setMapperClass(Mapper.class);
        job.setMapOutputKeyClass(CustomKey.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        
        job.setReducerClass(Reducer.class);
     
        
        boolean success = job.waitForCompletion(true);
        return (success ? 0 : 1);
	}

}
