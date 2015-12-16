import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
/**
 * 
 * @author Liyoung
 *
 */
public class WordCount {
	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private String filter = "[^A-Za-z0-9]"; // A regular expression, it
												// stand for character which is
												// not a letter or a figure.
		// Define this HashMap to store the key-value pair in the method map.
		HashMap<String, IntWritable> map = new HashMap<String, IntWritable>();

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
		 * org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// All the words will be converted to lowercase.
			String line = value.toString().toLowerCase();
			line = line.replaceAll(filter, " "); // Filter all punctuation.
			StringTokenizer itr = new StringTokenizer(line);
			while (itr.hasMoreTokens()) {
				String value1 = itr.nextToken();
				if (map.containsKey(value1)) {
					int p = map.get(value1).get() + 1;
					map.put(value1, new IntWritable(p));
				} else {
					map.put(value1, new IntWritable(1));
				}
			}
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * org.apache.hadoop.mapreduce.Mapper#run(org.apache.hadoop.mapreduce
		 * .Mapper.Context)
		 */
		@Override
		public void run(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			super.run(context);
			Iterator<?> it = map.entrySet().iterator();
			while (it.hasNext()) {
				@SuppressWarnings("unchecked")
				Map.Entry<String, IntWritable> entry = (Map.Entry<String, IntWritable>) it
						.next();
				context.write(new Text(entry.getKey()), entry.getValue());
			}
		}
	}

	/*
	 * Reducer. In the traditional program of mapReduce,this class is very
	 * important. But in this program, this class is not important because we
	 * have done the work in the map step which should do by reduce to promote
	 * the rate of the calculation.
	 */
	public static class CalculateReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable quantity = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable v : values) {
				sum += v.get();
			}
			quantity.set(sum);
			context.write(key, quantity);
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf);
		job.setJarByClass(WordCount.class);
		job.setOutputKeyClass(Text.class);
		// Set the separator of the result.
		job.getConfiguration().set("mapred.textoutputformat.separator", ":");
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(CalculateReducer.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
