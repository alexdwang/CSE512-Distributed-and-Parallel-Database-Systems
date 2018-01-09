import java.awt.List;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Equijoin {
	public static class JoinMapper extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String words[] = value.toString().replaceAll(" ", "").split(",");
			Text mykey = new Text(words[1]);
			context.write(mykey, value);
		}
	}

	public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> content, Context context)
				throws IOException, InterruptedException {
			Vector<String> vecA = new Vector<String>();
			Vector<String> vecB = new Vector<String>();
			Iterator values = content.iterator();
			String str = values.next().toString();
			String tablename1 = str.replaceAll(" ", "").split(",")[0];
			vecA.add(str);
			while (values.hasNext()) {
				String value = values.next().toString();
				if (value.replaceAll(" ", "").split(",")[0].equals(tablename1))
					vecA.add(value);
				else
					vecB.add(value);
			}

			int i, j;
			for (i = 0; i < vecA.size(); i++) {
				for (j = 0; j < vecB.size(); j++) {
					context.write(new Text(vecA.get(i)),
							new Text(", " + vecB.get(j)));
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "join");
		job.setJarByClass(Equijoin.class);
		job.setMapperClass(JoinMapper.class);
		job.setReducerClass(JoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
