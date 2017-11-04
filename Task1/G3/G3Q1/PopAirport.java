import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;
import java.util.TreeSet;

public class PopAirport extends Configured implements Tool {

 	private static final Log LOG = LogFactory.getLog(PopAirport.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PopAirport(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/T1/G3Q1/tmp");
        fs.delete(tmpPath, true);

        Job jobA = Job.getInstance(conf, "Airport");
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(IntWritable.class);

        jobA.setMapperClass(PopAirportMap.class);
        jobA.setReducerClass(PopAirportReduce.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);

        jobA.setJarByClass(PopAirport.class);
        jobA.waitForCompletion(true);


        Job jobB = Job.getInstance(conf, "Popularity");
        jobB.setOutputKeyClass(Text.class);
        jobB.setOutputValueClass(IntWritable.class);

        jobB.setMapOutputKeyClass(NullWritable.class);
        jobB.setMapOutputValueClass(TextArrayWritable.class);

        jobB.setMapperClass(PopularityMap.class);
        jobB.setReducerClass(PopularityReduce.class);
        jobB.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

        jobB.setJarByClass(PopAirport.class);

        return jobB.waitForCompletion(true) ? 0 : 1;
    }

    public static String readHDFSFile(String path, Configuration conf) throws IOException{
        Path pt=new Path(path);
        FileSystem fs = FileSystem.get(pt.toUri(), conf);
        FSDataInputStream file = fs.open(pt);
        BufferedReader buffIn=new BufferedReader(new InputStreamReader(file));

        StringBuilder everything = new StringBuilder();
        String line;
        while( (line = buffIn.readLine()) != null) {
            everything.append(line);
            everything.append("\n");
        }
        return everything.toString();
    }

    public static class TextArrayWritable extends ArrayWritable {
        public TextArrayWritable() {
            super(Text.class);
        }

        public TextArrayWritable(String[] strings) {
            super(Text.class);
            Text[] texts = new Text[strings.length];
            for (int i = 0; i < strings.length; i++) {
                texts[i] = new Text(strings[i]);
            }
            set(texts);
        }
    }


    public static class PopAirportMap extends Mapper<Object, Text, Text, IntWritable> {
        String delimiters;

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {

            Configuration conf = context.getConfiguration();
            String delimitersPath = conf.get("delimiters");
            this.delimiters = readHDFSFile(delimitersPath, conf);
        }


        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line, this.delimiters);	
            int i = 0; 
            int count=0;
   
            // each line contains (passengers, origin, dest)
			while (tokenizer.hasMoreTokens()) {
				String nextToken = tokenizer.nextToken();
                
                switch (i) {
  					case 0: 
                      	count = Integer.valueOf(nextToken.trim());
                        break;
      				case 1:
                        String origin = nextToken.trim().toUpperCase();
					    context.write(new Text(origin), new IntWritable(count));
                        break;
					case 2:	
                        String dest = nextToken.trim().toUpperCase();
					    context.write(new Text(dest), new IntWritable(count));
                        break;
                    default:
						LOG.info("Bad data:"+nextToken.trim());
				}
 				i++;
			}  
        }
    }

    public static class PopAirportReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
        }
    }


    public static class PopularityMap extends Mapper<Text, Text, NullWritable, TextArrayWritable> {

        public static final Log log = LogFactory.getLog(PopularityMap.class);
        private TreeSet<Pair<Integer, String>> topN = new TreeSet<Pair<Integer, String>>();
        private TextArrayWritable word = new TextArrayWritable();


        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

            Integer count = Integer.parseInt(value.toString());
            String word = key.toString();

            topN.add(new Pair(count, word));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Pair<Integer, String> item : topN) {
                String[] strings = {item.second, item.first.toString()};
                TextArrayWritable val = new TextArrayWritable(strings);
                context.write(NullWritable.get(), val);
            }
        }
    }

    public static class PopularityReduce extends Reducer<NullWritable, TextArrayWritable, Text, IntWritable> {

        private TreeSet<Pair<Integer, String>> topN = new TreeSet<Pair<Integer, String>>(); 

        @Override
        public void reduce(NullWritable key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {

            for (TextArrayWritable value : values) {
                Text[] textArray = (Text[]) value.toArray();

                String word = textArray[0].toString();
                Integer count = Integer.parseInt(textArray[1].toString());
//                System.out.println("TopAirportsMap 1: " + word + " " + count);

                topN.add(new Pair<Integer, String>(count, word));
            }

            for (Pair<Integer, String> item: topN) {
                Text word = new Text(item.second);
                IntWritable count = new IntWritable(item.first);
//                System.out.println("TopAirportsMap 2: " + word + " " + count);

                context.write(word, count);
            }
        }
    }
}


// >>> Don't Change
class Pair<A extends Comparable<? super A>,
        B extends Comparable<? super B>>
        implements Comparable<Pair<A, B>> {

    public final A first;
    public final B second;

    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public static <A extends Comparable<? super A>,
            B extends Comparable<? super B>>
    Pair<A, B> of(A first, B second) {
        return new Pair<A, B>(first, second);
    }

    @Override
    public int compareTo(Pair<A, B> o) {
        int cmp = o == null ? 1 : (this.first).compareTo(o.first);
        return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
    }

    @Override
    public int hashCode() {
        return 31 * hashcode(first) + hashcode(second);
    }

    private static int hashcode(Object o) {
        return o == null ? 0 : o.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Pair))
            return false;
        if (this == obj)
            return true;
        return equal(first, ((Pair<?, ?>) obj).first)
                && equal(second, ((Pair<?, ?>) obj).second);
    }

    private boolean equal(Object o1, Object o2) {
        return o1 == o2 || (o1 != null && o1.equals(o2));
    }

    @Override
    public String toString() {
        return "(" + first + ", " + second + ')';
    }
}
// <<< Don't Change
