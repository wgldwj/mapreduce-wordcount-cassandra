/*
/bin/rm -rf WordCountCassandra_classes ; mkdir WordCountCassandra_classes
classpath=. && for jar in /usr/share/brisk/{cassandra,hadoop}/lib/*.jar; do classpath=$classpath:$jar done
javac -classpath $classpath -d WordCountCassandra_classes WordCountCassandra.java
jar -cvf /root/brisk/WordCountCassandra.jar -C WordCountCassandra_classes/ .
brisk hadoop jar /root/brisk/WordCountCassandra.jar WordCountCassandra 
[default@wordcount] get output_words[ascii('bible')]['advantage'];
=> (column=advantage, value=15, timestamp=1308734369159)

*/

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.hadoop.ColumnFamilyOutputFormat;

import static com.google.common.base.Charsets.UTF_8;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.hadoop.ColumnFamilyInputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This counts the occurrences of words in ColumnFamily "input_words", that has a single column (that we care about)
 * containing a sequence of words.
 *
 * For each word, we output the total number of occurrences across all texts.
 *
 * When outputting to Cassandra, we write the word counts as a {word, count} column/value pair,
 * with a row key equal to the name of the source column we read the words from.
 */
public class WordCountCassandra extends Configured implements Tool
{

    public static class TokenizerMapper extends Mapper<ByteBuffer, SortedMap<ByteBuffer, IColumn>, Text, IntWritable>
    {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private ByteBuffer sourceColumn;
        
        String punctuationsToStrip[] = { "\"", "'", ",", ";", "!", ":", "\\?", "\\.", "\\(", "\\-", "\\[", "\\)", "\\]" };

        protected void setup(Mapper.Context context) throws IOException, InterruptedException {
            sourceColumn = ByteBufferUtil.bytes(context.getConfiguration().get("columnname"));         
        }

        public void map(ByteBuffer key, SortedMap<ByteBuffer, IColumn> columns, Context context) throws IOException, InterruptedException
        {         
            // Our slice predicate contains only one column. We fetch it here
            IColumn column = columns.get(sourceColumn);
            if (column == null)
                return;
            String value = ByteBufferUtil.string(column.value());
            
            value = value.toLowerCase();
            for (String pattern : punctuationsToStrip) {
              value = value.replaceAll(pattern, "");
            }            

            StringTokenizer itr = new StringTokenizer(value);
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class ReducerToCassandra extends Reducer<Text, IntWritable, ByteBuffer, List<Mutation>>
    {
        private ByteBuffer outputKey;

        protected void setup(Reducer.Context context) throws IOException, InterruptedException
        {
            // The row key is the name of the column from which we read the text
            outputKey = ByteBufferUtil.bytes(context.getConfiguration().get("columnname"));
        }

        public void reduce(Text word, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(outputKey, Collections.singletonList(getMutation(word, sum)));
        }

        // See Cassandra API (http://wiki.apache.org/cassandra/API)
        private static Mutation getMutation(Text word, int sum)
        {
            Column c = new Column();
            c.setName(Arrays.copyOf(word.getBytes(), word.getLength()));
            c.setValue(ByteBufferUtil.bytes(String.valueOf(sum)));
            c.setTimestamp(System.currentTimeMillis());

            Mutation m = new Mutation();
            m.setColumn_or_supercolumn(new ColumnOrSuperColumn());
            m.column_or_supercolumn.setColumn(c);
            return m;
        }
    }

    public int run(String[] args) throws Exception
    {
        String columnName = "bible";
        getConf().set("columnname", columnName);

        Job job = new Job(getConf(), "wordcount");
        job.setJarByClass(WordCountCassandra.class);
        
        job.setMapperClass(TokenizerMapper.class);
        // Tell the Mapper to expect Cassandra columns as input
        job.setInputFormatClass(ColumnFamilyInputFormat.class); 
        // Tell the "Shuffle/Sort" phase of M/R what type of Key/Value to expect from the mapper        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(ReducerToCassandra.class);
        job.setOutputFormatClass(ColumnFamilyOutputFormat.class);        
        job.setOutputKeyClass(ByteBuffer.class);
        job.setOutputValueClass(List.class);

        // Set the keyspace and column family for the output of this job
        ConfigHelper.setOutputColumnFamily(job.getConfiguration(), "wordcount", "output_words");
        // Set the keyspace and column family for the input of this job
        ConfigHelper.setInputColumnFamily(job.getConfiguration(), "wordcount", "input_words");
        
        ConfigHelper.setRpcPort(job.getConfiguration(), "9160");
        ConfigHelper.setInitialAddress(job.getConfiguration(), "88.190.17.139");
        ConfigHelper.setPartitioner(job.getConfiguration(), "org.apache.cassandra.dht.RandomPartitioner");
      
        // Set the predicate that determines what columns will be selected from each row
        SlicePredicate predicate = new SlicePredicate().setColumn_names(Arrays.asList(ByteBufferUtil.bytes(columnName)));
        // The "get_slice" (see Cassandra's API) operation will be applied on each row of the ColumnFamily.
        // Each row will be handled by one Map job.
        ConfigHelper.setInputSlicePredicate(job.getConfiguration(), predicate);

        job.waitForCompletion(true);
        return job.isSuccessful() ? 0:1;
    }
    
    public static void main(String[] args) throws Exception
    {
        // Let ToolRunner handle generic command-line options
        ToolRunner.run(new Configuration(), new WordCountCassandra(), args);
        System.exit(0);
    }    
}
