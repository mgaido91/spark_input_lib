package it.mgaido.spark.io;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class FileWithHeaderReader extends FileInputFormat<LongWritable,Text> {

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit is,
			TaskAttemptContext tc) throws IOException, InterruptedException {
		return new InputFileWithHeaderReader();
	}

}
