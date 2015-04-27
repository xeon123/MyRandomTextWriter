package org.apache.hadoop.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public class SequenceFilesUtils {
	private static final Configuration conf = new Configuration();

	public static <K, V> void merge(Path fromDirectory, Path toFile, Class<K> keyClass, Class<V> valueClass) throws IOException {
		FileSystem fs = FileSystem.get(conf);

		if (!fs.isDirectory(fromDirectory)) {
			throw new IllegalArgumentException("'" + fromDirectory.toString() + "' is not a directory");
		}

		SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, toFile, keyClass, valueClass);

		for (FileStatus status : fs.listStatus(fromDirectory)) {
			if (status.isDir()) {
				System.out.println("Skip directory '" + status.getPath().getName() + "'");
				continue;
			}

			Path file = status.getPath();

			if (file.getName().startsWith("_")) {
				System.out.println("Skip \"_\"-file '" + file.getName() + "'"); //There are files such "_SUCCESS"-named in jobs' ouput folders 
				continue;
			}

			System.out.println("Merging '" + file.getName() + "'");

			SequenceFile.Reader reader = new SequenceFile.Reader(fs, file, conf);
			Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);

			while (reader.next(key, value)) {
				writer.append(key, value);
			}

			reader.close();
		}

		writer.close();
	}
}