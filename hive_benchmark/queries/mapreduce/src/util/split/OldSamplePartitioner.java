package util.split;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

public class OldSamplePartitioner<K, V> implements Partitioner<K, V> {

	public void configure(JobConf job) {
	}

	/** Use {@link Object#hashCode()} to partition. */
	public int getPartition(K key, V value, int numReduceTasks) {
		int N = numReduceTasks;
		int r = (key.hashCode() & Integer.MAX_VALUE) % (N * (N+1) / 2) + 1;
		int partitionNum = (int) ((-1 + Math.sqrt(1 - 4 * (2 - 2 * r))) / 2);
	    return partitionNum;
	}

}
