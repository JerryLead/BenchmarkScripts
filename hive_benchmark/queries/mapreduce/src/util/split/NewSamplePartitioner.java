package util.split;

import org.apache.hadoop.mapreduce.Partitioner;

public class NewSamplePartitioner<K,V> extends Partitioner<K, V> {

  public int getPartition(K key, V value,
                          int numReduceTasks) {
	int N = numReduceTasks;
	int r = (key.hashCode() & Integer.MAX_VALUE) % (N * (N+1) / 2) + 1;
	int partitionNum = (int) ((-1 + Math.sqrt(1 - 4 * (2 - 2 * r))) / 2);
    return partitionNum;
  }

}