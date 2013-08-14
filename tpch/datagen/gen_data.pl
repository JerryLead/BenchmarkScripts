#!/usr/bin/perl -w

###############################################################################
# This script can be used to generate TPCH data in a distributed
# fashion and load them into HDFS.
#
# Usage:
#  perl gen_data.pl scale_factor num_files zipf_factor host_list local_dir hdfs_dir
#  
#  where:
#    scale_factor = TPCH Scale factor (GB of data to generate)
#    num_files    = The number of files to generate for each table
#    zipf_factor  = Zipfian distribution factor (0-4, 0 means uniform)
#    host_list    = File containing a list of host machines
#    local_dir    = Local directory to use in the host machines
#    hdfs_dir     = HDFS directory to store the generated data
#
# Assumptions/Requirements:
# 1. The enviromental variable $HADOOP_HOME is defined in the master node and
#    all the slave nodes and it is the same in all nodes.
# 2. The local directory does not exist in the slave nodes
# 3. The HDFS directory does not exist
# 4. There is enough local disk space on the slave nodes to generate the data
# 5. The number of files must be greater than half the scale factor to ensure
#    that we don't try to generate a file that is greater than 2GB
#
# The data is loaded into HDFS. The name for each file is of the form
# "tablename.tbl.x", where tablename is lineitem, orders etc, and 
# x is a number between 1 and <num_files>.
# Each table is placed in a corresponding directory under <hdfs_dir>.
#
# Author: Herodotos Herodotou
# Date: July 07, 2010
#
##############################################################################

# Simple method to print new lines
sub println {
    local $\ = "\n";
    print @_;
}

# Make sure we have all the arguments
if ($#ARGV != 5)
{
   println qq(Usage: perl $0 scale_factor num_files zipf_factor host_list local_dir hdfs_dir);
   println qq(  scale_factor: TPCH Scale factor \(GB of data to generate\));
   println qq(  num_files:    The number of files to generate for each table);
   println qq(  zipf_factor:  Zipfian distribution factor \(0-4, 0 means uniform\));
   println qq(  host_list:    File containing a list of host machines);
   println qq(  local_dir:    Local directory to use in the host machines);
   println qq(  hdfs_dir:     HDFS directory to store the generated data);
   exit(-1);
}

# Get the input data
my $SCALE_FACTOR    = $ARGV[0];
my $NUM_FILE_SPLITS = $ARGV[1];
my $ZIPF_FACTOR     = $ARGV[2];
my $HOST_LIST       = $ARGV[3];
my $LOCAL_DIR       = $ARGV[4];
my $HDFS_DIR        = $ARGV[5];

# Start data generation
println qq(Starting data generation at: ) . `date`;
println qq(Input Parameters:);
println qq(  Scale Factor:    $SCALE_FACTOR);
println qq(  Number of Files: $NUM_FILE_SPLITS);
println qq(  ZIPF Factor:     $ZIPF_FACTOR);
println qq(  Host List:       $HOST_LIST);
println qq(  Local Directory: $LOCAL_DIR);
println qq(  HDFS Directory:  $HDFS_DIR);
println qq();

# Error checking
if ($SCALE_FACTOR <= 0)
{
   println qq(ERROR: The scale factor must be greater than 0);
   exit(-1);
}

if ($NUM_FILE_SPLITS < $SCALE_FACTOR / 2)
{
   println qq(ERROR: The number of files must be greater than half the scale factor);
   exit(-1);
}

if ($ZIPF_FACTOR < 0 || $ZIPF_FACTOR > 4)
{
   println qq(ERROR: The zipf factor must be between 0 and 4);
   exit(-1);
}

if (!-e $HOST_LIST)
{
   println qq(ERROR: The file '$HOST_LIST' does not exist);
   exit(-1);
}

if (!$ENV{'HADOOP_HOME'})
{
   println qq(ERROR: \$HADOOP_HOME is not defined);
   exit(-1);
}

# Execute the hadoop-env.sh script for environmental variable definitions
!system qq(. \$HADOOP_HOME/conf/hadoop-env.sh) or die $!;
my $hadoop_home = $ENV{'HADOOP_HOME'};
my $ssh_opts = ($ENV{'HADOOP_SSH_OPTS'}) ? $ENV{'HADOOP_SSH_OPTS'} : "";

# Get the hosts
open INFILE, "<", $HOST_LIST;
my @hosts = ();
while ($line = <INFILE>)
{
   $line =~ s/(^\s+)|(\s+$)//g;
   push(@hosts, $line) if $line =~ /\S/
}
close INFILE;

# Make sure we have some hosts
my $num_hosts = scalar(@hosts);
if ($num_hosts <= 0)
{
   println qq(ERROR: No hosts were found in '$HOST_LIST');
   exit(-1);
}

# Create all the HDFS directories
if (`$hadoop_home/bin/hadoop fs -stat $HDFS_DIR 2>&1` !~ /cannot stat/) {
   println qq(ERROR: The directory '$HDFS_DIR' already exists);
   exit(-1);
}
println qq(Creating all the HDFS directories);
!system qq($hadoop_home/bin/hadoop fs -mkdir $HDFS_DIR/lineitem) or die $!;
!system qq($hadoop_home/bin/hadoop fs -mkdir $HDFS_DIR/orders) or die $!;
!system qq($hadoop_home/bin/hadoop fs -mkdir $HDFS_DIR/customer) or die $!;
!system qq($hadoop_home/bin/hadoop fs -mkdir $HDFS_DIR/partsupp) or die $!;
!system qq($hadoop_home/bin/hadoop fs -mkdir $HDFS_DIR/part) or die $!;
!system qq($hadoop_home/bin/hadoop fs -mkdir $HDFS_DIR/supplier) or die $!;
!system qq($hadoop_home/bin/hadoop fs -mkdir $HDFS_DIR/nation) or die $!;
!system qq($hadoop_home/bin/hadoop fs -mkdir $HDFS_DIR/region) or die $!;
println qq();

# Create the execution script that will be sent to the hosts
open OUTFILE, ">", "gen_and_load.sh" or die $!;
print OUTFILE qq(unzip -n tpch_data_gen.zip\n);
print OUTFILE qq(perl tpch_gen_data.pl data.properties\n);
print OUTFILE qq($hadoop_home/bin/hadoop fs -put data/lineitem.tbl* $HDFS_DIR/lineitem\n);
print OUTFILE qq($hadoop_home/bin/hadoop fs -put data/orders.tbl*   $HDFS_DIR/orders\n);
print OUTFILE qq($hadoop_home/bin/hadoop fs -put data/customer.tbl* $HDFS_DIR/customer\n);
print OUTFILE qq($hadoop_home/bin/hadoop fs -put data/partsupp.tbl* $HDFS_DIR/partsupp\n);
print OUTFILE qq($hadoop_home/bin/hadoop fs -put data/part.tbl*     $HDFS_DIR/part\n);
print OUTFILE qq($hadoop_home/bin/hadoop fs -put data/supplier.tbl* $HDFS_DIR/supplier\n);
print OUTFILE qq($hadoop_home/bin/hadoop fs -put data/nation.tbl*   $HDFS_DIR/nation\n);
print OUTFILE qq($hadoop_home/bin/hadoop fs -put data/region.tbl*   $HDFS_DIR/region\n);
print OUTFILE qq(rm -rf data/*.tbl*\n);
close OUTFILE;
chmod 0744, "gen_and_load.sh";

# Each host will generate a certain range of the file splits
my $num_splits_per_host = int($NUM_FILE_SPLITS / $num_hosts);
$num_splits_per_host = 1 if $num_splits_per_host < 1;
my $first_file_split = 1;

# Connect to each host and generate the data
for ($host = 0; $host < $num_hosts; $host++)
{
   # Calculate the last file split generated by this host
   $last_file_split = ($host == $num_hosts-1) 
                      ? $NUM_FILE_SPLITS 
                      : $first_file_split + $num_splits_per_host - 1;
   
   # Create the data.properties file and copy it to the host
   open OUTFILE, ">", "data.properties" or die $!;
   print OUTFILE qq(scaling_factor = $SCALE_FACTOR \n);
   print OUTFILE qq(num_file_splits = $NUM_FILE_SPLITS \n);
   print OUTFILE qq(first_file_split = $first_file_split \n);
   print OUTFILE qq(last_file_split = $last_file_split \n);
   print OUTFILE qq(zipf = $ZIPF_FACTOR \n);
   print OUTFILE qq(tpch_home = $LOCAL_DIR/data \n);
   close OUTFILE;

   # Copy the necessary files to the host
   println qq(Sending files to host: $hosts[$host]);
   !system qq(ssh $ssh_opts $hosts[$host] \"mkdir $LOCAL_DIR\") or die $!;
   !system qq(scp gen_and_load.sh   $hosts[$host]:$LOCAL_DIR/.) or die $!;
   !system qq(scp tpch_data_gen.zip $hosts[$host]:$LOCAL_DIR/.) or die $!;
   !system qq(scp data.properties   $hosts[$host]:$LOCAL_DIR/.) or die $!;

   # Start the data generation in a child process
   println qq(Starting data generation at host: $hosts[$host]\n);
   unless (fork)
   {
      system qq(ssh $ssh_opts $hosts[$host] ).
             qq(\"cd $LOCAL_DIR; ./gen_and_load.sh >& gen_and_load.out\");
      println qq(Data generation completed at host: $hosts[$host]\n);
      exit(0);
   }

   # Exit the loop if we have generated all file splits
   $first_file_split = $last_file_split + 1;
   last if $last_file_split == $NUM_FILE_SPLITS;
}

# Wait for the hosts to complete
println qq(Waiting for the data generation to complete);
for ($host = 0; $host < $num_hosts; $host++)
{
   wait;
}

# Clean up
system qq(rm data.properties);
system qq(rm gen_and_load.sh);

# Done
$time = time - $^T;
println qq();
println qq(Data generation is complete!);
println qq(Time taken (sec):\t$time);

