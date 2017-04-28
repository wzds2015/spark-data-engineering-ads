#! /usr/bin/env python

"""
    Created by Wenliang Zhao on 2/2/17.
"""

import os, subprocess

############# Step 1 remove old result files
print "Remove previous output directory if exists"
if os.path.exists("data/output_file"):
    remove_command = "rm -rf data/output_file"
    proc = subprocess.Popen(remove_command, shell=True,).wait()
    if proc != 0:
        print "Remove output directory failed"
        exit(1)
if os.path.exists("metastore_db"):
    remove_command = "rm -rf metastore_db"
    proc = subprocess.Popen(remove_command, shell=True,).wait()
    if proc != 0:
        print "Remove output directory failed"
        exit(1)

############ Step 2 clean old compiling, packaging files
print "Clean code before runing"
clean_command = "sbt clean"
proc = subprocess.Popen(clean_command, shell=True,).wait()
if proc != 0:
    print "Cleaning failed!"
    exit(1)

############ Step 3 package to jar
pack_command = "sbt assembly"
print "Start packing jar"
proc = subprocess.Popen(pack_command, shell=True,).wait()
if proc != 0:     
    print "assembly failed! Check the code again."
    exit(1)

############## Step 4 trim jar, delete manifest files
trim_command = "zip -d target/scala-2.11/user-activity-data-pipeline-assembly-1.0-SNAPSHOT.jar META-INF/*.RSA META-INF/*.DSA META-INF/*.SF"
print "delete manifest files from jar"
proc = subprocess.Popen(trim_command, shell=True,).wait()
if proc != 0:
    print "Error occurs during trim!"
    exit(1)

############ Step 5 run program
data_dir = "/Users/admin/Documents/Study/data/"
input_file = data_dir + "input_file.csv.gz"
wall_off_file = data_dir + "walled_off_segments.txt"
sparse_flag = "flase"
execute_command = "spark-submit target/scala-2.11/user-activity-data-pipeline-assembly-1.0-SNAPSHOT.jar " \
				+ input_file + " " + wall_off_file + " " + sparse_flag
print execute_command
print "Start work......"
proc = subprocess.Popen(execute_command, shell=True, stdout=subprocess.PIPE,).wait()
if proc != 0:
    print "Spark-submit failed! Check the code again."
    exit(1)


print "All tasks finished. Good luck!"
