#!/bin/bash

question=G1Q1

# dataset source dir
src_dir=/data/aviation/airline_ontime/*
# HDFS output dir 
dataset_dir=/dataset_T1/${question}

hdfs dfs -test -d $dataset_dir
if [ $? == 0 ]; then
   echo "HDFS folder $dataset_dir exists, script continue ..."
   hdfs dfs -rm $dataset_dir/*
else
   echo "HDFS folder $dataset_dir does not exist, create the folder"
   hdfs dfs -mkdir $dataset_dir
fi

CURR_DIR=${PWD}
tmp_dir=/tmp/${question}
pig_dir=${tmp_dir}/pigdata
if [ -d $tmp_dir ]; then 
   rm -r $tmp_dir
fi 
mkdir $tmp_dir
cp data_gen.py $tmp_dir 
cd $tmp_dir

n=0
for year_dir in $src_dir
do
   year=$(basename "$year_dir")

   i=0

   for mon in `seq 1 12`
      do
      file1="On_Time_On_Time_Performance_${year}_${mon}"
      echo "Processing $file1"
      let n=$n+1
      let i=$i+1

      filename="${year_dir}/${file1}.zip"
      base="${filename%.*}"       

      unzip -l ${filename} > /dev/null
      status=$?
      if [ $status == 0 ]; then
         unzip -p ${filename} ${file1}.csv | ./data_gen.py > ${year}_${mon}.txt
      else
         echo "File skipped ${file1}: Incorrect format"
      fi
   done
done

echo 'Number of files cleaned:' $n
hdfs dfs -copyFromLocal ./*.txt $dataset_dir
hdfs dfs -ls $dataset_dir

echo 'Files copied to HDFS, ready to run mapreduce.' 
cd $CURR_DIR
