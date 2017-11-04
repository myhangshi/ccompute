#!/bin/bash

DBG=0

question=G1Q2

# dataset source dir
src_dir=/data/aviation/airline_ontime/*

# test dir
#src_dir=/home/ubuntu/avia_info/dataset1/*.zip

# HDFS output dir 
# this folder needs to be available on HDFS
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
rm -r $tmp_dir
mkdir $tmp_dir
cp data_gen.py $tmp_dir 
cd $tmp_dir

n=0
for year_dir in $src_dir
do
   year=$(basename "$year_dir")

   i=0

   for mon in `seq 1 12`
   #for file in $year_dir/*.zip
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
         #unzip -c $file On_Time_On_Time_Performance_${year}_*.csv | sed '1,3d' | gzip > ${base}.gz
         #unzip -p $file 349108460_T_T100D_MARKET_ALL_CARRIER_${year}_All.csv | ./data_gen.py > ${year}.txt
         #echo "trying on ${filename} filename ${file1} "

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
