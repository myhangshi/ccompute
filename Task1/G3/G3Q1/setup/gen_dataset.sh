#!/bin/bash

question=G3Q1

# source dir
src_dir=/data/aviation/air_carrier_statistics_ALL/t-100_domestic_market/*.zip

# test dir
#src_dir=/home/ubuntu/avia_info/dataset1/*.zip

# HDFS dataset dir 
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
rm -r $tmp_dir
mkdir $tmp_dir
cd $tmp_dir

n=0
for file in $src_dir
do
   let n=$n+1

   filename=$(basename "$file")
   year="${filename%.*}"

   unzip -c $file 349108460_T_T100D_MARKET_ALL_CARRIER_${year}_All.csv | sed '1,3d' | gzip > ${year}.gz

   pig -x local -param PIG_INPUT_FILE=${year}.gz -f ${CURR_DIR}/airport.pig > ${year}.txt

done

echo 'File processed:' $n
hdfs dfs -copyFromLocal ./*.txt $dataset_dir
hdfs dfs -ls $dataset_dir
echo 'Files copied to HDFS, ready to run mapreduce.' 
cd $CURR_DIR
