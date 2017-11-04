#!/bin/bash
DBG=0

question=G2

# dataset source dir
src_dir=/data/aviation/airline_ontime/*

# data dir this folder needs to be available
dataset_dir=/data/${question}

# tmp dir
dataset_tmp=/data/tmp_${question}

#PIG data tmp output
pig_dir=/data/pigdata
rm -rf $pig_dir

if [ -d $dataset_dir ]; then
   echo "Folder $dataset_dir exists, script continue ..."
   rm $dataset_dir/*
else
   echo "Folder $dataset_dir does not exist, create the folder"
   mkdir $dataset_dir
fi

if [ -d $dataset_tmp ]; then
   echo "Folder $dataset_tmp exists, script continue ..."
   rm $dataset_tmp/*.*
else
   echo "Folder $dataset_tmp does not exist, creating the folder..."
   mkdir $dataset_tmp
fi

CURR_DIR=${PWD}
tmp_dir=/tmp/${question}
rm -r $tmp_dir
mkdir $tmp_dir
cd $tmp_dir

n=0
for year_dir in $src_dir
do
   year=$(basename "$year_dir")

   i=0
   for file in $year_dir/*.zip
   do
      echo "Processing $file"
      let n=$n+1
      let i=$i+1

      filename=$(basename "$file")
      base="${filename%.*}"       

      # if it is good zip file then process
      unzip -l $file > /dev/null
      if  [ $? -eq 0 ]; then
         unzip -c $file ${base}.csv | sed '1,3d' | gzip > ${base}.gz
      else
         echo "File skipped: Incorrect file format ${file}" 
      fi

      # for sanity check
      if [ $DBG -eq 1 ] && [ $i -gt 1 ]; then
         echo "Sanity check: Reaching $i, exiting loop..."
         break
      fi
    
   done

   # for sanity check
   if [ $DBG -eq 1 ] && [ $n -gt 4 ]; then
      echo "Sanity check: Reaching $n, terminating..."
      break
   fi

done

echo 'Number of files cleaned:' $n

cp *.gz ${dataset_tmp}/.
pig -x local -param PIG_IN_DIR=${dataset_tmp} -param PIG_OUT_DIR=${pig_dir}\
       -f ${CURR_DIR}/ontimeperf.pig 


if [ -f ${pig_dir}/_SUCCESS ]; then
   echo 'Files processed successfully.' $n
   mv ${pig_dir}/part* ${dataset_dir} 
   echo 'Files generated in ' ${dataset_dir} 
else 
   echo 'Failed!'
fi

cd $CURR_DIR

