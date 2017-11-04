#!/bin/bash
DBG=0

group=G3Q2

# dataset source dir
src_dir=/data/aviation/airline_ontime/2008

dataset_dir=/data/${group}

# PIG data tmp output
pig_dir=${dataset_dir}/pigdata

CURR_DIR=${PWD}
tmp_dir=/tmp/${group}
rm -r $tmp_dir 
mkdir $tmp_dir
cd $tmp_dir

i=0
for file in $src_dir/*.zip
do
      echo "Processing $file"
      let i=$i+1

      filename=$(basename "$file")
      base="${filename%.*}"       

      
      # if it is good zip file then process
      unzip -l $file > /dev/null
      if  [ $? -eq 0 ]; then
         unzip -c $file ${base}.csv | sed '1,3d' | gzip > ${base}.gz
         echo "unzipping ${file}"
      else
         echo "File skipped: Incorrect file format ${file}" 
      fi
done

echo 'Number of files cleaned:' $i

#pig -x local -param PIG_IN_DIR=${dataset_tmp} -param PIG_OUT_DIR=${pig_dir} -f ${CURR_DIR}/route.pig 


cd ${CURR_DIR}

