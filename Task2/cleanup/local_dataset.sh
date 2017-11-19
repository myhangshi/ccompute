#!/bin/bash

# data dir this folder needs to be available
dataset_dir=/data/T3

# dataset source dir
src_dir=/data/aviation/airline_ontime/2008

if [ -d $dataset_dir ]; then
   echo "Folder $dataset_dir exists, script continue ..."
   rm -rf $dataset_dir/*
else
   echo "Folder $dataset_dir does not exist, create the folder"
   mkdir $dataset_dir
fi


for src in /data/aviation/airline_ontime/* 
do 
   echo "Processing ${src} directory"  

   for my_file in ${src}/*
   do
       fname=$(basename "$my_file")
       base="${fname%.*}"    
       echo "${base} and ${my_file}"  
       unzip -p $my_file ${base}.csv | ./data_gen.py > ${dataset_dir}/${base}.txt
   done 

done 


echo 'Number of files cleaned:' 


