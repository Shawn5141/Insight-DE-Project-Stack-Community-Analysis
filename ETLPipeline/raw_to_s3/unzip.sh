#!/bin/bash

url=$1
#echo $url

file_name=$(echo $url| rev|cut -d '/' -f 1 | rev )
file_type=$(echo $file_name | rev | cut -d '-' -f-1 | rev)

#echo Filename$file_name
#echo Filetype$file_type

7za e "./tmp/"$file_name 

#echo "Finished unzip"
#echo "Delete the files"
