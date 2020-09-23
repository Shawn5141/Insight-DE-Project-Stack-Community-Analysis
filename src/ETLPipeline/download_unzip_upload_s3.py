import boto3
import botocore
import os
import multiprocessing
import subprocess
from config import bucket_name_xml_file, bucket_name_zipped_file,file_path,bucket_name_xml_file

def transfer(file_name,xml_file_name,url):                               
    print("File start transfering",file_name)
    # Download from s3
    s3 = boto3.resource('s3')
    s3.meta.client.download_file(bucket_name_zipped_file, file_name,'./tmp/'+file_name)
    command = os.getcwd() +"/unzip.sh " + url
    #print("file call subprocess")
    # Unzip
    process_output = subprocess.call([command],shell=True)
    s3_xml = boto3.resource('s3')
    # Upload
    s3_xml.meta.client.upload_file(xml_file_name,bucket_name_xml_file,xml_file_name) 
    #print("File stop upload",file_name)
    # remove file
    os.remove(xml_file_name)
    print("delete file")


if __name__=="__main__":

    pool = multiprocessing.Pool(processes = 4 )
    url_list = []
    with open(file_path,'r') as file:
        for url in file:
            url_list.append(url)
    
    
    for url in url_list:
        url_name = url
        file_name=url_name.split('/')[-1][:-1] # get rid of \n
        xml_file_name="".join("".join(file_name.split('.')[-3:-1]).split('-')[-1])+".xml"
        if "stackoverflow.com-" in file_name:
            pool.apply_async(transfer,(file_name,xml_file_name,url))
    pool.close()
    pool.join()
