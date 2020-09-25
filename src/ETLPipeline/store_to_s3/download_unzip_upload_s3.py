import boto3
import botocore
import os
import multiprocessing
import subprocess
from config import bucket_name_xml_file, bucket_name_zipped_file,file_path,bucket_name_xml_file

# Use EC2 with memory larger than 500 GB

def call_unzip_subproc(url):
    command = os.getcwd() +"/unzip.sh " + url
    process_output = subprocess.call([command],shell=True)
    
def download(bucket_name,file_name,s3):
    if os.path.isfile('./tmp/'+file_name):
        print("download",file_name)
        s3.meta.client.download_file(bucket_name, file_name,'./tmp/'+file_name)

def upload(file_name,bucket_name,s3_xml):
    print("upload",file_name,bucket_name,s3_xml)
    s3_xml.meta.client.upload_file(file_name,bucket_name,file_name)                                 



def transfer(file_name,xml_file_name,url):                               
    print("File start transfering",file_name)
    # Download from s3

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name_xml_file)
    key = xml_file_name
    objs = list(bucket.objects.filter(Prefix=key))
    if any([w.key == key for w in objs]):
        print("Exists!")
    else:
        print("Doesn't exist")
        s3 = boto3.resource('s3')
        #download(bucket_name_zipped_file,file_name,s3)
        call_unzip_subproc(url)
        upload(xml_file_name,bucket_name_xml_file,s3_xml)                                 



    # remove file
    if os.path.isfile(xml_file_name):
        
        os.remove(xml_file_name)
        print("delete file",file_name)
    else:
        print("does not delete for ",xml_file_name)


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
            # Use apply to block process
            pool.apply(transfer,(file_name,xml_file_name,url))
    pool.close()
    pool.join()
