import os
import errno
import boto3
import zipfile
import datetime

###################################################################################################################################################
# This Python code :                                                                                                                              # 
#         1. downloads files from S3 bucket to a local folder in EC2                                                                              #
#         2. unzips the files                                                                                                                     #
#         3. uploads the files to folder based on file date to raw                                                                                #
#         4. uploads to prod data lake folder                                                                                                     #
#                                                                                                                                                 #
#    Author                      Date                  Description                                                                                #
#                                                                                                                                                 #
#   Raju Pillai                 2022-03-18               Created                                                                                  #
###################################################################################################################################################



###################################################################################################################################################
##                                                          Setting up variables                                                                 ##
###################################################################################################################################################
client = boto3.client('s3')

stage_s3_bucket = 'private-datalake-stage'
stage_s3_folder = '/mis/stage'
local_destination_folder = '/data/mis/data/'
zip_file_extension = '.zip'
today=datetime.date.today()-datetime.timedelta(days=0)
raw_s3_bucket = 'private-datalake-raw'
raw_s3_folder = '/mis/file_date='+str(today)+'/'
prod_s3_bucket = 'private-datalake-prod'
prod_s3_folder = '/data/mis/'

###################################################################################################################################################
##                                                          Define Functions                                                                     ##
###################################################################################################################################################
def assert_dir_exists(path):
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise


def download_dir(bucket, path, target):
    # Handle missing / at end of prefix
    if not path.endswith('/'):
        path += '/'

    print("###################################################################################")
    paginator = client.get_paginator('list_objects_v2')
    for result in paginator.paginate(Bucket=bucket, Prefix=path):
        # Download each file individually
        for key in result['Contents']:
            # Calculate relative path
            rel_path = key['Key'][len(path):]
            # Skip paths ending in /
            if not key['Key'].endswith('/'):
                local_file_path = os.path.join(target, rel_path)
                # Make sure directories exist
                local_file_dir = os.path.dirname(local_file_path)
                assert_dir_exists(local_file_dir)
                print ("Downloading files:" + key['Key']+" to " + local_destination_folder)
                client.download_file(bucket, key['Key'], local_file_path)

def upload_to_aws(local_file, target_s3_bucket, target_s3_file_name):
    
    try:
        client.upload_file(local_file, target_s3_bucket, target_s3_file_name)
        print("Upload Successful")
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False
    

def delete_all_files_from_s3_folder(target_s3_bucket, target_s3_folder):
    # Handle missing / at end of prefix
    if not target_s3_folder.endswith('/'):
        target_s3_folder += '/'
    
    print("###################################################################################")
    paginator = client.get_paginator('list_objects_v2')
    for result in paginator.paginate(Bucket=target_s3_bucket, Prefix=target_s3_folder):
        # Download each file individually
        for key in result['Contents']:
            # Calculate relative path
            rel_path = key['Key'][len(target_s3_folder):]
            # Skip paths ending in /
            if not key['Key'].endswith('/'):
                print ("Files thats going to be deleted :" + key['Key'])
                client.delete_object(Bucket=target_s3_bucket,Key=key['Key'])
                
    
def unzip_local_files(local_destination_folder,zip_file_extension):
    print("###################################################################################")
    os.chdir(local_destination_folder)
    for files in os.listdir(local_destination_folder):     # loop through items in dir
        if files.endswith(zip_file_extension):       # check for ".zip" extension
            file_name = os.path.abspath(files)       # get full path of files
            print("Unzipping "+file_name)
            zip_ref = zipfile.ZipFile(file_name)     # create zipfile object
            zip_ref.extractall(local_destination_folder)   # extract file to dir
            zip_ref.close()                          # close file
            os.remove(file_name)                     # delete zipped file

            
def list_files_to_upload_to_aws(local_destination_folder,s3_bucket,s3_folder):
    print("###################################################################################")
    os.chdir(local_destination_folder)
    for local_files in os.listdir(local_destination_folder):  
        s3_file_name = s3_folder+local_files
        print ("Uploading "+local_files+" to "+s3_file_name)
        upload_to_aws(local_files,s3_bucket,s3_file_name)


def delete_all_local_file(local_destination_folder):
    print("###################################################################################")
    os.chdir(local_destination_folder)
    for local_files in os.listdir(local_destination_folder):  
        print("Deleting "+local_files+" from "+local_destination_folder)
        os.remove(local_files)


###################################################################################################################################################
##                                                          Execute Functions                                                                    ##
###################################################################################################################################################
if __name__ == "__main__":
    print("Starting MIS BCD extract, unzip and load process at "+str(today))
    #delete all files from the local directory
    print("Deleting files from local storage")
    delete_all_local_file(local_destination_folder)
    # download data from stage to local
    print("Downloading data from stage S3 bucket")
    download_dir(stage_s3_bucket, stage_s3_folder, local_destination_folder)    
    # unzip the downloaded file
    print("Unzipping downloaded files with .zip extension")
    unzip_local_files(local_destination_folder,zip_file_extension)
    # upload the unzipped file to raw s3 folder with file_date as subfolder
    print("Uploading unzipped files to RAW S3 bucket with file date as folder")
    list_files_to_upload_to_aws(local_destination_folder,raw_s3_bucket,raw_s3_folder)
    #delete files on the prod folder
    print("Deleting files in prod S3 before copying the new files")
    delete_all_files_from_s3_folder(prod_s3_bucket,prod_s3_folder)
    # upload the unzipped file to prod s3 folder
    print("Uploading the unzipped files to Prod S3 folder")
    list_files_to_upload_to_aws(local_destination_folder,prod_s3_bucket,prod_s3_folder)
    #delete files on the stage folder
    print("Deleting files from stage S3 bucket to be ready for the next extract from RecoverPro")
    delete_all_files_from_s3_folder(stage_s3_bucket,stage_s3_folder)
    print("Finished MIS BCD extract, unzip and load process at "+str(datetime.date.today()-datetime.timedelta(days=0)))
