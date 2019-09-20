import os
import boto3
import configparser

from datetime import date

config = configparser.ConfigParser()
config.read('config.cfg')

s3_bucket = config['S3']['S3_BUCKET']
s3_base_staging_dir = config['S3']['S3_BASE_STAGING_DIR']
s3_today_load_dir = ""

local_i94_dir = config['LOCAL_DATA_PATH']['I94_DATA_DIR']
local_iso_location_file = config['LOCAL_DATA_PATH']['ISO_LOC_FILE']
local_us_demography_file = config['LOCAL_DATA_PATH']['US_DEMO_FILE']


def create_staging_dir(s3):
    """ This function creates a new directoty inside staging directory for current day.
  
        Parameters: 
            s3: boto 3 client 
          
        Returns: 
            Nothing
     """
    
    global s3_today_load_dir
    s3_today_load_dir = "date=" + str(date.today())
    #s3.put_object(Bucket=s3_bucket, Key=("{}/{}/".format(s3_base_staging_dir, s3_today_load_dir)))
    s3_staging_dir = "s3://{}/{}/{}".format(s3_bucket, s3_base_staging_dir, s3_today_load_dir)
    print("Created a new dir under Staging for today's data: {} ".format(s3_staging_dir))
    

def stage_file(s3, local_file_path, dest_file_name):
    """ This function upload a local file to S3 Staging.
  
       Parameters: 
           s3: boto 3 client 
           local_file_path: Path of the local file
           dest_file_name: Destintation file name for the local file
         
       Returns: 
           Nothing
    """
    outputpathname = "{}/{}/{}".format(s3_base_staging_dir, s3_today_load_dir, dest_file_name)
    #s3.upload_file(local_file_path, s3_bucket, outputpathname)
    print("{} - {} - {} ".format(local_file_path,s3_bucket,dest_file_name))
    
    
def main():
    os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEYS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_ACCESS_KEYS']['AWS_SECRET_ACCESS_KEY']
    
    s3 = boto3.client('s3')
    
    create_staging_dir(s3)
    print ("H ->"+s3_today_load_dir)
    stage_file(s3, local_iso_location_file, "ISO_location_code_lookup.csv")
    stage_file(s3, local_us_demography_file, "US_demographics.csv")
    
    for filename in os.listdir(local_i94_dir):
        if filename.endswith(".sas7bdat"): 
            upload_file = os.path.join(local_i94_dir, filename)
            dest_file_name = "{}/{}".format(s3_base_staging_dir, s3_today_load_dir + "/i94_data/" + filename)
            stage_file(s3, upload_file, dest_file_name)
            continue
        else:
            continue

            
if __name__ == "__main__":
    main()
