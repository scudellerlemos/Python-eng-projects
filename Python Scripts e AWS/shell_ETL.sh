export AWS_ACCESS_KEY_ID=%%%%%%%%%%%%
export AWS_SECRET_ACCESS_KEY=6%%%%%%%%%%%%%%%%%%%%
aws s3 cp s3://aws-ffbucket/scripts/ffxiv_etl.py /home/ec2-user/python_code.py
sudo yum update
y
sudo yum install python3
python3 -m pip install --upgrade pip --user
python3 -m pip install pandas --user 
python3 -m pip install requests --user
python3 -m pip install datetime --user
python3 -m pip install numpy --user
python3 -m pip install boto3  --user
python3 /home/ec2-user/python_code.py