export AWS_ACCESS_KEY_ID=AKIA3KLMGFFOZQKIY4VF
export AWS_SECRET_ACCESS_KEY=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
aws s3 cp s3://aws-ffbucket/scripts/Teste1.py /home/ec2-user/python_code.py
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt update
apt list | grep python3.10
sudo apt install python3.10
python -m pip install --upgrade pip --user
python -m pip install pandas --user 
python -m pip install requests --user
python -m pip install datetime --user
python -m pip install numpy --user
python -m pip install boto3  --user
python /home/ec2-user/python_code.py