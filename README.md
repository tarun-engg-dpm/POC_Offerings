# POC_Offerings
1. move file to instance
scp -i ~/.ssh/onlyDev.pem -r /Users/User_1/CodeBase/PYTHON/offerings_lua ubuntu@0.0.0.0:/home/ubuntu/

2. Get into the Instance 
ssh -i ~/.ssh/onlyDev.pem ubuntu@0.0.0.0

3. Get into directory, Create venv and activate
cd offerings_lua
python -m venv test
source text/bin/activate

4. Download requirements
pip install -r requirements.txt

5.Run locust 
locust -f locust.py --web-host 0.0.0.0 --web-port 8090

6. web browser
http://<instance-private-ip>:8090

All set 
