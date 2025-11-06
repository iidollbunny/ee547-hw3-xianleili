#!/usr/bin/env bash
set -euo pipefail

if [ $# -ne 2 ]; then
  echo "Usage: $0 <key_file> <ec2_public_ip>" >&2
  exit 1
fi

KEY_FILE="$1"
EC2_IP="$2"

echo "Deploying to EC2 instance: $EC2_IP"

# Copy files
scp -i "$KEY_FILE" api_server.py ec2-user@"$EC2_IP":~
scp -i "$KEY_FILE" requirements.txt ec2-user@"$EC2_IP":~

# Install dependencies and start server
ssh -i "$KEY_FILE" ec2-user@"$EC2_IP" << 'EOF'
  set -e
  command -v python3 >/dev/null || sudo yum install -y python3 || { sudo apt-get update && sudo apt-get install -y python3; }
  command -v pip3 >/dev/null || curl -sS https://bootstrap.pypa.io/get-pip.py | sudo python3
  pip3 install -r requirements.txt
  pkill -f api_server.py || true
  nohup python3 api_server.py 8080 > server.log 2>&1 &
  echo "Server started. Test with: curl http://localhost:8080/papers/recent?category=cs.LG&limit=1"
EOF

echo "Deployment complete"
echo "Test with: curl http://$EC2_IP:8080/papers/recent?category=cs.LG&limit=5"
