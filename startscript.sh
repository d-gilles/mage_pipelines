cd ..

apt-get update && apt-get install -y git openssh-client
mkdir -p ~/.ssh
ssh-keyscan github.com >> ~/.ssh/known_hosts

rm -rf src 
git clone https://github.com/d-gilles/mage_pipelines.git
mv mage_pipelines src

cd scr
pip install --upgrade pip
pip install -r requirements.txt
