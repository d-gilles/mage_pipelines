cd ..

apt-get update

rm -rf src 
git clone https://github.com/d-gilles/mage_pipelines.git
mv mage_pipelines src

cd scr
pip install --upgrade pip
pip install -r requirements.txt
