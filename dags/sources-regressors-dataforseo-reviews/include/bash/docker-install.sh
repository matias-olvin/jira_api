sudo yum check-update
curl -fsSL https://get.docker.com/ | sh
sudo systemctl start docker
sudo systemctl enable docker
yes | sudo gcloud auth configure-docker europe-west1-docker.pkg.dev

sudo mkdir /home/data