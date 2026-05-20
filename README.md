git clone https://github.com/p-unix/gcp-org-visualization.git  
cd gcp-org-visualization  
python3 -m venv .venv  
source .venv/bin/activate  
gcloud auth login   
pip3 install -r requirements.txt  
python3 main.py  
Open http://localhost:8080/ in your local browser and enter GCP Organization ID in the text box.  
