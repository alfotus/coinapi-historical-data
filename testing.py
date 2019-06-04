import os
import pandas as pd
from shutil import copyfile

top_ten_folderpath=os.getcwd()+"/TOP10/"

if not os.path.exists(top_ten_folderpath):
	os.mkdir(top_ten_folderpath)

data=pd.read_csv("top_ten_pairing.csv")

for name in data.loc[:,"pairing_name"]:
	print(name)