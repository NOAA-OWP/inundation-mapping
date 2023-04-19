
import os
import numpy as np
from PIL import Image


# User inputs:
filepath_fim_output = "C:/projects/ca-overflooding/ca_overflooding_07" #"fim/output/filepath/here"
huc = "18030007"

## Iterate branch folders and read REM

# Branch folder path
branch_folder_path = os.path.join(filepath_fim_output, huc, "branches")

# Iterate through branch folders
for branch_id in os.listdir(branch_folder_path):
		rem_filename = "rem_" + branch_id + ".tif"
		rem_filepath = os.path.join(branch_folder_path, branch_id, rem_filename)
		if os.path.isfile(rem_filepath):
	            print(rem_filepath)

                rem_image = Image.open(rem_filepath)
                rem_array = np.array(rem_image)




