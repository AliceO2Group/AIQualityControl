import json 
import pandas as pd 
import os 
import shutil
from tqdm import tqdm 

def filter_cluster_versions_on_bkkp_runs(BASE_PATH, qcdb_json_data_REL_PATH, bkkp_json_data_REL_PATH, DEST_FILEPATH): 
    
    # Load the json files 
    with open(os.path.join(BASE_PATH, qcdb_json_data_REL_PATH), "r") as f: 
        qcdb_data = json.load(f)
        
    with open(os.path.join(BASE_PATH, bkkp_json_data_REL_PATH) , "r") as bkkpruns: 
        bkkp_good_runs = json.load(bkkpruns)
        
    # Convert to dataframes 
    bbkp_data_df = pd.json_normalize(bkkp_good_runs)
    qcdb_data_df = pd.json_normalize(qcdb_data)

    # Fix types 
    qcdb_data_df['RunNumber'] = qcdb_data_df['RunNumber'].astype("int64")

    # Keep onlyt the rows where the runnumber exists in the bkkp filtered dataframe 
    filtered_qcdb_objects = qcdb_data_df.loc[qcdb_data_df['RunNumber'].isin(bbkp_data_df['runNumber'])]
    filtered_qcdb_objects = filtered_qcdb_objects.reset_index(drop=True)

    # Create destination folder for the objects to keep
    os.makedirs(DEST_FILEPATH, exist_ok=True)
        
    print(f"{len(filtered_qcdb_objects)}/{len(qcdb_data_df)} objects will be copied to: \n{DEST_FILEPATH}")

    input()

    # Copy them to destination folder 
    for i, _ in tqdm(iterable=enumerate(filtered_qcdb_objects['RunNumber']), total = len(filtered_qcdb_objects))  : 
        
        qcdb_object_to_keep = filtered_qcdb_objects['fileName'][i]
        
        src = os.path.join(BASE_PATH, qcdb_json_data_REL_PATH.removesuffix('.json') , qcdb_object_to_keep)
        
        shutil.copy(src,os.path.join(DEST_FILEPATH, qcdb_object_to_keep))
        

if __name__ == '__main__': 
    
    BASE_PATH = os.getcwd()
    qcdb_json_data_REL_PATH = "qcdb_data/qc/TPC/MO/Clusters/c_Sides_N_Clusters.json"
    bkkp_json_data_REL_PATH = "bkkp_data/runs_stable_beams_with_good_tpc_quality.json"
    DEST_FILEPATH = os.path.join(BASE_PATH, os.path.dirname(qcdb_json_data_REL_PATH), "good_qc_filtered_clusters")

    filter_cluster_versions_on_bkkp_runs(BASE_PATH, qcdb_json_data_REL_PATH, bkkp_json_data_REL_PATH, DEST_FILEPATH)
