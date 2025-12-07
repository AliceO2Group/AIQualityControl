import json 
import pandas as pd 
import os 
import math, os, sys, re
try:
    import ROOT 
except Exception as e:
    raise SystemExit("PyROOT import failed. Make sure your kernel uses the env with ROOT installed.\n" + str(e))

from array import array
import numpy as np
from tqdm.auto import tqdm 
import shutil 
import logging 

from utils import load_json_file_into_df, load_quality_summ_from_root_objects, config_logger, plot_qs_cluster_per_run_number


logger = config_logger(output_file="output.log")

def filter_mo_based_on_quality_summaries(BASE_PATH, qcdb_mo_json_data_REL_PATH, bkkp_json_data_REL_PATH, qcdb_qs_mo_json_data_REL_PATH, qual_val_pairs, dst): 
    # metadata regards the versions of the objects 
    # an object is a path (ex. qc/TPC/MO/Clusters/c_Sides_N_Clusters), a version is one item in that path
    # mo could be an occupancy map, a cluster map, a graph etc. 
    mo_metadata = load_json_file_into_df(os.path.join(os.getcwd(), qcdb_mo_json_data_REL_PATH))    
    bkkp_filtered_runs =  load_json_file_into_df(os.path.join(os.getcwd(), bkkp_json_data_REL_PATH))   
    quality_summ_mo_metadata = load_json_file_into_df(os.path.join(os.getcwd(), qcdb_qs_mo_json_data_REL_PATH))    

    # Filter mo ex. cluster metadata based on wanted filters ex. the good runs loaded from book-keeping
        # Ensure same dtypes
    mo_metadata['RunNumber_int64'] = mo_metadata['RunNumber'].astype("int64") 
    quality_summ_mo_metadata['RunNumber_int64'] = mo_metadata['RunNumber'].astype("int64") 

    quality_summ_mo_metadata['RunNumber_int64'] = quality_summ_mo_metadata['RunNumber'].astype("int64") 
    bkkp_filtered_runs['runNumber_int64'] = bkkp_filtered_runs['runNumber']

        # Keep the MOs that exist in book-keeping 
    mo_metadata_bbkp_filtered = ( mo_metadata.loc[   mo_metadata['RunNumber_int64'].isin(bkkp_filtered_runs['runNumber_int64'])   ]
                                                .reset_index(drop=True) ) 

        # statistics to keep in mind 
    precentage = int(len(mo_metadata_bbkp_filtered) * 100/ len(mo_metadata))
    logger.info(f"{len(mo_metadata_bbkp_filtered)}/{len(mo_metadata)} or {precentage}% of cluster data (versions) are from the runs based on book-keeping filtering.")
    logger.info(f"The total number of runs taken into consideration from api bkkp limit --> {len(bkkp_filtered_runs)}")

    # Filter the mo ex. clusters FURTHER by a quality metric of the quality summaries loaded from qcdb 
    quality_dict = load_quality_summ_from_root_objects(filepath_of_root_objects= os.path.join(os.getcwd(), "qcdb_data/qc/TPC/MO/Q_O_physics/QualitySummary/"))

        # Chosen quality metric ex. Raw Occupancy quality is "Good"
    filtered_quality_summ_obj_names = [
        key 
        for key, value in quality_dict.items()
        if (
            not qual_val_pairs
            or all(value.get(q) == exp for q, exp in qual_val_pairs)
        )
    ]
        # stats
    logger.info(  f"{len(filtered_quality_summ_obj_names)}/{len(quality_dict)} quality summaries have the desired quality metrics {qual_val_pairs}")

        # However, the Root names are not the same across MOs! 
            # --> We need to correlate these objects with their corresponding Run Number and Creation Time (found on the object metadata)
    mask_metric = quality_summ_mo_metadata["fileName"].isin(filtered_quality_summ_obj_names)
    mask_runs = quality_summ_mo_metadata["RunNumber_int64"].isin(bkkp_filtered_runs["runNumber_int64"])


    quality_summ_mo_metadata_filtered = (
        quality_summ_mo_metadata[mask_metric & mask_runs]
        .reset_index(drop=True)
    )
        # stats
    logger.info(f"{len(quality_summ_mo_metadata_filtered)}/{len(quality_summ_mo_metadata)} or {int(len(quality_summ_mo_metadata_filtered) * 100 /len(quality_summ_mo_metadata))}% of quality summary objects have the specified filters.")

    ### CONCATENATE (put one on top of the other) the cluster data with the good quality summaries 
    # The only column they have in common is the 'RunNumber', so we group by it and then compare the creation times of the objects

    concat_mo_with_qsum_mo = pd.concat([mo_metadata_bbkp_filtered, quality_summ_mo_metadata_filtered], ignore_index=True)

    common_runs = set(mo_metadata_bbkp_filtered["RunNumber_int64"]).intersection(quality_summ_mo_metadata_filtered["RunNumber_int64"])
    logger.info(f"Number of commons runs between the mo and the quality summaries: {len(common_runs)}, Number of unique runs in mo data {len(set(mo_metadata_bbkp_filtered["RunNumber_int64"]))}, Number of unique runs in quality summaries {len(set(quality_summ_mo_metadata_filtered["RunNumber"]))}")

    concat_mo_with_qsum_mo = concat_mo_with_qsum_mo[ concat_mo_with_qsum_mo['RunNumber_int64'].isin(common_runs) ] 

    concat_mo_with_qsum_mo_group_by_run_number = concat_mo_with_qsum_mo.groupby("RunNumber_int64")
    n_groups = len(concat_mo_with_qsum_mo_group_by_run_number)
        
    # Create destination folder for the objects to keep
    os.makedirs(dst, exist_ok=True)

    global_idxs_saved = set()

    for _, group_df in tqdm(iterable=concat_mo_with_qsum_mo_group_by_run_number, total = n_groups, desc="Processing run groups"):
        
        mo_path = os.path.join(os.path.dirname(qcdb_mo_json_data_REL_PATH).replace("qcdb_data/", ""),
                                os.path.splitext(os.path.basename(qcdb_mo_json_data_REL_PATH))[0])

        qsum_path =os.path.join(os.path.dirname(qcdb_qs_mo_json_data_REL_PATH).replace("qcdb_data/", ""),
                                os.path.splitext(os.path.basename(qcdb_qs_mo_json_data_REL_PATH))[0])
        
        num_mo_objects = group_df.path.value_counts()[mo_path]
        num_qsum_mo_objects = group_df.path.value_counts()[qsum_path] 
        
        if num_mo_objects >=1 and num_qsum_mo_objects >= 1: #and num_mo_objects>num_qsum_mo_objects:
                
                ref_times_series = group_df.loc[group_df["path"] == qsum_path, "createTime"]
                cl_times_series = group_df.loc[group_df["path"] == mo_path, "createTime"]
                
                for i in range(num_qsum_mo_objects): 
                    
                    # Allocate the reference time of the quality summary object
                    ref_time = ref_times_series.iloc[i] 
                    
                    # Compute the difference of it, in minutes, between all other objects from that run  
                    group_df[f"diff_min"] = (group_df["createTime"] - ref_time).abs() / 1000 / 60
                    
                    # Only keep MOs (ex.clusters) that are within 10 min of the qsum ref time 
                    mask_valid = (group_df["path"] == mo_path) & (group_df[f"diff_min"]  < 10)
                    valid_idxs = group_df.index[mask_valid]
                    
                    # # Convert the creation time from Unix ms to full datetime stamp
                    # group_df["createTime_dt"] = pd.to_datetime(group_df["createTime"], unit="ms")
                    # # To debug and plot the cluster and quality summary timestamps
                    # logger.info(group_df[["path", "RunNumber", "createTime_dt", f"diff_min"]])
                    # logger.info(valid_idxs)
                    # plot_qs_cluster_per_run_number(ref_times_series, cl_times_series, group_df["RunNumber"].iloc[0])
                    # input()
                    
                    for idx in [i for i in valid_idxs if i not in global_idxs_saved]:
                        global_idxs_saved.add(idx)
                        cls_filename = group_df.loc[idx, "fileName"]
                        # with corresponding filename: 
                        qsum_fname = group_df.loc[ref_times_series.index[i],"fileName"]
            
                        src = os.path.join(
                            BASE_PATH,
                            qcdb_mo_json_data_REL_PATH.removesuffix(".json"),
                            cls_filename,
                        )
                        shutil.copy(src, os.path.join(dst, cls_filename))

                    group_df.drop('diff_min', axis=1, inplace=True)

        else:
            logger.critical("Either qsum or clusters don't exist.")
            print(group_df[["path", "RunNumber", "createTime"]])
            sys.exit(True)
            
    logger.info(f"{len(global_idxs_saved)}/{len(mo_metadata_bbkp_filtered)} total files were kept in the dst folder: {dst}")
    
    
    
if __name__ == '__main__': 
    
    logger = config_logger(output_file="output.log")
    BASE_PATH = os.getcwd()
    qcdb_mo_json_data_REL_PATH = "qcdb_data/qc/TPC/MO/Clusters/c_Sides_N_Clusters.json"
    bkkp_json_data_REL_PATH = "bkkp_data/runs_stable_beams_with_good_tpc_quality.json"
    qcdb_qs_mo_json_data_REL_PATH = "qcdb_data/qc/TPC/MO/Q_O_physics/QualitySummary.json"
    dest_folder = os.path.join(BASE_PATH, os.path.dirname(qcdb_mo_json_data_REL_PATH), "filtered_clusters")

    # qual_val_pairs = [("Raw occupancy quality","Good"), ("Cluster occupancy quality","Bad")]
    filter_mo_based_on_quality_summaries(BASE_PATH, qcdb_mo_json_data_REL_PATH, bkkp_json_data_REL_PATH, qcdb_qs_mo_json_data_REL_PATH, qual_val_pairs=[], dst=dest_folder)
