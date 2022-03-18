#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb 22 23:04:35 2022

@author: at1120
"""


#IMPORT PACKAGES
import pandas as pd
from collections import OrderedDict


import csv
import json
from pathlib import Path

from nltk.metrics import masi_distance
from nltk.metrics.agreement import AnnotationTask

#fos_level1 = pd.read_csv("/Users/at1120/Documents/embeddings_evaluation/level1.csv")
#level1_dict = dict(zip(fos_level1.FieldofStudyID, fos_level1.NormalizedName))

#FUNCTIONS


#This function matches the field lists for a MAG article and CSET article
def match_field_lists(mag_article, cset_article):
    mag_article_dict = dict()
    cset_article_dict = dict()
    
    for i in range(len(mag_article["field"])):
        mag_article_dict[mag_article["field"][i]] = mag_article["score"][i]
    
    for i in range(len(cset_article["field"])):
        cset_article_dict[cset_article["field"][i]] = cset_article["score"][i]
    
    
    for field in mag_article["field"]:
        if field not in cset_article["field"]:
            cset_article_dict[field] = 0
            
            
    for field in cset_article["field"]:
        if field not in mag_article["field"]:
            mag_article_dict[field] = 0
            
    mag_df = pd.DataFrame.from_dict(mag_article_dict, orient="index")
    #mag_df.reset_index(inplace=True)
    mag_df.columns = ["mag_score"]
    
    cset_df = pd.DataFrame.from_dict(cset_article_dict, orient="index")
    #cset_df.reset_index(inplace=True)
    cset_df.columns = ["cset_score"]
    
        
    return mag_df, cset_df
    


#mag_dict = {"1234":{"field": ["computer science", "mathematics"], "score": [0.89, 0.4]}}
#cset_dict = {"1234":{"field": ["computer science", "physics"], "score": [0.95, 0.78]}}


mag_dict = {"1234":{"field": ["computer science", "mathematics"], "score": [0.5, 0]}}
cset_dict = {"1234":{"field": ["computer science", "physics"], "score": [0.5, 0]}}

field_annotation_agreement = dict()

for article_id in mag_dict.keys():
    x, y = match_field_lists(mag_dict[article_id], cset_dict[article_id])

    merged_df = x.join(y)

    data = []
    for idx, row in merged_df.iterrows():
        data.append(("mag", idx, row["mag_score"]))
        data.append(("cset", idx, row["cset_score"]))

    task = AnnotationTask(data=data)
    field_annotation_agreement[article_id] = round(task.alpha(), 2)
    