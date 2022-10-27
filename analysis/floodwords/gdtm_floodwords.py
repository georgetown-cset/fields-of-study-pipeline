#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 27 11:00:09 2022

@author: at1120
"""

#Import packages
from gensim import corpora
from gdtm.wrappers import TNDMallet
from gdtm.helpers.common import load_flat_dataset
import pickle 
import json

# Parameters
iterations = 1000
num_topics = 19

path_to_data = "/home/at1120/flood_words/en_abstracts/merged_en_abstract.csv"
tnd_path = "/home/at1120/topic-noise-models-source/mallet-tnd/bin/mallet"
out_file = "/home/at1120/flood_words/floodwords_{}_iter.json".format(iterations)


#load in dataset
dataset = load_flat_dataset(path_to_data, delimiter=' ')

# Format the data set for consumption by the wrapper (this is done automatically in class-based models)
dictionary = corpora.Dictionary(dataset)
dictionary.filter_extremes()
corpus = [dictionary.doc2bow(doc) for doc in dataset]
# Pass in the path to the java code along with the data set and parameters
model = TNDMallet(tnd_path, corpus, num_topics=num_topics, id2word=dictionary, 
                  skew=25, noise_words_max=200, iterations=iterations)

topics = model.get_topics()
noise = model.load_noise_dist()

with open(out_file, 'w') as f:
    json.dump(noise, f)