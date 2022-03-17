#IMPORT PACKAGES
import pandas as pd
from nltk.metrics.agreement import AnnotationTask



mag_csv = "/Users/atoney/Documents/annotation_task/mag_level0_1.csv"
mag_sample = "/Users/atoney/Documents/annotation_task/mag_sample.json"
cset_sample = "/Users/atoney/Documents/annotation_task/cset_sample.json"

fos_id_name_level = pd.read_csv(mag_csv, header = 0)

level0 = fos_id_name_level.loc[(fos_id_name_level['level'] == 0)]
level1 = fos_id_name_level.loc[(fos_id_name_level['level'] == 1)]


mag_df = pd.read_json(mag_sample, orient='records')
cset_df = pd.read_json(cset_sample, orient='records')
                                
                                

#FUNCTIONS

def get_document_dictionary(document_data, level0_df = level0, level1_df = level1, level = 0):
    if level == 0:
        level_names = list(level0_df.name)
    elif level == 1:
        level_names = list(level1_df.name)
    else:
        print("incorrect level")
        
    ids = [document_data[i]['name'] for i in range(0, len(document_data)) if document_data[i]['name'] in level_names]
    scores = [float(document_data[i]['score']) for i in range(0, len(document_data)) if document_data[i]['name'] in level_names]    
    
    print("Getting data for level {} fields".format(level))
    return {"field": ids}, {"score": scores}
        

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

    mag_df.columns = ["mag_score"]
    
    cset_df = pd.DataFrame.from_dict(cset_article_dict, orient="index")

    cset_df.columns = ["cset_score"]
    
        
    return mag_df, cset_df
    


article_ids = list(mag_df.merged_id)
mag_dict_level0 = dict()
cset_dict_level0 = dict()
for merged_id in article_ids:
    mag_idx = mag_df.index[mag_df['merged_id'] == merged_id]
    cset_idx = cset_df.index[cset_df['merged_id'] == merged_id]
    try:
        mag_field_dict, mag_score_dict = get_document_dictionary(mag_df.iloc[mag_idx[0],0], level=0)
        mag_dict_level0[merged_id] = {**mag_field_dict, **mag_score_dict}
    
        cset_field_dict, cset_score_dict = get_document_dictionary(cset_df.iloc[cset_idx[0],0], level=0)
        cset_dict_level0[merged_id] = {**cset_field_dict, **cset_score_dict}
    except Exception:
        pass

        print("ignored the exception")
    


field_annotation_agreement = dict()


for article_id in cset_dict_level0.keys():
    x, y = match_field_lists(mag_dict_level0[article_id], cset_dict_level0[article_id])

    merged_df = x.join(y)

    data = []
    for idx, row in merged_df.iterrows():
        data.append(("mag", idx, row["mag_score"]))
        data.append(("cset", idx, row["cset_score"]))

    task = AnnotationTask(data=data)
    field_annotation_agreement[article_id] = round(task.alpha(), 2)
