import affine
from py2neo import Graph
import sys
from py2neo.matching import *
import pandas as pd
import editdistance
from py_stringmatching.similarity_measure import *
import math
import dimsim
import itertools
import re

# Need to work on the institution similar identification. Because the name strings are so similar, probably need to make regex to match the important information and then filter for that
# Defining constants that act as thresholds for the scores
EDITDISTANCE_THRESHOLD = 0.25       # want to be less than this
AFFINE_THRESHOLD = 0.80             # want to be greater than this
CHINESE_PHONETIC_THRESHOLD = 10     # want to be less than this

def is_valid_pinyin_or_chinese(name):
    """
    Checks if the given name is valid Chinese characters or Pinyin.
    """
    if not isinstance(name, str):
        return False
    for char in name:
        # Check if char is a Chinese character or a valid Pinyin letter
        # Adjust the condition based on valid Pinyin characters or tone marks if necessary
        if not ('\u4e00' <= char <= '\u9fff' or 'a' <= char.lower() <= 'z' or char in "āáǎàēéěèīíǐìōóǒòūúǔùǖǘǚǜü"):
            return False
    return True
# Compares two names in Chinese characters and returns true/false
def compare_two_names_chinese(name1, name2):
    # Using following package: https://github.com/IBM/MAX-Chinese-Phonetic-Similarity-Estimator 

    if not is_valid_pinyin_or_chinese(name1) or not is_valid_pinyin_or_chinese(name2):
        print(f"Invalid Pinyin or Chinese characters: '{name1}' or '{name2}'")
        return False
    if len(name1) == len(name2) and len(name2) > 0:
        # print(f"{name1} ** {name2}")
        hz_sim_score = dimsim.get_distance(name1, name2)   
        return hz_sim_score < CHINESE_PHONETIC_THRESHOLD
    return False
        
# Compares two names in a Western language
def compare_two_names_western(name1, name2):
    aff = affine.Affine()
  
    if len(name1) == 0 or len(name2) == 0:
        return False

    ed_score = 2.0 * editdistance.eval(name1, name2) / (len(name1) + len(name2))
    aff_score = aff.get_raw_score(name1, name2) / min(len(name1), len(name2))
    
    # print(f"{name1} ** {name2} = {ed_score} {aff_score}")

    # Update output
    return (ed_score < EDITDISTANCE_THRESHOLD) or (aff_score > AFFINE_THRESHOLD)
    


def compare_two_inst_entries(entry1, entry2):
    # Chinese names
    x = entry1.loc["chinese_name_hanzi"]
    x = "".join(re.findall(r'[\u4e00-\u9fff]+', x))
    y = entry2.loc["chinese_name_hanzi"]
    if x is None or y is None:
        result1 = False
    else:
        result1 = compare_two_names_chinese(x, y)
    
    # Western names
    x = entry1.loc["name_western"]
    y = entry2.loc["name_western"]
    if x is None or y is None:
        result2 = False
    else:
        result2 = compare_two_names_western(x, y)

    # #cor western name, cor chinese name, geography in chinese, geography in english
    # other_similars11 = ["c_name_western", "name_wes"]
    # result3 = False
    # for s in other_similars11:
    #     x = entry1.loc[s]
    #     y = entry2.loc[s]
    #     if compare_two_names_western(x, y) == True:
    #         result3 = True
    #         break
    #     else:
    #         continue
    
    # other_similars12 = ["c_chinese_name_hanzi", "name_zh"]
    # result4 = False
    # for s in other_similars12:
    #     x = entry1.loc[s]
    #     y = entry2.loc[s]
    #     if compare_two_names_chinese(x, y) == True:
    #         result4 = True
    #         break
    #     else:
    #         continue


    other_similars2 = ["c_name_western", "name_wes", "c_chinese_name_hanzi", "name_zh", "christian_tradition", "gender_served", "alternative_name_western", "nationality", "religious_family", "start_day", "start_month", "start_year", "end_day", "end_year", "end_month", ]
    result5 = False
    for s in other_similars2:
        x = entry1.loc[s]
        y = entry2.loc[s]
        if x == y:
            result5 = True
            break
        else:
            continue

    # Currently not doing anything with alternative names
    return ((result1 or result2) and (result5 == True))

def find_similar_institutions(graph):
    # Create dataframe with all institution names 
    q1 = '''MATCH (co:County)-[]-(i:Institution)-[]-(c:CorporateEntity) where i.institution_subcategory="School" or i.institution_subcategory="Blind School" 
    or i.institution_subcategory="Bible School" or i.institution_subcategory="Language School" 
    or i.institution_subcategory="Nursing School" or i.institution_subcategory="Catechetichl School" 
    RETURN i.id as id, i.institution_subcategory as institution_subcategory, i.name_western as name_western, i.chinese_name_hanzi as chinese_name_hanzi, i.christian_tradition as christian_tradition, i.gender_served as gender_served, i.alternative_name_western as alternative_name_western, i.nationality as nationality, i.religious_family as religious_family, i.start_day as start_day, i.start_month as start_month, i.start_year as start_year, i.end_day as end_day, i.end_year as end_year, i.end_month as end_month, c.name_western as c_name_western, c.chinese_name_hanzi as c_chinese_name_hanzi, co.name_zh as name_zh, co.name_wes as name_wes
    '''
    
    df = graph.run(q1)
    df = df.to_data_frame().set_index(["id"])
    df = df.fillna("")
    comparison_results = []
    seen_pairs = set()
    # Compare all pairs of people names
    for id1, entry1 in df.iterrows():
        for id2, entry2 in df.iterrows():
            if id1 == id2: continue
            similar = compare_two_inst_entries(entry1, entry2)
            if similar:
                name1 = entry1.loc["name_western"]
                name2 = entry2.loc['name_western']
                #similars = ["christian_tradition", "gender_served", "alternative_name_western", "nationality", "religious_family", "start_day", "start_month", "start_year", "end_day", "end_year", "end_month", "c_name_western", "c_chinese_name_hanzi", "name_zh", "name_wes"]
                pair = tuple(sorted([id1, id2]))
        
        # Check if the pair has already been seen
                if pair in seen_pairs:
                    continue
        
        # Add the pair to the set of seen pairs
                seen_pairs.add(pair)
                #df['duplicate_temp_id'] = []
                comparison_results.append({
            "ID1": id1,
            "Name1": name1,
            "More Info1": "https://data.chcdatabase.com/search?nodeSelect="+id1,
            "ID2": id2,
            "Name2": name2,
            "More Info2": "https://data.chcdatabase.com/search?nodeSelect="+id2
           })

# Convert the list of results to a DataFrame
  #  df.to_csv('updated_file.csv', index=False)
    results_df = pd.DataFrame(comparison_results)

# Save the DataFrame to an Excel file
    results_df.to_excel("duplicate_institutions.xlsx", index=False)

    print("Comparison results saved to 'comparison_results.xlsx'")

 

def main():
    # Connect to the database
    print("hello")
    graph = Graph("neo4j@bolt://localhost:7687", auth=("neo4j", "chcd1234"))
    find_similar_institutions(graph)
    
if __name__ == "__main__":
    main()