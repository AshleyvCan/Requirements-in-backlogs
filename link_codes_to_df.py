import codecs
import pandas as pd
import os
import re
import numpy as np
import string

def open_codes(file_name):
    with codecs.open(file_name,  'r', encoding="utf-16") as f:
        lines = f.read()
    return lines

def replace_column_char(df,char):
    for c in char:
        df = df.str.replace(c, '')
    return df


def check_dups(req,dups):
    if len(dups) > 0:
        for d in dups:
            if req in d: 
                print('dups: ', req)
                return True
    return False


# If a substring of the current requirement is already taken into account, make sure that it is not double counted
def check_substringdubs(df, req, dups, rows, col, count_type, code_type, codes):
    if req in dups:
        larger_dups = [c for c in codes if req in c and req != c]

        if len(larger_dups) > 0:
            larger_dups_i = []
            for d in larger_dups:
                larger_dups_i= larger_dups_i + list(df[df[col].notnull() & (df[col].str.match(d) | (df[col] == d))].index)
                
            for i in larger_dups_i:

                count_type[i] -= 1
    return count_type


# Increase the count of an item if a requirement is found
def count_item(rows, req, count_type, located_req, dups):
    if len(rows) > 0 and not str.isspace(req) and req != '':
        for row in rows:
            if row in count_type.keys() and not check_dups(req,set(dups)): #do not count if they are already in the duplicate list
                count_type[row] += 1
            elif row not in count_type.keys():
                count_type[row] = 1
        located_req = True
    if len(rows) > 1 and not str.isspace(req):
        dups.append(req)
        located_req = True

    return count_type, located_req, dups


def find_item_of_req(df, count_type_sum, count_type_desc,req, category, dups,  project_name, all_req_of_category):
    
        # Remove punctuations 
    req = re.sub(r'\s+', ' ', re.sub("[()+*/|?“”:<>]","", req.replace('\r', '')).replace('[','').replace(']','').replace('\\','').replace('_x000D_',''))
                            
        # Get item index of the req
    rows_desc = df[df['description'].notnull() & (df['description'].str.contains(req) | (df['description'] == req)) & ((df[category] == 1))].index
    rows_sum = df[df['summary'].notnull() & (df['summary'].str.contains(req) | (df['summary'] == req)) & ((df[category] == 1))].index
                
    located_req = False
    overlap_sum_desc = [i for i in rows_sum if i in rows_desc]

        # Count req in item 
    count_type_desc, located_req, dups = count_item(rows_desc, req, count_type_desc, located_req, dups)

    if not located_req or len(overlap_sum_desc)== 0:  count_type_sum, located_req, dups = count_item(rows_sum, req, count_type_sum, located_req, dups)    
    if len(overlap_sum_desc) >0: dups.append(req)

        # Extra check on duplications            
    count_type_sum = check_substringdubs(df, req, dups, rows_sum, 'summary', count_type_sum, category, all_req_of_category[project_name])
    count_type_desc = check_substringdubs(df, req, dups, rows_desc, 'description', count_type_desc, category, all_req_of_category[project_name])

    if not located_req and not str.isspace(req): print('ERROR: could not find:', req) 
    
    return count_type_sum, count_type_desc, dups

# Link all tags to the original samples
def link_req_to_df(project_names, categories_per_item, codes_2):
    df_all = pd.DataFrame()
    categories_per_item['id'] = categories_per_item['id'].astype(str)
    for project_name in project_names:
        df = pd.read_excel('Projects' + '/original_samples/'+ project_name + '.xlsx')
        df['project_name'] = len(df) * [project_name]


        df['id'] = df['id'].astype(str)
        
        df = df.merge(categories_per_item[categories_per_item['project_name'] == project_name].reset_index(), on = ['id', 'summary', 'description', 'issuetype', 'project_name'], how= 'left')
        print(df[df['id'] == '12990082'])
        dups = []
        df['summary'] = replace_column_char(df['summary'], '()+[]*/|:“”<>?').str.replace('\\','').str.replace('_x000D_','').str.replace(r'\s+', ' ', regex = True)
        df['description'] = replace_column_char(df['description'], '()+[]*/|:“”<>?').str.replace('\\','').str.replace('_x000D_','').str.replace(r'\s+', ' ', regex = True)
        
        count_type_per_project = {}
        for k, v in codes_2.items():
            count_type_desc = {} 
            count_type_sum = {}

            if project_name in v:
                for req in v[project_name]:
                    if not str.isspace(req) and req != '': 
                        count_type_sum, count_type_desc, dups= find_item_of_req(df,count_type_sum, count_type_desc, req, k, dups, project_name, v)
            count_type_per_project[k + '_' + 'desc'] = count_type_desc
            count_type_per_project[k + '_' + 'sum'] = count_type_sum
 
        df_counts = pd.DataFrame(count_type_per_project)

        df = df.join(df_counts).fillna(0)
        
        df_all = pd.concat([df_all, df])
        print(df.head())
    return df_all.reset_index(drop=True)

# Join the tags of the summary and description together.
def merge_sum_and_desc(df_all, codes_2):
    for k in codes_2.keys():
 
        df_all[k] = df_all[k +'_desc'] + df_all[k + '_sum']
        correcting_rows = df_all[(df_all[k + '_desc'] > 0) & (df_all[k+'_sum'] > 0)][[k + '_sum', k + '_desc']]
        if len(correcting_rows) > 0:
 
            for row in correcting_rows.iterrows():
                correct_value_col = row[1].idxmax()
                df_all.loc[row[0],k] =  df_all.loc[row[0], correct_value_col]

    return df_all

def export_file(df_all):
    df_all[[ 'id', 'issuetype', 'description', 'summary', 'created','project_name', 'high_user', 'high_system', 'high_nfr', 
            'medium_user', 'medium_system', 'medium_nfr', 'low_user', 'low_system', 'low_nfr']].to_excel('all_codes.xlsx')

def main(dir):

    # Read all raw labeled text
    codes = {'high_user': open_codes(dir+'high_user.txt').split('<Files\\')[1:],
            'high_system': open_codes(dir+'high_system.txt').split('<Files\\')[1:],
            'high_nfr': open_codes(dir+'high_nfr.txt').split('<Files\\')[1:],
            'medium_user': open_codes(dir+'medium_user.txt').split('<Files\\')[1:],
            'medium_system': open_codes(dir+'medium_system.txt').split('<Files\\')[1:],
            'medium_nfr': open_codes(dir+'medium_nfr.txt').split('<Files\\')[1:],
            'low_user': open_codes(dir+'low_user.txt').split('<Files\\')[1:],
            'low_system': open_codes(dir+'low_system.txt').split('<Files\\')[1:],
            'low_nfr': open_codes(dir+'low_nfr.txt').split('<Files\\')[1:],
            }

    # Extracting project names
    project_names = [codes['medium_user'][i].split('Reference')[0].split('> -')[0].replace('\\', '') for i in range(len(codes['medium_user']))]

    # For each category and file, place all independent tags in a dictionary 
    codes_2 = {k: {v[i].split('Reference')[0].split('> -')[0].replace('\\', ''): [ref.split('\r\n\r\n')[1].strip(' .,+!?/\"#*-')  for ref in v[i].split('Reference')[1:]]
                    for i in range(len(v))}
                    for k,v in codes.items()}
    

    categories_per_item = pd.read_excel('categories_per_item.xlsx')

    # Link all tags to the original samples
    df_all = link_req_to_df(project_names, categories_per_item, codes_2)

    # Join the tags of the summary and description together.
    df_all = merge_sum_and_desc(df_all, codes_2)

    export_file(df_all)

if __name__ == "__main__":
    dir_raw_tags = 'Tagged data/raw tags/OSS/'
    main(dir_raw_tags)
