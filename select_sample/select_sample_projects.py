import pandas as pd
import numpy as np
import datetime
from dateutil.relativedelta import relativedelta
import random
import sys

def DFS(adj_list, k, visited, component, parent, cycle):
    visited[k] = True
    component.append(k)

    for v in adj_list[k]:
        if visited[v] == False:
            DFS(adj_list, v, visited, component, k, cycle)
        elif v != parent:
            cycle = True
    
    return component, cycle

# Depth First Search
def get_components(adj_list):
    visited = {k: False for k in adj_list.keys()}
    components = []
    component_dict = {}
    include_cycle = []
    clusters = {}

    for i, k in enumerate(adj_list.keys()):
        if visited[k] == False:
            component = DFS(adj_list, k, visited, [], -1, False)
            components.append(component[0])
            include_cycle.append(component[1])
            for c in component[0]:
                component_dict[c] = component[0]
                clusters[c] = i

    return components, include_cycle, component_dict, clusters

# create adjacency list
def get_adj_list(df, colnames):
    col_id = colnames['col_id']
    col_target = colnames['col_target_y'] 
    adj_list = {i: list(df[(df[col_id] == i) & (~df[col_target].isna())][col_target]) for i in df[(~df[col_target].isna()) & (~df[col_id].isna())][col_id].unique()}
    return adj_list

def select_project(df, project_x, colnames):
    df_x = df[(df[colnames['col_project']] == project_x)]

    cross_project = list(set(list(df_x[(~df_x[colnames['col_target_y']].isna())][colnames['col_target_y']].unique())) - set(list(df_x[colnames['col_id']].unique())))

    df_x.loc[(df[colnames['col_target_y']].isin(cross_project)), colnames['col_target_y']] = np.nan
    return df_x

# Select a sample of 100 items (or based on a time period)
def select_period(df, df_x, period_length, colnames, select_months = True, is_tawos = True):

    if is_tawos:
        df_x[colnames['col_created']] = pd.to_datetime(df_x[colnames['col_created']], format='%Y-%m-%d %H:%M:%S')
    elif df[colnames['col_created']].dtype == 'object':
        df_x[colnames['col_created']] = pd.to_datetime(df_x[colnames['col_created']].transform(lambda x: x[:-9]), format='%Y-%m-%dT%H:%M:%S')
    else:
        df_x[colnames['col_created']] = pd.to_datetime(df_x[colnames['col_created']], unit='ms')
    
    if select_months:
        return select_timeperiod(df_x, period_length, colnames['col_created'], is_tawos)
    else:
        
        unique_items = df_x[[colnames['col_id'],colnames['col_created']]].drop_duplicates().sort_values(by = [colnames['col_created']]).reset_index()
        
        random_id = unique_items[0:-(period_length)].sample(axis='rows').index[0]
        
        select_ids = unique_items.loc[random_id:(random_id + (period_length-1))][colnames['col_id']]
        end_date = max(df_x.loc[df_x[colnames['col_id']].isin(select_ids), colnames['col_created']]) 
        return df_x[df_x[colnames['col_id']].isin(select_ids)], end_date

# Optional if it is desired to sample using a time interval
def select_timeperiod(df_x, period_length, colname_created, is_tawos):
    end_date = max(df_x[colname_created])- relativedelta(months=period_length)

    start_period_int = datetime.timedelta(days =random.randint(0,(end_date - min(df_x[colname_created])).days))

    start_date = min(df_x[colname_created]) + start_period_int 
    end_date = start_date + relativedelta(months=period_length)
    return df_x[(df_x[colname_created] > start_date) & (df_x[colname_created] < end_date)]


def include_linked_issues(df_x, df_x_subset, colnames):
    df_hac_components = get_components(get_adj_list(df_x, colnames))
    linked_issues_x = list(set(df_hac_components[2].keys()) & set(df_x_subset[colnames['col_id']].unique()))
    extra_issues = []

    for issue in linked_issues_x:
        extra_issues.extend(df_hac_components[2][issue])
    
    extra_issues = list(set(extra_issues) - set(df_x_subset[colnames['col_id']].unique()))
    df_x_subset =  pd.concat([df_x_subset, 
                    df_x[df_x[colnames['col_id']].isin(extra_issues)]])
    df_x_subset['cluster'] = df_x_subset[colnames['col_id']].map(df_hac_components[3])
    return df_x_subset


def select_repo_project(df, project_name,colnames): 
    return select_project(df, project_name, colnames) 


# Select a sample and extract links between issues, if the dataset contains less than 100 items, no sample is extracted
def select_subset_project(df, colnames, project_name, is_tawos):
    df_project = select_repo_project(df[~df[colnames['col_type']].isin(['Bug', 'Defect'])], project_name, colnames)
    try:
        
        df_project_subset, end_date = select_period(df, df_project, 100, colnames, select_months = False, is_tawos= is_tawos)

        df_project_subset = include_linked_issues(df_project, df_project_subset, colnames)

        df_project_subset = df_project_subset[df_project_subset[colnames['col_created']] <= end_date]
        file_name = str(project_name).replace(' ', '_').replace('/', '_')
        df_project_subset[df_project_subset[colnames['col_type']] != 'Bug'][[colnames['col_id'],colnames['col_type'], colnames['col_desc'],colnames['col_title'],colnames['col_created'], 'cluster']].drop_duplicates().to_excel('samples/'+file_name+'.xlsx')
        print('sample of ', project_name)
    except:
        print(str(project_name) + ' does not have 100 or more unqiue issues (excluding bugs)')

# Select a sample for each project.
def create_subset_projects(df, project_names, colnames,  is_tawos):
    for project_name in project_names:
        if len(df[df[colnames['col_project']] == project_name]) > 0: 
            select_subset_project(df, colnames, project_name, is_tawos)


def main(arg_tawos):
    if arg_tawos == '1': is_tawos= True
    else: is_tawos= False

    if is_tawos:
        df = pd.read_csv('convert_oss/TAWOS/issues_tawos.csv', encoding = "ISO-8859-1" ,sep= ';')
        df_projects = pd.read_excel('possible_projects.xlsx', sheet_name = 'TAWOS')
        colnames = dict(
        col_created = 'Creation_Date',
        col_id = 'ID',
        col_title = 'Title',
        col_desc = 'Description',
        col_type = 'Type',
        col_target_y = 'Target_Issue_ID',
        col_project = 'Project_ID'
        )

    else:
        # run convert_oss_json script to extract the file 'issues_Jira.json' 
        df = pd.read_json('issues_Jira.json')
        df.pop('index')

        df_projects = pd.read_excel('possible_projects.xlsx', index_col=[0], sheet_name = 'Public Jira Dataset')

        colnames = dict(
        col_created = 'created',
        col_id = 'id',
        col_title = 'summary',
        col_desc = 'description',
        col_type = 'issuetype',
        col_target_y = 'ID_y',
        col_project = 'project_name' 
        )
        

    
    project_names = df_projects[df_projects.columns[0]] 
    create_subset_projects(df, project_names, colnames,  is_tawos)


if __name__ == "__main__":
    arg_tawos = sys.argv[1]
    print(arg_tawos)
    main(arg_tawos)
    
  
