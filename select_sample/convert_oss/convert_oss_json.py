import json
import pandas as pd
import numpy as np
import os
import sys

def open_json(filename):
    with open(filename, 'r',  encoding="utf8") as f:
        file = json.load(f)
    return file

def get_project_df(file):
    project_info = [file[i]['fields']['project'] for i in range(len(file))]
    df_projects_all = pd.DataFrame(project_info)[['self', 'id', 'key', 'name', 'projectTypeKey', 'projectCategory']] #.drop_duplicates()
    df_projects_all['projectCategory'] = [pc['name'] if isinstance(pc, dict) else np.nan for pc in df_projects_all['projectCategory']] 

    return df_projects_all.drop_duplicates()

# retrieve all different links (epic, subtask, and general) for each issue
def get_link_df(file, file_epiclinks):
    epic_links = {file_epiclinks[i]['id']: item["items"][-1]['to'] for i in range(len(file_epiclinks)) for item in file_epiclinks[i]['changelog']['histories'] 
                  if len(item["items"]) >0 and item["items"][0]['field'] == 'Epic Link' and isinstance(item["items"][-1]['to'], str)} 

    issue_links = []
    for i in range(len(file)):
        
        p = []
        s = []

        # Retrieve subtaks links
        if "parent" in file[i]['fields']:
            p.append(file[i]['fields']["parent"]["id"])
            issue_links.append({'ID_x': file[i]['id'], 'link_name': 'Sub-task', 'link_type': 'is a child of', 'Direction': 'inwards', 'ID_y':p[0]})
            issue_links.append({'ID_x': p[0], 'link_name': 'Sub-task', 'link_type': 'is a parent of', 'Direction': 'outwards', 'ID_y':file[i]['id']})    
        
        # Retrieve epic links
        if file[i]['id'] in epic_links.keys():
            p.append(epic_links[file[i]['id']])
            issue_links.append({'ID_x': file[i]['id'], 'link_name': 'Epic link', 'link_type': 'is a child of', 'Direction': 'inwards', 'ID_y':p[-1]})
            issue_links.append({'ID_x': p[-1], 'link_name': 'Epic link', 'link_type': 'is a parent of', 'Direction': 'outwards', 'ID_y':file[i]['id']})

        # Retrieve other subtaks links
        if len(file[i]['fields']['subtasks']) > 0:  
            for stask in file[i]['fields']['subtasks']:
                s.append(stask['id'])
                issue_links.append({'ID_x': file[i]['id'], 'link_name': 'Subtask link', 'link_type': 'is a parent of', 'Direction': 'inwards', 'ID_y':stask['id']})
                issue_links.append({'ID_x': stask['id'], 'link_name': 'Subtask link', 'link_type': 'is a child of', 'Direction': 'outwards', 'ID_y':file[i]['id']}) 

        # Retrieve 'general' issue links
        for link in file[i]['fields']['issuelinks']:
            if link[list(link.keys())[-1]]['id'] not in (p + s): 
                issue_links.append({'ID_x': file[i]['id'], 'link_name': link['type']['name'],'link_type': link['type'][list(link.keys())[-1][:-5]],
                'Direction': list(link.keys())[-1][:-5] ,'ID_y': link[list(link.keys())[-1]]['id']})

    df_issue_links = pd.DataFrame(issue_links)
    df_issue_links['ID_y'] = pd.to_numeric(df_issue_links['ID_y'], errors='coerce') #.astype('float')

    return df_issue_links 
# rename key names and convert to dataframe
def create_df(file):

    issues_info = [{'id': issue['id'], 
    'issuetype' :issue['fields']['issuetype']['name'],
    'project_id' :issue['fields']['project']['id'],
    'project_name' :issue['fields']['project']['name'],
    'resolutiondate' :issue['fields']['resolutiondate'],
    'created' :issue['fields']['created'],
    'updated' :issue['fields']['updated'],
    'description' :issue['fields']['description'] if 'description' in issue['fields'].keys() else '',
    'summary' :issue['fields']['summary']
    }
    for issue in file]
    return pd.DataFrame(issues_info)

# create dataframe with all issues and issues links. 
def merge_df_issues_links(parent_dir, file_name):
    print(file_name)
    file = open_json(parent_dir +file_name + '.json')
    file_epiclinks = open_json(parent_dir + 'epic_links/' + file_name + '.epiclinks'+'.json')
    issue_df = create_df(file)
    link_df=get_link_df(file, file_epiclinks)
    
    return pd.merge(issue_df, link_df, left_on='id', right_on='ID_x', how = 'left')


# Merge repo datasets to one 
def merge_all_dfs(*dfs):
    all_dfs = pd.DataFrame()
    for name, df in dfs:
        
        df['repo_id'] = [name for j in range(len(df))]
        all_dfs = pd.concat([all_dfs,df])
    return all_dfs.reset_index()


def main():
    parent_dir = 'raw projects - Public Jira data/'
    df = merge_all_dfs(('SecondLife', merge_df_issues_links(parent_dir,'JiraRepos.SecondLife')), #('Apache',merge_df_issues_links('JiraRepos.Apache')),
    # ('Mindville',merge_df_issues_links('JiraRepos.Mindville')),
    # ('MariaDB',merge_df_issues_links('JiraRepos.MariaDB')), #('Jira',merge_df_issues_links('JiraRepos.Jira')),
    # ('JFrog',merge_df_issues_links('JiraRepos.JFrog')),
    # ('Spring',merge_df_issues_links('JiraRepos.Spring')),
    # ('Sonatype',merge_df_issues_links('JiraRepos.Sonatype')),
    # ('Sakai',merge_df_issues_links('JiraRepos.Sakai')),
    # ('RedHat',merge_df_issues_links('JiraRepos.RedHat')),
    # ('Qt',merge_df_issues_links('JiraRepos.Qt')),
    # ('MongoDB',merge_df_issues_links('JiraRepos.MongoDB')),
    # #('Mojang',merge_df_issues_links('JiraRepos.Mojang')),
    # ('JiraEcosystem',merge_df_issues_links('JiraRepos.JiraEcosystem')),
    ('IntelDAOS',merge_df_issues_links(parent_dir, 'JiraRepos.IntelDAOS')),
    ('Hyperledger',merge_df_issues_links(parent_dir, 'JiraRepos.Hyperledger'))
    )


    df.to_json('../issues_Jira.json')

if __name__ == "__main__":
    main()



