import re
import numpy as np

from pgsql_async_client import get_pool
from data_retrieval import *
from mappings import *
from queries import pgsql_queries
import asyncio
# from data_preprocessing.helper_fns import query_builder
from cache import cache_manager

pg_client = PGSQLData()

async def l1_get_rawdata_cleaned():
    pool = await get_pool()
    pg_client.pool = pool
    query = pgsql_queries['retrieve_dashboard_data']
    df = await asyncio.gather(pg_client.execute_query(query))
    df = df[0]
    df = pd.DataFrame(df)

    df = df.query('`candidateName` != @dummy_data_employees').copy()

    df['dep_prefix'] = df['rollNo'].apply(lambda x:re.sub(r'(?=\d).*$', '', x))
    df = df.query('dep_prefix != @dummy_rollno_prefixes').copy()
    df['dep_prefix'] = df['dep_prefix'].map(deps_mapping)
    

    # df['Employee Code'] = df['rollNo'].apply(lambda x:"GE00"+re.sub(r"\D+", "", x))
    df['Employee Code'] = df['rollNo'].apply(lambda x: "GE00" + re.sub(r"\D+", "", x).lstrip('0') if re.sub(r"\D+", "", x) else "GE00")

    df['batch_suffix'] = df['rollNo'].apply(lambda x:re.sub(r'(?=\d).*$', '', x[::-1])[::-1])
    df['batch_suffix'] = df['batch_suffix'].map(rollno_suffix_mapping)
    return df

async def l1_get_userid_name_mapping():
    df = await cache_manager.get_or_fetch(l1_get_rawdata_cleaned)
    return df[['candidateName','Employee Code']].drop_duplicates().reset_index(drop=True)

async def l1_get_proper_dashboard_data_unprocessed():
    df = await cache_manager.get_or_fetch(l1_get_rawdata_cleaned)
    dep_wise_max_marks = df[['MaxPossibleScore','dep_prefix']].drop_duplicates().copy()
    df['courseName_cleaned'] = df["dep_prefix"].astype(str) + "_" + df["batch_suffix"].astype(str)
    df['courseName_cleaned_percent'] = df['courseName_cleaned'] + " %"
    
    score_cols = df.pivot(columns='courseName_cleaned', values='TotalCandidateScore').copy()
    percent_cols = df.pivot(columns='courseName_cleaned_percent', values='ScorePercentage').copy()
    df = df[[col for col in list(df.columns) if col not in ['dep_prefix','batch_suffix','courseName_cleaned','courseName','rollNo','examDate','TotalCandidateScore','ScorePercentage','courseName_cleaned_percent']]].copy()

    df = pd.concat([df,score_cols,percent_cols],axis=1).copy()

    df = df.groupby('candidateName').agg('first').reset_index()

    for dep in deps_mapping.values():
        df[f'{dep} Max Marks'] = dep_wise_max_marks.query("dep_prefix == @dep").MaxPossibleScore.iloc[0]
    for dep in deps_mapping.values():
        df[f'{dep} Final Score'] = df[[col for col in df.columns if col.startswith(f'{dep}_') and '%' not in col]].max(axis=1).reset_index(drop=True)

    for dep in deps_mapping.values():
        df[f'{dep} Final Score %'] = df[f'{dep} Final Score']/df[f'{dep} Max Marks'] * 100

    def pass_fail_fn(score_percent):
        if np.isnan(score_percent):
            return 'Pending'
        if score_percent == 0:
            return 'Not Attempted'
        if score_percent>=75.0:
            return 'Pass'
        else:
            return 'Fail'
    
    for dep in deps_mapping.values():
        cols = [f'{dep}_Attempt 1 %',f'{dep}_Attempt 2 %',f'{dep}_Attempt 3 %',f'{dep} Final Score %']
        status_cols = [f'{dep} Attempt 1 Status',f'{dep} Attempt 2 Status',f'{dep} Attempt 3 Status',f'{dep} Final Status']
        for col,status_col in zip(cols,status_cols):
            if col in df.columns:
                df[status_col] = df[col].map(pass_fail_fn)
            else:
                df[col] = None
                df[col.replace('%','').strip()] = None
                df[status_col] = 'Pending'
        

    new_order = ['candidateName','Employee Code','hallName']

    # Add department columns in order
    columns = df.columns.tolist()
    for dep in deps_mapping.values():
        # Collect all columns for this department
        dept_cols = {
            'attempts': [],
            'max_marks': None,
            'final_score': None,
            'final_score_pct': None,
            'status' : None
        }

        for col in columns:
            # Match attempt patterns
            match = re.match(rf'^{dep}_Attempt (\d+)$', col)
            if match:
                attempt_num = int(match.group(1))
                dept_cols['attempts'].append((attempt_num, col, None))

            match = re.match(rf'^{dep}_Attempt (\d+) %$', col)
            if match:
                attempt_num = int(match.group(1))
                # Find the corresponding attempt entry and update it
                for i, (num, att_col, pct_col) in enumerate(dept_cols['attempts']):
                    if num == attempt_num:
                        dept_cols['attempts'][i] = (num, att_col, col)
                        break
                else:
                    dept_cols['attempts'].append((attempt_num, None, col))

            # Match Max Marks, Final Score, and Final Score %
            if col == f'{dep} Max Marks':
                dept_cols['max_marks'] = col
            if col == f'{dep} Final Score':
                dept_cols['final_score'] = col
            if col == f'{dep} Final Score %':
                dept_cols['final_score_pct'] = col
            if col == f'{dep} Status':
                dept_cols['status'] = col

        # Sort attempts by attempt number
        dept_cols['attempts'].sort(key=lambda x: x[0])

        # Add attempts in order
        for attempt_num, att_col, pct_col in dept_cols['attempts']:
            if att_col:
                new_order.append(att_col)
            if pct_col:
                new_order.append(pct_col)

        # Add Max Marks, Final Score, Final Score %
        if dept_cols['max_marks']:
            new_order.append(dept_cols['max_marks'])
        if dept_cols['final_score']:
            new_order.append(dept_cols['final_score'])
        if dept_cols['final_score_pct']:
            new_order.append(dept_cols['final_score_pct'])
        if dept_cols['status']:
            new_order.append(dept_cols['status'])

    # Add all remaining columns
    for col in columns:
        if col not in new_order:
            new_order.append(col)
    df = df[new_order].copy()

    indiv_pass_fail_status = df[[item for item in new_order if 'Final Status' in item]].fillna('None')
    indiv_pass_fail_status = indiv_pass_fail_status.replace('None',None)

    indiv_pass_fail_status['Total Pending Departments'] = indiv_pass_fail_status.apply(lambda row: row.map({'Pending':True}).sum(),axis=1)
    indiv_pass_fail_status['Total Fail Departments'] = indiv_pass_fail_status.apply(lambda row: row.map({'Fail':True}).sum(),axis=1)
    indiv_pass_fail_status['Total Pass Departments'] = indiv_pass_fail_status.apply(lambda row: row.map({'Pass':True}).sum(),axis=1)

    df = pd.concat([df,indiv_pass_fail_status[['Total Pass Departments','Total Fail Departments','Total Pending Departments']]],axis=1)

    df = df.fillna('None')
    df = df.replace('None',None)
    df.drop('MaxPossibleScore',axis=1,inplace=True)
    df.columns = [item.replace('_',' ') for item in df.columns.to_list()]

    return df