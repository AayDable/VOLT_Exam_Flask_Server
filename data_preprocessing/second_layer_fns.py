from data_preprocessing.first_layer_fns import *
from data_preprocessing.helper_fns import *
from cache import cache_manager
from urllib.parse import unquote
from copy import deepcopy

@pre_post_process
async def l2_get_proper_dashboard_data():
    data = await cache_manager.get_or_fetch(l1_get_proper_dashboard_data_unprocessed)
    return data

@pre_post_process
async def l2_get_dashboard_data_citylevel(city):
    dashboard_data = await cache_manager.get_or_fetch(l1_get_proper_dashboard_data_unprocessed)
    dashboard_data = dashboard_data[0]
    dashboard_data.query('City == @city',inplace=True)
    return dashboard_data

@pre_post_process
async def l2_get_stats_main():
    dashboard_df = await cache_manager.get_or_fetch(l1_get_rawdata_cleaned)
    stats_df = pd.DataFrame({'Total Departments': dashboard_df['dep_prefix'].nunique(),
                  'Total Candidates':dashboard_df['candidateName'].nunique(),
                  'Total Core Schools': dashboard_df['hallName'].nunique()},index=range(0,1))

    return stats_df

@pre_post_process
async def l2_get_citywise_barchart():
    dashboard_df = await cache_manager.get_or_fetch(l1_get_rawdata_cleaned)
    bar_df = dashboard_df.drop_duplicates('candidateName').groupby('hallName').agg({'Employee Code':'count'}).reset_index()
    return bar_df

@pre_post_process
async def l2_get_citywise_barchart():
    dashboard_df = await cache_manager.get_or_fetch(l1_get_rawdata_cleaned)
    bar_df = dashboard_df.drop_duplicates('candidateName').groupby('hallName').agg({'Employee Code':'count'}).reset_index()
    return bar_df

@pre_post_process
async def l2_get_stats_city(city):
    dashboard_df = await cache_manager.get_or_fetch(l1_get_rawdata_cleaned)
    dashboard_df.query('hallName == @city',inplace=True)
    stats_df = pd.DataFrame({'Total Departments': dashboard_df['dep_prefix'].nunique(),
                  'Total candidates':dashboard_df['candidateName'].nunique(),
                  'Total Cities': dashboard_df['hallName'].nunique()},index=range(0,1))
    return stats_df
    
@pre_post_process
async def l2_get_coursewise_barchart(city):
    dashboard_df = await cache_manager.get_or_fetch(l1_get_rawdata_cleaned)
    dashboard_df.query('hallName == @city',inplace=True)
    bar_df = dashboard_df.drop_duplicates('candidateName').groupby('courseName').agg({'Employee Code':'count'}).reset_index()

    return bar_df

@pre_post_process
async def l2_score_wise_grid(attempts,format):
    attempts = attempts.split("|")
    df = await cache_manager.get_or_fetch(l1_get_proper_dashboard_data_unprocessed)
    
    if len(attempts)>1:
        fin_score_cols = []
        for attempt_name in attempts:
            if format == "Percentage":
                fin_score_cols.extend([string+" %" for string in df.columns if string.endswith(attempt_name)])
            elif format == "Scores":
                fin_score_cols.extend([string for string in df.columns if string.endswith(attempt_name)])
    else:
        if format == "Percentage":
            fin_score_cols = [string+" %" for string in df.columns if string.endswith(attempts[0])]
        elif format == "Scores":
            fin_score_cols = [string for string in df.columns if string.endswith(attempts[0])]

    df = df[['candidateName']+fin_score_cols]
    df.columns = [item.replace('\n','').replace(' Score','').strip() for item in df.columns]
    df = df.fillna('__TEMP__').replace('__TEMP__', None)
    df = df.sort_values('candidateName').copy()
    df = df.rename({'candidateName':'#Employee Name'},axis=1).copy()
    return df

@pre_post_process
async def l2_status_wise_grid(attempts):
    attempts = attempts.split("|")
    df = await cache_manager.get_or_fetch(l1_get_proper_dashboard_data_unprocessed)
    

    if len(attempts)>1:
        status_cols = []
        for attempt_name in attempts:
            status_cols.extend([string for string in df.columns if string.endswith('Status') and attempt_name in string])
    else:
        status_cols = [string for string in df.columns if string.endswith('Status') and attempts[0] in string]

    df = df[['candidateName']+status_cols]
    for col in status_cols:
        df.loc[:, col] = df[col].str.upper()
    df.columns = [item.replace('\n','').replace('Status','').strip() for item in df.columns]
    df = df.sort_values('candidateName').copy()
    df = df.rename({'candidateName':'#Employee Name'},axis=1).copy()
    return df

@pre_post_process
async def l2_overall_score_distribution(dep):
    df = await cache_manager.get_or_fetch(l1_get_proper_dashboard_data_unprocessed)
    
    df = df[[f'{dep} Final Score']].dropna()
    df.columns = ["Score"]
    return df

@pre_post_process
async def l2_get_available_cities():
    df = await cache_manager.get_or_fetch(l1_get_proper_dashboard_data_unprocessed)
    
    df = df[['City']].drop_duplicates()
    return df

@pre_post_process
async def l2_departmentwise_average_scores():
    df = await cache_manager.get_or_fetch(l1_get_proper_dashboard_data_unprocessed)
    

    df = df[[col for col in df.columns if col.endswith('Final Score')]].mean().reset_index().copy()
    df.columns = ['Department','Average Final Score']
    df['Department'] = df['Department'].str.replace(' Final Score','')
    return df

@pre_post_process
async def l2_retrieve_departments():
    df = await cache_manager.get_or_fetch(l1_get_rawdata_cleaned)
    return pd.DataFrame(df['dep_prefix'].unique(),columns=['Departments']).sort_values('Departments')

@pre_post_process
async def l2_pass_fail_pending_count():
    df = await cache_manager.get_or_fetch(l1_get_proper_dashboard_data_unprocessed)
    
    stat_pass = df[[col for col in df.columns if col.endswith('Final Status')]]=='Pass'
    stat_fail = df[[col for col in df.columns if col.endswith('Final Status')]]=='Fail'
    stat_pending = df[[col for col in df.columns if col.endswith('Final Status')]] == 'Pending'
    stat_absent  =  df[[col for col in df.columns if col.endswith('Final Status')]]=='Not Attempted'
    pass_count = stat_pass.sum()
    fail_count = stat_fail.sum()
    pending_count = stat_pending.sum()
    absent_count = stat_absent.sum()
    df = pd.concat([pass_count,fail_count,pending_count,absent_count],keys=['Pass','Fail','Pending','Not Attempted'],axis=1)
    df.index = df.index.str.replace(' Final Status','')
    df.reset_index(inplace=True, names = ['Department'])
    return df

@pre_post_process
async def l2_get_dashboard_data_for_dep(chosen_dep,candidate):
    global deps_mapping
    dep_map = deepcopy(deps_mapping)
    df = await cache_manager.get_or_fetch(l1_get_rawdata_cleaned)
    dep_wise_max_marks = df[['MaxPossibleScore','dep_prefix']].drop_duplicates().copy()
    if chosen_dep:
        if chosen_dep != 'All':
            df.query("dep_prefix == @chosen_dep",inplace=True)
            dep_map = {key:dep_map[key] for key in dep_map if dep_map[key] == chosen_dep}
    if candidate:
        if candidate != 'All':
            candidate = unquote(candidate)
            df.query("candidateName == @candidate",inplace=True)

    df['courseName_cleaned'] = df["dep_prefix"].astype(str) + "_" + df["batch_suffix"].astype(str)
    df['courseName_cleaned_percent'] = df['courseName_cleaned'] + " %"
    
    score_cols = df.pivot(columns='courseName_cleaned', values='TotalCandidateScore').copy()
    percent_cols = df.pivot(columns='courseName_cleaned_percent', values='ScorePercentage').copy()
    df = df[[col for col in list(df.columns) if col not in ['dep_prefix','batch_suffix','courseName_cleaned','courseName','rollNo','examDate','TotalCandidateScore','ScorePercentage','courseName_cleaned_percent']]].copy()

    df = pd.concat([df,score_cols,percent_cols],axis=1).copy()

    df = df.groupby('candidateName').agg('first').reset_index()

    for dep in dep_map.values():
        df[f'{dep} Max Marks'] = dep_wise_max_marks.query("dep_prefix == @dep").MaxPossibleScore.iloc[0]
    for dep in dep_map.values():
        df[f'{dep} Final Score'] = df[[col for col in df.columns if col.startswith(f'{dep}_') and '%' not in col]].max(axis=1).reset_index(drop=True)

    for dep in dep_map.values():
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
    
    for dep in dep_map.values():
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
    for dep in dep_map.values():
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

    if chosen_dep == 'All':
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

@pre_post_process
async def l2_get_candidate_names(dep):
    df = await cache_manager.get_or_fetch(l1_get_rawdata_cleaned)

    if dep != 'All':
        df.query('dep_prefix == @dep',inplace=True)
    
    return df[['candidateName']].drop_duplicates().sort_values('candidateName')
    
    
if __name__ == '__main__':
    dashboard_data = asyncio.run(l2_retrieve_departments())
    dashboard_df = pd.DataFrame(dashboard_data)
    print(dashboard_df)
