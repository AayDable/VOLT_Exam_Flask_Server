import functools
from mappings import *
import pandas as pd
from jinja2 import Template

def query_builder(template_string, **kwargs):
    """
    Build SQL query from Jinja2 template string.
    
    Args:
        template_string: Jinja2 template string (SQL query with placeholders)
        **kwargs: Key-value pairs to substitute in the template
        
    Returns:
        Rendered SQL query string
        
    Example:
        >>> template = "SELECT * FROM {{ table }} WHERE id = {{ id }}"
        >>> query = query_builder(template, table="users", id=123)
        >>> print(query)
        SELECT * FROM users WHERE id = 123
    """
    template = Template(template_string)
    return template.render(**kwargs)

#Pre-post processing wrapper for 2nd layer fns
def pre_post_process(func):
    """Decorator that adds pre and post processing around an async function"""
    
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        # ========== PRE-PROCESS STEPS ==========
        #None yet
        
        # ========== EXECUTE MAIN FUNCTION ==========
        try:
            result = await func(*args, **kwargs)
        except Exception as e:
            print(f"❌ ERROR in {func.__name__}: {e}")
            raise
        
        # ========== POST-PROCESS STEPS ==========
        result.rename(dashboard_data_col_mapping,axis=1,inplace=True)
        
        return result
    
    return wrapper

def dataframe_to_json(df):
    """Convert pandas DataFrame to JSON format suitable for Grafana"""
    # Convert DataFrame to records (list of dictionaries)
    # Handle datetime and other pandas-specific types
    df_copy = df.copy()
    
    # Convert datetime columns to strings
    for col in df_copy.select_dtypes(include=['datetime64']).columns:
        df_copy[col] = df_copy[col].dt.strftime('%Y-%m-%d')
    
    # Convert any NaN values to None (which becomes null in JSON)
    df_copy = df_copy.where(pd.notnull(df_copy), None)
    
    return df_copy.to_dict('records')

def transform_to_matrix(df):
    """
    Transform the variable-value DataFrame into a matrix format
    
    Parameters:
    df: DataFrame with 'variable' and 'value' columns
    
    Returns:
    DataFrame with departments as rows and attempts/final/status as columns
    """
    # Extract department names
    departments = ['Sales', 'Legal', 'Marketing', 'Finance', 'Transport', 'HR']
    
    # Initialize result dictionary
    result = {
        'Department': [],
        'Attempt 1':[],
        'Attempt 1 %': [],
        'Attempt 2':[],
        'Attempt 2 %': [],
        'Attempt 3':[],
        'Attempt 3 %': [],
        'Final Score': [],
        'Final Score %': [],
        'Status': []
    }
    
    # Process each department
    for dept in departments:
        result['Department'].append(dept)
        
        # Get Attempt 1 %
        attempt1 = df[df['variable'] == f'{dept} Attempt 1']['value'].values
        attempt1_per = df[df['variable'] == f'{dept} Attempt 1 %']['value'].values

        result['Attempt 1'].append(attempt1[0] if len(attempt1) > 0 else None)
        result['Attempt 1 %'].append(attempt1_per[0] if len(attempt1_per) > 0 else None)

        # Get Attempt 2 %
        attempt2 = df[df['variable'] == f'{dept} Attempt 2']['value'].values
        attempt2_per = df[df['variable'] == f'{dept} Attempt 2 %']['value'].values

        result['Attempt 2'].append(attempt2[0] if len(attempt2) > 0 else None)
        result['Attempt 2 %'].append(attempt2_per[0] if len(attempt2_per) > 0 else None)

        # Get Attempt 3 %
        attempt3 = df[df['variable'] == f'{dept} Attempt 3']['value'].values
        attempt3_per = df[df['variable'] == f'{dept} Attempt 3 %']['value'].values

        result['Attempt 3'].append(attempt3[0] if len(attempt3) > 0 else None)
        result['Attempt 3 %'].append(attempt3_per[0] if len(attempt3_per) > 0 else None)
        
        # Get Final Score %
        final = df[df['variable'] == f'{dept} Final Score']['value'].values
        final_per = df[df['variable'] == f'{dept} Final Score %']['value'].values
        result['Final Score'].append(final[0] if len(final) > 0 else None)
        result['Final Score %'].append(final_per[0] if len(final_per) > 0 else None)
        
        # Get Final Status
        status = df[df['variable'] == f'{dept} Final Status']['value'].values
        result['Status'].append(status[0] if len(status) > 0 else None)
    
    return pd.DataFrame(result)