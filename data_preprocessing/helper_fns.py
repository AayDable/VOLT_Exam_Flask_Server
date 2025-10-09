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