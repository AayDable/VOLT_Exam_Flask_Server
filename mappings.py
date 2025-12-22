#Department abbreviation name mapping
DEPS_MAPPING = {
    'SA': 'Sales',
    'LE': 'Legal',
    'MA': 'Marketing',
    'FI': 'Finance',
    'TR': 'Transport',
    'HR': 'HR',
    'ADO': 'Admin Operations',
    'ACO': 'Academic Operations',
    'ACS': 'Academic Support',
    'D&E': 'Development And Expansion',
    'IT': 'IT',
    'ACC': 'Academic Content PP',
    'ACP': 'Academic Content Primary',
    'AP': 'Academic Partnership',
    'SPA': 'Spark',
    'SCHL': 'SCHL',
}


employee_status_mapping =   {
                            "Pass":"Pass",
                            "Fail":"Fail",
                            "Absent":"Not Attempted",
                            "Pending":"Pending"
                            }

dummy_data_employees = ["Prem Dummy","Prem","Shaileja Nema","Beki Sunil","Ankit Mehta","Vikas Ghete"]
dummy_rollno_prefixes = ["OM","F","AM","MR","GS"]

rollno_suffix_mapping =  {
                            'A':'Attempt 1',
                            'B':'Attempt 2',
                            'C':'Attempt 3'
                         }

dashboard_data_col_mapping = {'candidateName':'Employee Name','hallName':'Core School'}