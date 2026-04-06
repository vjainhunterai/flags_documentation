#!/usr/bin/env python
# coding: utf-8


import pandas as pd
import numpy as np, os
import json, re
from tqdm import tqdm
from datetime import datetime
import urllib.parse
import pymysql
import time

from dateutil.relativedelta import relativedelta
import itertools
tqdm.pandas()
import re

from itertools import combinations
from fuzzywuzzy import fuzz
from sqlalchemy import create_engine

from Levenshtein import distance as levenshtein_distance


# In[ ]:


# Standard month mapping
MONTH_MAP = {
    'JANUARY': 'JAN', 'JAN': 'JAN',
    'FEBRUARY': 'FEB', 'FEB': 'FEB',
    'MARCH': 'MAR', 'MAR': 'MAR',
    'APRIL': 'APR', 'APR': 'APR',
    'MAY': 'MAY', 'MAY': 'MAY',
    'JUNE': 'JUN', 'JUN': 'JUN',
    'JULY': 'JUL', 'JUL': 'JUL',
    'AUGUST': 'AUG', 'AUG': 'AUG',
    'SEPTEMBER': 'SEP', 'SEP': 'SEP', 'SEPT' : 'SEP',
    'OCTOBER': 'OCT', 'OCT': 'OCT',
    'NOVEMBER': 'NOV', 'NOV': 'NOV',
    'DECEMBER': 'DEC', 'DEC': 'DEC'
}

def normalize_date_token(text):
    # Uppercase and replace separators
    text = str(text).upper()
    text = re.sub(r'[^A-Z0-9]', '', text)  # Remove non-alphanum

    # Normalize months
    for full, short in MONTH_MAP.items():
        text = text.replace(full, short)
    
    # Normalize years
    text = re.sub(r'20(\d{2})', r'\1', text)  # 2024 -> 24

    return text

def is_date_variant_match(inv1, inv2, threshold=90):
    n1 = normalize_date_token(inv1)
    n2 = normalize_date_token(inv2)
    
    # Fuzzy token sort match
    score = fuzz.token_sort_ratio(n1, n2)
    return score >= threshold

def flag_date_format_match(invoice_list, threshold=90):
    cleaned_list = [str(x) for x in invoice_list if pd.notna(x)]
    for a, b in combinations(cleaned_list, 2):
        if is_date_variant_match(a, b, threshold=threshold):
            return 1
    return 0

def normalize(s):
    s = str(s).lower()
    s = re.sub(r'[^a-z0-9]', ' ', s)
    return re.sub(r'\s+', ' ', s).strip()

def longest_common_substring(s1, s2):
    m, n = len(s1), len(s2)
    dp = [[0]*(n+1) for _ in range(m+1)]
    lcs_len = 0
    for i in range(m):
        for j in range(n):
            if s1[i] == s2[j]:
                dp[i+1][j+1] = dp[i][j] + 1
                lcs_len = max(lcs_len, dp[i+1][j+1])
    return lcs_len

def is_partial_match(a, b, min_lcs=8, fuzzy_threshold=85):
    s1, s2 = normalize(a), normalize(b)
    lcs = longest_common_substring(s1, s2)
    fuzzy = fuzz.token_sort_ratio(s1, s2)
    return lcs >= min_lcs or fuzzy >= fuzzy_threshold

def flag_core_substring_match(invoice_list, min_lcs=8, fuzzy_threshold=85):
    cleaned_list = [str(x) for x in invoice_list if pd.notna(x)]
    for a, b in combinations(cleaned_list, 2):
        if is_partial_match(a, b, min_lcs, fuzzy_threshold):
            return 1
    return 0
    
def extract_numeric_normalized(s):
    # Extract all digits and remove leading zeros
    num = ''.join(re.findall(r'\d+', s))
    return num.lstrip('0')

def is_alpha_diff_only(inv1, inv2):
    return extract_numeric_normalized(inv1) == extract_numeric_normalized(inv2)

def has_duplicates(text_list):
    seen = set()
    for item in text_list:
        if item in seen:
            return True
        seen.add(item)
    return False

def extract_digits(s):
    return ''.join(re.findall(r'\d+', str(s)))

def is_near_numeric_match(s1, s2, max_distance=2):
    d1 = extract_digits(s1)
    d2 = extract_digits(s2)

    # Ignore if either is empty or too short to compare meaningfully
    if not d1 or not d2 or len(d1) < 5 or len(d2) < 5:
        return False

    # Levenshtein distance between numeric parts
    dist = levenshtein_distance(d1, d2)
    return dist <= max_distance

def flag_near_match_in_list(invoice_list, max_distance=2):
    cleaned_list = [str(x) for x in invoice_list if pd.notna(x)]
    for a, b in combinations(cleaned_list, 2):
        if is_near_numeric_match(a, b, max_distance=max_distance):
            return True
    return False
def getDataFromDatabaseFull_Description(table_name, schema):    
    user = "rdsdev1"
    pwd = "Gpo!@!health"
    password = urllib.parse.quote_plus(pwd)
    ip = "prod-db.c969yoyq9cyy.us-east-1.rds.amazonaws.com"
    engine = create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.format(user, password, ip, schema))
    query = f"SELECT * FROM {schema}.{table_name}"
#     query = f"SELECT {columnName} FROM {schema}.{table_name};" 

    with engine.connect() as conn:        
        df = pd.read_sql(query, conn)
    return df

def filterSimilarDescriptions(group, threshold = 90):
    descriptions = group['Line_Description'].tolist()
    ref_desc = descriptions[0]  # use the first as reference
    group['similarity'] = group['Line_Description'].apply(lambda x: fuzz.token_set_ratio(x, ref_desc))
    return group[group['similarity'] >= threshold].drop(columns='similarity')

    
def flag_numeric_within_1000(group):
    
    numeric_df = group[group['is_numeric_invoice'].fillna(False)].copy()
    nums = numeric_df['invoice_number_digits'].dropna().values

    # Initialize False for all
    group['flag_invoiceno_diff_within_1000'] = 0

    if len(nums) >= 2:
        flag = any(abs(i - j) <= 1000 for i in nums for j in nums if i != j)
        if flag:
            # Set True only for numeric rows
            group.loc[group['is_numeric_invoice'].fillna(False), 'flag_invoiceno_diff_within_1000'] = 1
    return group

def flag_cleaned_digits_within_1000(group):
    nums = group['invoice_number_cleaned'].dropna().values

    # Initialize all rows as False
    group['invoice_diff_within_1000_cleaned'] = 0

    if len(nums) >= 2:
        # Check for any pair difference ≤ 1000
        flag = any(abs(i - j) <= 1000 for i in nums for j in nums if i != j)
        if flag:
            # Flag only cleaned numeric invoice rows
            mask = group['invoice_number_cleaned'].notna()
            group.loc[mask, 'invoice_diff_within_1000_cleaned'] = 1
    return group

def getDataFromDatabaseFull_Description_top5(table_name, schema):    
    user = "rdsdev1"
    pwd = "Gpo!@!health"
    password = urllib.parse.quote_plus(pwd)
    ip = "prod-db.c969yoyq9cyy.us-east-1.rds.amazonaws.com"
    engine = create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.format(user, password, ip, schema))
    query = f"SELECT * FROM {schema}.{table_name} LIMIT 5;"
#     query = f"SELECT {columnName} FROM {schema}.{table_name};" 

    with engine.connect() as conn:        
        df = pd.read_sql(query, conn)
    return df

def truncateFromDatabase(old_table):

    import mysql.connector
    from datetime import datetime
       
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    new_table = old_table + "_" + timestamp

    ###########################

    dbConn = pymysql.connect(host = "prod-db.c969yoyq9cyy.us-east-1.rds.amazonaws.com",
                             database = "anomaly",
                             user = "rdsdev1", 
                             password = "Gpo!@!health")
    dbCursor = dbConn.cursor()
    

    query = f"""
        CREATE TABLE {new_table} AS SELECT * from {old_table};
    """
    dummy = dbCursor.execute(query)
    dbConn.close()
    
    dbConn = pymysql.connect(host = "prod-db.c969yoyq9cyy.us-east-1.rds.amazonaws.com",
                             database = "anomaly",
                             user = "rdsdev1", 
                             password = "Gpo!@!health")
    dbCursor = dbConn.cursor()
    
    dummy = dbCursor.execute("SET SQL_SAFE_UPDATES = 0;")
    dummy = dbCursor.execute(f"TRUNCATE TABLE `{old_table}`;")
    dummy = dbCursor.execute("SET SQL_SAFE_UPDATES = 1;")
    dbConn.close()
    return

def updateDataBaseTable_v2(df, table_name):

    truncateFromDatabase(table_name)

    user = "rdsdev1"
    pwd = "Gpo!@!health"
    password = urllib.parse.quote_plus(pwd)
    schema = "anomaly"
    ip = "prod-db.c969yoyq9cyy.us-east-1.rds.amazonaws.com"
    engine = create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.format(user,password,ip,schema))

    dataLoaded = True
    with engine.connect() as conn:
        tran = conn.begin()
        start_time = time.time()
        dataLoaded = True
        df.to_sql(name = table_name,
                        schema = "anomaly", 
                        con = conn, if_exists = "append", index = False)
        tran.commit()

    tran.close()
    return

def readSchemaOfTable(tableName):
    dbConn = pymysql.connect(host = "prod-db.c969yoyq9cyy.us-east-1.rds.amazonaws.com",
                             database = "anomaly",
                             user = "rdsdev1", 
                             password = "Gpo!@!health")
    dbCursor = dbConn.cursor()

    dummy = dbCursor.execute("DESCRIBE " + tableName + ";")
    tableInvMaster = dbCursor.fetchall()
    tableInvMasterKey = [value[0] for value in tableInvMaster]

    return tableInvMasterKey


# In[ ]:


if __name__ == "__main__":

    target_table = "duplicate_ap_invoice"
    
    dfSource = getDataFromDatabaseFull_Description(target_table, "anomaly")
    dfSource.insert(0, "Primary_key", range(1, len(dfSource) + 1))

    ## STEP - 1 
    multi_match_ids = dfSource['Matched_Record_Number'].value_counts()
    multi_match_ids = multi_match_ids[multi_match_ids > 1].index
    
    # Step 2: Filter original DataFrame to keep only those records
    df1 = dfSource[dfSource['Matched_Record_Number'].isin(multi_match_ids)]

    #####################################################
    ## STEP 3 - flag_equal_amount - Keep only rows where Invoice amount (claim level) is equal to sum of extended_amount (at line level) for an invoice
    df = df1.copy()
    
    df["Supplier_Invoice_Number"] = df["Supplier_Invoice_Number"].astype(str).str.strip()
    df["Extended Amount"] = df["Extended Amount"].astype(float)
    df["Invoice_amount"] = df["Invoice_amount"].astype(float)
    
    grouped = df.groupby('Supplier_Invoice_Number')['Extended Amount'].sum().reset_index()
    grouped.columns = ['Supplier_Invoice_Number', 'Total_Extended_Amount']
    
    # Step 3a: Merge the total extended amount back to the original DataFrame
    df = df.merge(grouped, on='Supplier_Invoice_Number', how='left')
    
    # Step 3b: Keep only those lines where total extended amount equals invoice amount
    df = df[df['Total_Extended_Amount'] == df['Invoice_amount']]    
    df = df.drop(columns=['Total_Extended_Amount'])
    
    multi_match_ids = df['Matched_Record_Number'].value_counts()
    multi_match_ids = multi_match_ids[multi_match_ids > 1].index
    
    # Step 3c: Filter original DataFrame to keep only those records
    df = df[df['Matched_Record_Number'].isin(multi_match_ids)]
    
    df["flag_equal_amount"] = 1
    
    segmentReplaceDict = dict(df[["Primary_key" , "flag_equal_amount"]].values)
    df1["flag_equal_amount"] = df1["Primary_key"].map(segmentReplaceDict).fillna(0)

    print ("Duplicate post processing - Flag Compled - flag_equal_amount")


    ####################################################
    ## STEP 4 - flag_similar_descriptions - Keep only similar descriptions
    df = df1.copy()
    
    df = df.groupby('Matched_Record_Number', group_keys=False).progress_apply(filterSimilarDescriptions)
    
    multi_match_ids = df['Matched_Record_Number'].value_counts()
    multi_match_ids = multi_match_ids[multi_match_ids > 1].index
    
    # Step 4a: Filter original DataFrame to keep only those records
    df = df[df['Matched_Record_Number'].isin(multi_match_ids)]
    
    df["flag_similar_descriptions"] = 1
    
    segmentReplaceDict = dict(df[["Primary_key" , "flag_similar_descriptions"]].values)
    df1["flag_similar_descriptions"] = df1["Primary_key"].map(segmentReplaceDict).fillna(0)

    print ("Duplicate post processing - Flag Compled - flag_similar_descriptions")


    ######################################################
    ### STEP 5 - 1000 difference check only for invoice_numbers that are numeri
    df = df1.copy()
    
    df['is_numeric_invoice'] = df['Supplier_Invoice_Number'].str.isnumeric()
    df['invoice_number_digits'] = pd.to_numeric(df['Supplier_Invoice_Number'].where(df['is_numeric_invoice']), errors='coerce')
    
    df = df.groupby('Matched_Record_Number', group_keys=False).progress_apply(flag_numeric_within_1000)
    df = df.drop(columns=['invoice_number_digits', 'is_numeric_invoice'])
    
    df['invoice_number_digits_only'] = df['Supplier_Invoice_Number'].str.extract(r'(\d+)', expand=False)
    df['invoice_number_cleaned'] = pd.to_numeric(df['invoice_number_digits_only'], errors='coerce')
    
    # Step 5A: Apply group-wise with progress bar
    df = df.groupby('Matched_Record_Number', group_keys=False).progress_apply(flag_cleaned_digits_within_1000)
    
    df = df.drop(columns=['invoice_number_digits_only', 'invoice_number_cleaned'])
    
    ## Check both and have 1 common flag
    df['flag_invoiceno_diff_within_1000'] = (
        df['flag_invoiceno_diff_within_1000'] | df['invoice_diff_within_1000_cleaned']
    ).astype(int)

    print ("Duplicate post processing - Flag Compled - flag_invoiceno_diff_within_1000")


    ######### STEP 6 - SEQUENTIAL INVOICE NUMBER
    # Generate flags per group
    group_flags = []
    for _, group in df.groupby("Matched_Record_Number"):
        indices = group.index
        invoice_numbers = group["Supplier_Invoice_Number"].tolist()
    
        ## check for duplicate invoice numbers
        if has_duplicates(invoice_numbers):
            flag = 1
        elif flag_near_match_in_list(invoice_numbers):
            flag = 1
        elif flag_date_format_match(invoice_numbers):
            flag = 1
        elif flag_core_substring_match(invoice_numbers):
            flag = 1
        else:
            flag = 1
            for inv1, inv2 in combinations(invoice_numbers, 2):
                if not is_alpha_diff_only(str(inv1), str(inv2)):
                    flag = 0
                    break
        group_flags.extend([(i, flag) for i in indices])
    
    # Assign flag column
    flag_df = pd.DataFrame(group_flags, columns=["index", "flag_invoiceno_no_sequence"]).set_index("index")
    df["flag_invoiceno_no_sequence"] = flag_df["flag_invoiceno_no_sequence"]

    print ("Duplicate post processing - Flag Compled - flag_invoiceno_no_sequence")


    ###### map to source
    segmentReplaceDict = dict(df[["Primary_key" , "flag_invoiceno_no_sequence"]].values)
    dfSource["flag_invoiceno_no_sequence"] = dfSource["Primary_key"].map(segmentReplaceDict).fillna(0)
    
    segmentReplaceDict = dict(df[["Primary_key" , "flag_similar_descriptions"]].values)
    dfSource["flag_similar_descriptions"] = dfSource["Primary_key"].map(segmentReplaceDict).fillna(0)
    
    segmentReplaceDict = dict(df[["Primary_key" , "flag_equal_amount"]].values)
    dfSource["flag_equal_amount"] = dfSource["Primary_key"].map(segmentReplaceDict).fillna(0)
    
    segmentReplaceDict = dict(df[["Primary_key" , "flag_invoiceno_diff_within_1000"]].values)
    dfSource["flag_invoiceno_diff_within_1000"] = dfSource["Primary_key"].map(segmentReplaceDict).fillna(0)

    ### MAP TO DB SCHEMA
    dfTop5 = getDataFromDatabaseFull_Description_top5("duplicate_ap_invoice", "anomaly")
    reqdCols = dfTop5.columns.tolist()
    
    dfSource_v2 = dfSource[reqdCols]

    dfSource_v2["flag_invoiceno_no_sequence"] = dfSource_v2["flag_invoiceno_no_sequence"].astype(int)
    dfSource_v2["flag_similar_descriptions"] = dfSource_v2["flag_similar_descriptions"].astype(int)
    dfSource_v2["flag_equal_amount"] = dfSource_v2["flag_equal_amount"].astype(int)
    dfSource_v2["flag_invoiceno_diff_within_1000"] = dfSource_v2["flag_invoiceno_diff_within_1000"].astype(int)

    dfSource_v2 = dfSource_v2.reset_index(drop = True)

    outputSchema = readSchemaOfTable("duplicate_ap_invoice")

    ## Upate new fields
    newColsList = list(np.setdiff1d(outputSchema, dfSource_v2.columns.tolist()))
    for col in newColsList:
        dfSource_v2[col] = ""
    dfSource_v2 = dfSource_v2[outputSchema]

    dfSource_v2 = dfSource_v2.reset_index(drop = True)
    updateDataBaseTable_v2(dfSource_v2, target_table)

    print ("Duplicate post processing completed - Filters Loaded into the table")

    exit()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




