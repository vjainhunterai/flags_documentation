import pandas as pd
import Levenshtein
from Levenshtein import editops
import pandas as pd
import numpy as np
from datetime import datetime
from itertools import count
from tqdm import tqdm
import Levenshtein
import re
import ast
import networkx as nx
from sqlalchemy import create_engine
import urllib
import sys
tqdm.pandas()
a=pd.DataFrame()
from itertools import combinations
import gc

def PRIORITY_E2E():
    import pandas as pd
    from cryptography.fernet import Fernet
    import os
    import sys
    try:
        env_local= os.getenv("ENV", "prod")
    except Exception as e:
        print(f"{e}")
    def readEncryptedConfig(excelFilePath,env):
        """
        Reads the encrypted configuration from the Excel file.

        Args:
            excel_file_path (str): The path to the Excel file.

        Returns:
            dict: A dictionary containing the decrypted configuration.
        """
        def decryptData(data, keyDirectory):

            scKey = open(keyDirectory, 'rb').read()
            cipherSuite = Fernet(scKey)
            if isinstance(data, str):
                return cipherSuite.decrypt(data.encode()).decode()
            else:
                return data
        # Read paths from Excel file
        pathsDf1 = pd.read_excel(excelFilePath)
        pathsDf = pathsDf1[pathsDf1['Env'] == env]
        pathsDict = pathsDf.set_index('Key_name')['Path'].to_dict()

        # Read the encryption key
        keyPath = pathsDict['key_path']
        encryptedFile = pathsDict['encrypted_file']

        # Read the encryption key
        scKey = open(keyPath, 'rb').read()
        cipherSuite = Fernet(scKey)

        # Read the encrypted file
        df = pd.read_csv(encryptedFile)

        # Decrypt the data
        df_decrypted = df.applymap(lambda x: decryptData(str(x), keyPath))
        df = pd.DataFrame(df_decrypted)

        # Extract configuration
        config = {
             'host': str(df.at[0, 'host']),
             'database': str(df.at[0, 'database']),
             'user': str(df.at[0, 'user']),
             'password': str(df.at[0, 'password']),
             'port': 3306
         }

        return config

    def priority_flag(df):
        df['Priority_flag'] = None
        df['Priority_reason'] = None

        def max_consecutive_same(s1, s2):
            m = len(s1)
            n = len(s2)
            max_len = 0
            end = 0
            dp = [[0] * (n + 1) for _ in range(m + 1)]
            for i in range(m):
                for j in range(n):
                    if s1[i] == s2[j]:
                        dp[i + 1][j + 1] = dp[i][j] + 1
                        if dp[i + 1][j + 1] > max_len:
                            max_len = dp[i + 1][j + 1]
                            end = i + 1
            return max_len

        df['A'] = None
        df['B'] = None
        for mrn in df['Matched_Record_Number'].unique():
            mask = df['Matched_Record_Number'] == mrn
            sub = df.loc[mask]
            unique_suppl = sub['Supplier_Invoice_Number'].unique()
            if len(unique_suppl) == 2:
                s1, s2 = str(unique_suppl[0]), str(unique_suppl[1])
                lcs_len = max_consecutive_same(s1, s2)
                df.loc[mask, 'A'] = lcs_len
                df.loc[mask, 'B'] = sub['Supplier_Invoice_Number'].astype(str).apply(
                    lambda x: lcs_len / len(x) if len(x) > 0 else 0)
                df['C'] = 0
        for mrn in df['Matched_Record_Number'].unique():
            mask = df['Matched_Record_Number'] == mrn
            sub = df.loc[mask]
            if sub['Supplier_Invoice_Number'].nunique() == 2:
                cond1 = (sub['B'] == 1).any()
                cond2 = (sub[sub['B'] > 0.74]['Supplier_Invoice_Number'].nunique() > 1)
                cond3 = (sub[sub['A'] >= 8]['Supplier_Invoice_Number'].nunique() > 1)
                if cond1 or cond2 or cond3:
                    df.loc[mask, 'C'] = 1
        df.loc[df['C'] == 1, 'Priority_flag'] = 3
        df.loc[df['C'] == 1, 'Priority_reason'] = "Matching75-79%"
        df['C'] = 0
        for mrn in df['Matched_Record_Number'].unique():
            mask = df['Matched_Record_Number'] == mrn
            sub = df.loc[mask]
            if sub['Supplier_Invoice_Number'].nunique() == 2:
                cond1 = (sub['B'] == 1).any()
                cond2 = (sub[sub['B'] > 0.79]['Supplier_Invoice_Number'].nunique() > 1)
                cond3 = (sub[sub['A'] >= 8]['Supplier_Invoice_Number'].nunique() > 1)
                if cond1 or cond2 or cond3:
                    df.loc[mask, 'C'] = 1
        df.loc[df['C'] == 1, 'Priority_flag'] = 3
        df.loc[df['C'] == 1, 'Priority_reason'] = "Matching80-84%"
        df['C'] = 0
        for mrn in df['Matched_Record_Number'].unique():
            mask = df['Matched_Record_Number'] == mrn
            sub = df.loc[mask]
            if sub['Supplier_Invoice_Number'].nunique() == 2:
                cond1 = (sub['B'] == 1).any()
                cond2 = (sub[sub['B'] > 0.84]['Supplier_Invoice_Number'].nunique() > 1)
                cond3 = (sub[sub['A'] >= 8]['Supplier_Invoice_Number'].nunique() > 1)
                if cond1 or cond2 or cond3:
                    df.loc[mask, 'C'] = 1
        df.loc[df['C'] == 1, 'Priority_flag'] = 3
        df.loc[df['C'] == 1, 'Priority_reason'] = "Matching85+%"
        df['Levenshtein'] = None
        for mrn in df['Matched_Record_Number'].unique():
            mask = df['Matched_Record_Number'] == mrn
            sub_df = df[mask]
            uni = sub_df['Supplier_Invoice_Number'].dropna().unique()
            if len(uni) == 2:
                lev_dist = Levenshtein.distance(str(uni[0]).replace(" ", ""), str(uni[1]).replace(" ", ""))
                df.loc[mask, 'Levenshtein'] = lev_dist
        print(df[df['Levenshtein'] == 1].shape)
        print(df[df['Levenshtein'] == 1]['Confirmed'].value_counts())

        df.loc[((df['Levenshtein'] == 1) & (df['flag_invoiceno_diff_within_999'] != 1) & (df['Invoice_amount'] > 999) & (
                    df['Invoice_amount'] < 5001)), 'Priority_flag'] = 3
        df.loc[((df['Levenshtein'] == 1) & (df['Invoice_amount'] > 5000)), 'Priority_flag'] = 3
        df.loc[((df['Levenshtein'] == 1) & (df['flag_invoiceno_diff_within_999'] != 1) & (df['Invoice_amount'] > 999) & (
                    df['Invoice_amount'] < 5001)), 'Priority_reason'] = "2digitsMissKeying"
        df.loc[((df['Levenshtein'] == 1) & (df['Invoice_amount'] > 5000)), 'Priority_reason'] = "1LevinshteinDistance"
        df['2digitsMissKeying'] = None
        for mrn in df[df['Levenshtein'] == 2]['Matched_Record_Number'].unique():
            mask = (df['Matched_Record_Number'] == mrn) & (df['Levenshtein'] == 2)
            sub_df = df.loc[mask]
            # display(sub_df[['Supplier_Invoice_Number','Lev']])
            uni = sub_df['Supplier_Invoice_Number'].dropna().unique()
            if len(uni) == 2:
                s1, s2 = str(uni[0]), str(uni[1])

                if len(s1) == len(s2):
                    diff_idx = [i for i in range(len(s1)) if s1[i] != s2[i]]
                    # print(s1)
                    # print(s2)
                    # print(diff_idx)
                    # print("========")
                    if (
                            len(diff_idx) == 2 and
                            diff_idx[1] == diff_idx[0] + 1 and
                            # list(s1)[diff_idx[0]], list(s1)[diff_idx[1]] == list(s2)[diff_idx[1]], list(s2)[diff_idx[0]]
                            s1[diff_idx[0]] == s2[diff_idx[1]] and
                            s1[diff_idx[1]] == s2[diff_idx[0]]

                    ):
                        # print("yes")
                        df.loc[mask, '2digitsMissKeying'] = 1
        print(df[df['2digitsMissKeying'] == 1].shape)
        print(df[df['2digitsMissKeying'] == 1]['Confirmed'].value_counts())

        df.loc[((df['2digitsMissKeying'] == 1) & (df['flag_invoiceno_diff_within_999'] == 1) & (
                    df['Invoice_amount'] > 999) & (df['Invoice_amount'] < 5001)), 'Priority_flag'] = 3
        df.loc[((df['2digitsMissKeying'] == 1) & (df['Invoice_amount'] > 5000)), 'Priority_flag'] = 3
        df.loc[((df['2digitsMissKeying'] == 1) & (df['flag_invoiceno_diff_within_999'] == 1) & (
                    df['Invoice_amount'] > 999) & (df['Invoice_amount'] < 5001)), 'Priority_reason'] = "2digitsMissKeying"
        df.loc[((df['2digitsMissKeying'] == 1) & (df['Invoice_amount'] > 5000)), 'Priority_reason'] = "2digitsMissKeying"
        df['1LetterAway'] = None

        def op_has_letter(src, dst, op, i, j):
            if op == 'insert':
                return dst[j].isalpha()
            elif op == "replace":
                return dst[j].isalpha()
            elif op == "delete":
                return src[i].isalpha()
            return False

        for mrn in df['Matched_Record_Number'].unique():
            mask = df['Matched_Record_Number'] == mrn
            sub_df = df[mask]
            uni = sub_df['Supplier_Invoice_Number'].dropna().unique()
            if len(uni) == 2:
                s1, s2 = str(uni[0]), str(uni[1])
                lev_dist = Levenshtein.distance(s1, s2)
                if lev_dist == 1:
                    op1, i1, j1 = Levenshtein.editops(s1, s2)[0]
                    ok = op_has_letter(s1, s2, op1, i1, j1)

                    if not ok:
                        op2, i2, j2 = Levenshtein.editops(s2, s1)[0]
                        ok = op_has_letter(s2, s1, op2, i2, j2)
                    if not ok:
                        lev_dist = 2
                df.loc[mask, '1LetterAway'] = lev_dist
        print(df[df['1LetterAway'] == 1].shape)
        print(df[df['1LetterAway'] == 1]['Confirmed'].value_counts())
        df.loc[df['1LetterAway'] == 1, 'Priority_flag'] = 2
        df.loc[df['1LetterAway'] == 1, 'Priority_reason'] = "1LetterAway"

        def max_consecutive_same(s1, s2):
            m = len(s1)
            n = len(s2)
            max_len = 0
            end = 0
            dp = [[0] * (n + 1) for _ in range(m + 1)]
            for i in range(m):
                for j in range(n):
                    if s1[i] == s2[j]:
                        dp[i + 1][j + 1] = dp[i][j] + 1
                        if dp[i + 1][j + 1] > max_len:
                            max_len = dp[i + 1][j + 1]
                            end = i + 1
            return max_len

        df['A'] = None
        df['B'] = None
        df['TotInside'] = 0
        for mrn in df['Matched_Record_Number'].unique():
            mask = df['Matched_Record_Number'] == mrn
            sub = df.loc[mask]
            unique_suppl = sub['Supplier_Invoice_Number'].unique()
            if len(unique_suppl) == 2:
                s1, s2 = str(unique_suppl[0]), str(unique_suppl[1])
                lcs_len = max_consecutive_same(s1, s2)
                df.loc[mask, 'A'] = lcs_len
                df.loc[mask, 'B'] = sub['Supplier_Invoice_Number'].astype(str).apply(
                    lambda x: lcs_len / len(x) if len(x) > 0 else 0)
                inside_f = int(np.isclose(df.loc[mask, 'B'].astype(float), 1.0).any())
                df.loc[mask, 'TotInside'] = inside_f
        print(df[df['TotInside'] == 1].shape)
        print(df[df['TotInside'] == 1]['Confirmed'].value_counts())
        df.loc[df['TotInside'] == 1, 'Priority_flag'] = 2
        df.loc[df['TotInside'] == 1, 'Priority_reason'] = "OneWithinAnother"
        df['Levenshtein'] = None
        for mrn in df['Matched_Record_Number'].unique():
            mask = df['Matched_Record_Number'] == mrn
            sub_df = df[mask]
            sub_df = sub_df[~sub_df['Supplier_Invoice_Number'].astype(str).str.contains('e+')]
            sub_df = sub_df[~sub_df['Supplier_Invoice_Number'].astype(str).str.contains('e-')]
            uni = sub_df['Supplier_Invoice_Number'].dropna().unique()
            # print(uni)
            if len(uni) == 2:
                lev_dist = Levenshtein.distance(str(uni[0]).replace(" ", ""), str(uni[1]).replace(" ", ""))
                df.loc[mask, 'Levenshtein'] = lev_dist
            if len(uni) == 1:
                # print("yes")
                df.loc[mask, 'Levenshtein'] = 0
        # print(df[df['Levenshtein']==0].shape)
        # print(df[df['Levenshtein']==0]['Confirmed'].value_counts())
        df.loc[df['Levenshtein'] == 0, 'Priority_flag'] = 2
        df.loc[df['Levenshtein'] == 0, 'Priority_reason'] = "IdenticalSupplierInvoiceNumber"
        import math
        df['ExtraInMiddle'] = 0
        m = df['1LetterAway'] == 1
        for mrn, g in df[m].groupby('Matched_Record_Number'):
            s = g['Supplier_Invoice_Number'].dropna().astype(str).unique()
            if len(s) != 2:
                continue
            a, b = s
            ops = editops(b, a)
            if not ops:
                continue
            _, i, j = ops[0]
            pos = max(i, j)
            L = max(len(a), len(b))
            left = math.ceil(L / 5)
            right = math.floor(2 * L / 3) - 1
            if left <= pos <= right:
                df.loc[g.index, 'ExtraInMiddle'] = 1

        df.loc[df['ExtraInMiddle'] == 1, 'Priority_flag'] = 1
        df.loc[df['ExtraInMiddle'] == 1, 'Priority_reason'] = "1LetterAwayInMiddle"
        mask_id = df['Priority_reason'].eq('IdenticalSupplierInvoiceNumber')
        mrns_with_diff_chqn = (
            df.loc[mask_id].groupby('Matched_Record_Number')['Invoice_Date'].nunique().loc[lambda s: s > 1].index)
        mrns_with_same_supplier = (
            df.loc[mask_id].groupby('Matched_Record_Number')['Supplier'].nunique().loc[lambda s: s.eq(1)].index)
        mrns_with_diff_chqn = mrns_with_diff_chqn.intersection(mrns_with_same_supplier)
        rows_to_update = mask_id & df['Matched_Record_Number'].isin(mrns_with_diff_chqn)
        df.loc[rows_to_update, 'Priority_reason'] = "IdenticalSupplierInvoiceNumberWithDiffDates"

        # ADD EXTRAC CONDITION (MANDATORY) THAT SUPPLIERS MUST BE SAME
        ## UPDATED
        mask_id = df['Priority_reason'].eq('IdenticalSupplierInvoiceNumber')
        mrns_with_diff_chqn = (df.loc[mask_id & df['supplier_error_flag'].eq(1), 'Matched_Record_Number'].unique())
        rows_to_update = mask_id & df['Matched_Record_Number'].isin(mrns_with_diff_chqn)
        df.loc[rows_to_update, 'Priority_flag'] = 1
        df.loc[rows_to_update, 'Priority_reason'] = "IdenticalSupplierInvoiceNumberWithDiffSuppliers"
        df['ChangeOf1Symbol'] = None

        def is_approved_change(c1, c2):
            if c1 == c2:
                return False
            if c1.isalpha() and c2.isalpha():
                return False
            if c1.isdigit() and c2.isdigit():
                return False
            if (c1.isalpha() and c2.isdigit()) or (c1.isdigit() and c2.isalpha()):
                return False

            def is_special(ch):
                return (not ch.isalnum()) or ch.isspace()

            if (c1.isalpha() and is_special(c2)) or (c2.isalpha() and is_special(c1)):
                return True
            if (c1.isdigit() and is_special(c2)) or (c2.isdigit() and is_special(c1)):
                return True
            if is_special(c1) and is_special(c2):
                return True
            return False

        for mrn in df['Matched_Record_Number'].unique():
            mask = df['Matched_Record_Number'] == mrn
            sub_df = df[mask]
            uni = sub_df['Supplier_Invoice_Number'].dropna().unique()
            if len(uni) == 2:
                s1, s2 = str(uni[0]), str(uni[1])
                lev_dist = Levenshtein.distance(s1, s2)
                if lev_dist == 1:
                    op1, i1, j1 = Levenshtein.editops(s1, s2)[0]
                    if op1 == 'replace':
                        c1, c2 = s1[i1], s2[j1]
                        if is_approved_change(c1, c2):
                            df.loc[mask, 'ChangeOf1Symbol'] = lev_dist
        print(df[df['ChangeOf1Symbol'] == 1].shape)
        print(df[df['ChangeOf1Symbol'] == 1]['Confirmed'].value_counts())
        df.loc[df['ChangeOf1Symbol'] == 1, 'Priority_flag'] = 1
        df.loc[df['ChangeOf1Symbol'] == 1, 'Priority_reason'] = "ChangeOf1Symbol"
        df['1NonLetterAway'] = None
        for mrn in df['Matched_Record_Number'].unique():
            mask = df['Matched_Record_Number'] == mrn
            sub_df = df[mask]
            uni = sub_df['Supplier_Invoice_Number'].dropna().unique()
            if len(uni) == 2:
                s1, s2 = str(uni[0]), str(uni[1])
                lev_dist = Levenshtein.distance(s1, s2)
                if lev_dist == 1:
                    op, i, j = Levenshtein.editops(s1, s2)[0]
                    if op == 'replace':
                        lev_dist = 2
                    else:
                        ch = s2[j] if op == "insert" else s1[i]
                        if ch.isalpha():
                            lev_dist = 2

                df.loc[mask, '1NonLetterAway'] = lev_dist
        print(df[df['1NonLetterAway'] == 1].shape)
        print(df[df['1NonLetterAway'] == 1]['Confirmed'].value_counts())
        df.loc[df['1NonLetterAway'] == 1, 'Priority_flag'] = 1
        df.loc[df['1NonLetterAway'] == 1, 'Priority_reason'] = "1NonLetterAway"
        df['LeadingZero'] = None

        def max_consecutive_same(s1, s2):
            m = len(s1)
            n = len(s2)
            max_len = 0
            end = 0
            dp = [[0] * (n + 1) for _ in range(m + 1)]
            for i in range(m):
                for j in range(n):
                    if s1[i] == s2[j]:
                        dp[i + 1][j + 1] = dp[i][j] + 1
                        if dp[i + 1][j + 1] > max_len:
                            max_len = dp[i + 1][j + 1]
                            end = i + 1
            return max_len

        df['A'] = None
        df['B'] = None
        df['Inside'] = 0

        def leading_zero_count(s):
            s = str(s)
            return len(s) - len(s.lstrip('0'))

        for mrn in df['Matched_Record_Number'].unique():
            mask = df['Matched_Record_Number'] == mrn
            sub = df.loc[mask]
            unique_suppl = sub['Supplier_Invoice_Number'].unique()
            if len(unique_suppl) == 2:
                s1, s2 = str(unique_suppl[0]), str(unique_suppl[1])
                lcs_len = max_consecutive_same(s1, s2)
                df.loc[mask, 'A'] = lcs_len
                df.loc[mask, 'B'] = sub['Supplier_Invoice_Number'].astype(str).apply(
                    lambda x: lcs_len / len(x) if len(x) > 0 else 0)
                inside_flag = int(np.isclose(df.loc[mask, 'B'].astype(float), 1.0).any())
                z1 = leading_zero_count(s1)
                z2 = leading_zero_count(s2)
                has_diff_lz = (z1 != z2)
                inside_f = int(inside_flag and has_diff_lz)
                df.loc[mask, 'Inside'] = inside_f
        print(df[df['Inside'] == 1].shape)
        print(df[df['Inside'] == 1]['Confirmed'].value_counts())
        df.loc[df['Inside'] == 1, 'Priority_flag'] = 1
        df.loc[df['Inside'] == 1, 'Priority_reason'] = "LeadingZero"
        df = df.drop(['A', 'B', 'C', 'ExtraInMiddle', 'Levenshtein', '2digitsMissKeying', '1LetterAway', 'TotInside',
                      '1NonLetterAway', 'Inside'], axis=1)
        return df
    def getDataFromDatabase():
        a = readEncryptedConfig(
            r'/home/ubuntu/anomaly_data_pipeline/Data_Engg_Data_Science_Pre_Prod/Anomaly/Config/Paths.xls', env_local)
        user = a['user']
        pwd = a['password']
        password = urllib.parse.quote_plus(pwd)
        ip = a['host']
        schema = "anomaly"
        engine = create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.format(user, password, ip, schema))
        query = "SELECT * FROM anomaly.duplicate_ap_invoice"
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
        return df

    df = getDataFromDatabase()
    df['Invoice_amount'] = pd.to_numeric(df.Invoice_amount, errors = 'coerce')
    df['MRN_OLD_SEMEN'] = df['Matched_Record_Number']
    df['Matched_Record_Number'] = df['new_matched_record_number']
    try:
        df['flag_invoiceno_diff_within_999'] = df['flag_invoiceno_diff_within_1000']
    except Exception as e:
        print(e)
    try:
        df['Check_Number'] = df['Check_Number_trimmed']
    except Exception as e:
        print(e)
    df = priority_flag(df)
    df['Priority_reason_base'] = df['Priority_reason']
    df['Priority_flag_base'] = df['Priority_flag']
    df['Supplier_Invoice_Number_orig'] = df['Supplier_Invoice_Number']
    df['Supplier_Invoice_Number'] = df['Supplier_Invoice_Number'].astype('string').str.replace(r'[oO]', '0', regex=True).str.replace(r'[Ili]', '1', regex=True).str.replace(r'[B]', '8', regex=True)
    df = priority_flag(df)
    df['Supplier_Invoice_Number'] = df['Supplier_Invoice_Number_orig']
    df['Priority_reason_additional'] = df['Priority_reason']
    df['Priority_flag_additional'] = df['Priority_flag']
    df = df.drop(['Supplier_Invoice_Number_orig'], axis=1)
    df['Matched_Record_Number'] = df['MRN_OLD_SEMEN']
    df = df.drop(['MRN_OLD_SEMEN', 'Priority_flag', 'Priority_reason', 'ChangeOf1Symbol', 'LeadingZero'], axis=1)
    df = df.drop(['flag_invoiceno_diff_within_999'], axis=1)

    df['h'] = 0
    df.loc[df['Priority_flag_additional'].fillna(9) < df['Priority_flag_base'].fillna(9), 'h'] = 1
    df.loc[df['h'] == 1, 'Priority_reason_base'] = "Transition"
    df.rename(columns={'Priority_reason_base': 'Priority_reason'}, inplace=True)
    df.rename(columns={'Priority_flag_additional': 'Priority_flag'}, inplace=True)
    df = df.drop(['Priority_flag_base', 'Priority_reason_additional', 'h'], axis=1)

    def uploadOutput(output):
        a = readEncryptedConfig(
            r'/home/ubuntu/anomaly_data_pipeline/Data_Engg_Data_Science_Pre_Prod/Anomaly/Config/Paths.xls', env_local)
        user = a['user']
        pwd = a['password']
        password = urllib.parse.quote_plus(pwd)
        ip = a['host']
        query = "SELECT * FROM anomaly.duplicate_ap_invoice"
        schema = "anomaly"
        engine = create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.format(user, password, ip, schema))
        table_name = 'duplicate_ap_invoice' #output
        output.to_sql(table_name, con = engine, if_exists='replace', index=False, chunksize=5000)
    uploadOutput(df)

def main():
    PRIORITY_E2E()
    return


# In[8]:


if __name__ == "__main__":

    main()
    exit()
