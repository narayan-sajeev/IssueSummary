import json
import os

import pandas as pd
import tqdm

# directory containing the parsed files
DIR = '/Users/narayansajeev/Desktop/MIT/parsed_files'

# dictionary to store the number of rows with issues
rows = {}
files = {}
sources = {}


def loop_fnames(prov):
    # loop through the files in the directory and return a list of files that have been parsed
    current = []
    files = []
    curr_dir = '%s/%s' % (DIR, prov)
    for file in os.listdir(curr_dir):
        is_xl = file.endswith(('.xlsx', '.xls'))
        pkl = file + '.pkl.gz'
        is_food = not any([i in file for i in ['业', '商']])
        if is_xl and file not in current and pkl in os.listdir(curr_dir) and is_food:
            files.append(file)

    return files


def get_df(prov, fname):
    # read in the first file from the list using pandas
    return pd.read_pickle('%s/%s/%s.pkl.gz' % (DIR, prov, fname))


def get_known_cols():
    # checking column classifier
    known_cols_fn = 'known_columns.json'
    with open(known_cols_fn) as f:
        return json.load(f)


def clean(col_headers):
    # clean up the headers by removing newline, carriage return, non-breaking space and space characters
    return [n.replace('\n', '').replace('\r', '').replace('\xa0', '').replace(' ', '') for n in col_headers]


def substr_check(substr_sets, k):
    # check for substrings in the column headers
    substr_dict = {}
    for s in substr_sets:
        try:
            substr_dict[s] = any([i in k for i in substr_sets[s]])
        except TypeError:
            print(k)
            raise
    return substr_dict


def substring(df, known_cols):
    # list of column headers
    col_headers = ['抽样编号', '序号', '被抽样单位名称', '被抽样单位单位地址', '被抽样省份', '标识生产企业名称',
                   '标识生产企业地址', '食品名称', '规格型号', '生产日期/批号', '商标', '不合格项目║检验结果║标准值',
                   '分类', '公告号', '公告日期', '任务来源/项目名称', '检验机构名称', '备注']

    col_headers = clean(col_headers)

    # dictionary of substrings to check for in column headers
    substr_sets = {
        'announcement_date': ['检', '抽', '报'],
        'address': ['地址', '所在地'],
        'region': ['省', '县', '市', '区'],
        'been_sampled': ['受', '被'],
        'name': ['名称', '单位', '机构', '人'],
        'testing_agency': ['采样', '检', '抽', '委托'],
        'value_or_result': ['值', '结果', '要求'],
        'limit': ['标', '限'],
        'actual': ['测', '检', '实', '不合格'],
        'result': ['结论', '结果', '判定'],
        'not_qualified': ['不合格', '不符合'],
        'item_or_reason': ['项', '原因'],
        'produce': ['生产'],
        'illegal': ['违法']
    }

    # list of possible inspection result values
    insp_res_vals = [
        '合格',
        '合  格',
        '合格（阴性）',
        '符合',
        '不合格',
        '不符合',
        '所检项目符合标准',
        '铝不符合国家标准',
        '所检项目符合标准要求。',
        '所检项目符合国家标准或地方标准']

    # list to store unmatched column headers
    unmatched_cols = []
    # review these cols
    review_cols = []

    # check for unmatched column headers
    for term in col_headers:
        if term not in known_cols:
            unmatched_cols.append(term)

    # check substrings for unmatched columns
    for k in unmatched_cols:
        substr_dict = substr_check(substr_sets, k)
        if '日期' in k and '生产' in k:
            continue
        if '日期' in k and substr_dict['announcement_date']:
            continue
        if substr_dict['address'] and '生产' in k and not substr_dict['region']:
            continue
        if substr_dict['address'] and substr_dict['been_sampled'] and not substr_dict['region']:
            continue
        if substr_dict['name'] and substr_dict['been_sampled'] and not substr_dict['address']:
            cup = k
            for i in substr_sets['name']:
                cup = cup.replace(i, '')
            for i in substr_sets['been_sampled']:
                cup = cup.replace(i, '')
            for i in substr_sets['testing_agency']:
                cup = cup.replace(i, '')
            if len(cup) <= 1:
                continue
        if substr_dict['name'] and '生产' in k and not substr_dict['address'] and not '违法' in k:
            continue
        if substr_dict['name'] and substr_dict['testing_agency'] and not substr_dict['been_sampled']:
            cup = k
            for i in substr_sets['name']:
                cup = cup.replace(i, '')
            for i in substr_sets['testing_agency']:
                cup = cup.replace(i, '')
            if len(cup) <= 1:
                continue
        if substr_dict['value_or_result'] and substr_dict['limit']:
            if substr_dict['actual']:
                continue
            else:
                continue
        if substr_dict['value_or_result'] and substr_dict['actual']:
            continue
        if substr_dict['not_qualified'] and substr_dict['item_or_reason'] and '数' not in k:
            continue
        if '规格' in k:
            continue
        if substr_dict['result']:
            if len(df.index) >= 5:
                first_obs = df.iloc[0:4][k]
            else:
                first_obs = df.loc[:][k]
            if any([n in insp_res_vals for n in first_obs]):
                continue
        else:
            review_cols.append(k)

    return review_cols


def drop_columns(df, col_headers):
    # list of columns to keep
    cols = ['manufacturer_name', 'manufacturer_address', 'sampled_location_name', 'sampled_location_address',
            'food_name', 'specifications_model', 'announcement_date', 'production_date',
            'product_classification', 'task_source_or_project_name', 'testing_agency', 'adulterant',
            'inspection_results', 'failing_results', 'test_outcome', 'legal_limit']

    # select only the existing columns from the dataframe
    df = df[[col for col in cols if col in df.columns]]

    # drop columns that are not in the list of column headers
    drop_df = df.dropna(axis=1, how='all')

    dropped = []

    for col in df.columns:
        if col not in drop_df.columns:
            dropped.append(col)

    col_headers = sorted(col_headers)

    for _ in ['商标', '备注', '序号', '抽样编号', '购进日期', '被抽样单位省', '被抽样单位盟市', '被抽样单位所在盟市',
              '公告网址链接', '产品具体名称', '销售单位/电商']:
        try:
            col_headers.remove(_)
        except:
            pass

    if len(dropped) > 0 and len(col_headers) > 0:
        print(col_headers)
        print(dropped)

    return drop_df


def newline(df, prov):
    # Define global variables
    global rows, files, sources

    # Count the number of newlines in each row of the dataframe
    count = df.apply(lambda row: row.astype(str).str.contains('\n').any(), axis=1).sum()

    # Get the name of the current function
    name = newline.__name__

    # Update the 'rows' dictionary with the count of newlines
    rows[name] = rows.get(name, 0) + count

    # If there are newlines in the dataframe, update the 'files' and 'sources' dictionaries
    if count > 0:
        # Update the 'files' dictionary with the count of files containing newlines
        files[name] = files.get(name, 0) + 1

        # Update the 'sources' dictionary with the provenance of the data
        if name not in sources.keys():
            sources[name] = [prov]
        elif prov not in sources[name]:
            lst = sources[name]
            lst.append(prov)
            sources[name] = sorted(lst)


def tab(df, prov):
    # Define global variables
    global rows, files, sources

    # Count the number of rows in the dataframe that contain a tab character
    count = df.apply(lambda row: row.astype(str).str.contains('\t').any(), axis=1).sum()

    # Get the name of the current function
    name = tab.__name__

    # Update the 'rows' dictionary with the count
    rows[name] = rows.get(name, 0) + count

    # If there are any rows with a tab character
    if count > 0:
        # Update the 'files' dictionary with the count
        files[name] = files.get(name, 0) + 1

        # If the function name is not already in the 'sources' dictionary
        if name not in sources.keys():
            # Add it along with the 'prov' value
            sources[name] = [prov]
        # If the 'prov' value is not already associated with the function name in the 'sources' dictionary
        elif prov not in sources[name]:
            # Add it to the list of values associated with the function name
            lst = sources[name]
            lst.append(prov)
            sources[name] = sorted(lst)


def adltrnt_msrmnt(df, prov):
    # Define global variables
    global rows, files, sources

    # Check if 'adulterant' column exists in the dataframe
    if 'adulterant' in df.columns:
        # Count the number of rows where 'adulterant' contains both "/" and "g"
        count = df[df['adulterant'].str.contains("/", na=False) & df['adulterant'].str.contains("g", na=False)].shape[0]

        # Get the name of the current function
        name = adltrnt_msrmnt.__name__

        # Update the 'rows' dictionary with the count
        rows[name] = rows.get(name, 0) + count

        # If count is greater than 0, update 'files' dictionary and 'sources' dictionary
        if count > 0:
            # Update the 'files' dictionary with the count
            files[name] = files.get(name, 0) + 1

            # If current function name is not in 'sources', add it with 'prov' as its value
            if name not in sources.keys():
                sources[name] = [prov]
            # If current function name is in 'sources' but 'prov' is not in its value list, append 'prov' to the list
            elif prov not in sources[name]:
                lst = sources[name]
                lst.append(prov)
                sources[name] = sorted(lst)


def adltrnt_none(df, prov):
    # Define global variables
    global rows, files, sources

    # Check if 'adulterant' column exists in the dataframe
    if 'adulterant' in df.columns:
        # Count the number of null values in 'adulterant' column
        count = df['adulterant'].isnull().sum()

        # Get the name of the current function
        name = adltrnt_none.__name__

        # Update the 'rows' dictionary with the count of null values
        rows[name] = rows.get(name, 0) + count

        # If there are null values, update the 'files' dictionary
        if count > 0:
            files[name] = files.get(name, 0) + 1

            # If the current function name is not in 'sources', add it
            if name not in sources.keys():
                sources[name] = [prov]

            # If the provider is not in the list of sources for this function, add it
            elif prov not in sources[name]:
                lst = sources[name]
                lst.append(prov)
                sources[name] = sorted(lst)


def headers(df, prov):
    # Define global variables
    global rows, files, sources

    # Count the number of times '标称生产企业名称' appears in the dataframe
    count = df.applymap(lambda x: '标称生产企业名称' in str(x)).sum().sum()

    # Get the name of the current function
    name = headers.__name__

    # Update the 'rows' dictionary with the count
    rows[name] = rows.get(name, 0) + count

    # If '标称生产企业名称' appears in the dataframe
    if count > 0:
        # Update the 'files' dictionary with the count
        files[name] = files.get(name, 0) + 1

        # If the function name is not in the 'sources' dictionary
        if name not in sources.keys():
            # Add it along with the province name
            sources[name] = [prov]
        # If the province name is not in the list of provinces for this function
        elif prov not in sources[name]:
            # Get the list of provinces for this function
            lst = sources[name]
            # Add the new province to the list
            lst.append(prov)
            # Sort the list and update the 'sources' dictionary
            sources[name] = sorted(lst)


def none(df, prov):
    # Declare 'rows', 'files', and 'sources' as global variables
    global rows, files, sources

    # Count the number of rows in the DataFrame where all values are null
    count = df.isnull().all(axis=1).sum()

    # Get the name of the current function
    name = none.__name__

    # Update the 'rows' dictionary with the count of null rows
    rows[name] = rows.get(name, 0) + count

    # If there are any null rows
    if count > 0:
        # Update the 'files' dictionary with the count of files with null rows
        files[name] = files.get(name, 0) + 1

        # If the function name is not already in the 'sources' dictionary
        if name not in sources.keys():
            # Add the provider to the 'sources' dictionary under the function name
            sources[name] = [prov]
        # If the provider is not already listed under the function name in the 'sources' dictionary
        elif prov not in sources[name]:
            # Get the list of providers for this function name
            lst = sources[name]
            # Add the new provider to the list
            lst.append(prov)
            # Sort the list and update it in the 'sources' dictionary under the function name
            sources[name] = sorted(lst)


def test_legal_none(df, prov):
    # Global variables to store the results
    global rows, files, sources

    # Check if both 'test_outcome' and 'legal_limit' columns exist in the dataframe
    if 'test_outcome' in df.columns and 'legal_limit' in df.columns:

        # Count the number of rows where both 'test_outcome' and 'legal_limit' are None
        count = len(df[(df['test_outcome'].isna()) & (df['legal_limit'].isna())])

        # Get the name of the current function
        name = test_legal_none.__name__

        # Update the 'rows' dictionary with the count
        rows[name] = rows.get(name, 0) + count

        # If there are any such rows, update the 'files' dictionary and 'sources' dictionary
        if count > 0:

            # Update the 'files' dictionary with the count
            files[name] = files.get(name, 0) + 1

            # Update the 'sources' dictionary with the provider name
            if name not in sources.keys():
                sources[name] = [prov]

            elif prov not in sources[name]:
                lst = sources[name]
                lst.append(prov)
                sources[name] = sorted(lst)


def test_concat_legal_none(df, prov):
    # Define global variables
    global rows, files, sources

    # Check if 'test_outcome' and 'legal_limit' columns exist in the dataframe
    if 'test_outcome' in df.columns and 'legal_limit' in df.columns:

        # Count the number of rows where 'test_outcome' is not null and 'legal_limit' is null
        count = df[(~df['test_outcome'].isnull()) & (df['legal_limit'].isnull())].shape[0]

        # Get the name of the current function
        name = test_concat_legal_none.__name__

        # Update the 'rows' dictionary with the count
        rows[name] = rows.get(name, 0) + count

        # If count is greater than 0, update the 'files' dictionary and 'sources' dictionary
        if count > 0:
            files[name] = files.get(name, 0) + 1

            # If the current function name is not in the keys of 'sources', add it with the provider as its value
            if name not in sources.keys():
                sources[name] = [prov]

            # If the provider is not in the values of the current function name in 'sources', add it
            elif prov not in sources[name]:
                lst = sources[name]
                lst.append(prov)
                sources[name] = sorted(lst)


# Loop through all the files in the specified directory
for prov in tqdm.tqdm(os.listdir(DIR)):

    # Skip the '.DS_Store' file
    if prov == '.DS_Store':
        continue

    # Loop through all the file names in the current directory
    for fname in loop_fnames(prov):
        # Get the dataframe for the current file
        df = get_df(prov, fname)

        # Get the columns that contain a specific substring
        review_cols = substring(df, get_known_cols())

        # Drop the specified columns from the dataframe
        df = drop_columns(df, review_cols)

        newline(df, prov)
        tab(df, prov)
        adltrnt_msrmnt(df, prov)
        adltrnt_none(df, prov)
        headers(df, prov)
        none(df, prov)
        test_legal_none(df, prov)
        test_concat_legal_none(df, prov)

print()

# Sort the dictionary 'rows' in descending order based on its values
rows = {k: v for k, v in sorted(rows.items(), key=lambda item: item[1], reverse=True)}

# Iterate over the keys in the sorted dictionary 'rows'
for func in rows.keys():

    # Print the current key
    print(func)

    try:
        # Print the value associated with the current key in 'rows'
        print('Rows:', rows[func])
    except:
        pass

    try:
        # Print the value associated with the current key in 'files'
        print('Files:', files[func])
    except:
        pass

    try:
        # Print the sources associated with the current key, joined by commas
        print('Sources:', ', '.join(sources[func]))
    except:
        pass

    print()
