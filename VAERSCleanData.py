'''
===================  VAERSCleanData  ===================
    This program is meant to clean and combine VAERS data files for
    use in a data analysis tool, such as WEKA, Tableau, etc. 

    WARNING: This application is very resource intensive and could max
    out your CPU and memory. It is recommended that you close all other
    applications.
    
    Please verify your directory structure contains data and verify results! 
    The code currently drops a small number of records
    which have errors (approximately 15). Future plans are to fix the
    error records. Data can be downloaded from the CDC VAERS site,
    https://vaers.hhs.gov/data.html. It is recommended to download the data
    for all years, extract it, and use that folder for your data folder.

    For best performance run CPU 4+ core 3.0GHz+, 8GB+ RAM, 10GB+ available SSD
    Should run OK on slower hardware, but time will greatly increase.
    Run time is approximately 3 hours on hardware similar to what is listed
    above. Using multi-threading can reduce the time.
    
    This code is open source and licensed under the GNU GPL v3 at:
    https://www.gnu.org/licenses/gpl-3.0.en.html.

    Copyright (c) 2025 Things and Stuff LTD
'''
import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime
from multiprocessing import Process
from multiprocessing import Pool
from functools import partial
import argparse
from itertools import chain
import chardet  

#================ Constants ================
__error_begin_year_validation__ = 'Error: Start year validation error'
__error_stop_year_validation__ = 'Error: End year validation'
__error_missing_files__ = 'Error: Missing files'
#===========================================

#================ User provided variables ================
original_dir_name = 'C:/Users/Giant/Desktop/Vaccine2/Data/' 
clean_dir_name = 'C:/Users/Giant/Desktop/Vaccine2/CleanData/'
output_dir_name = 'C:/Users/Giant/Desktop/Vaccine2/TotalCleanData/'    
begin_year = None
stop_year = None
non_domestic_flag = False
multi_thread = False
#========================================================== 


'''
Get the List of all files in the directory tree 
Optional: start and end year to get files prefixed with those years
Optional: file name suffix to get one of the three file types (eg. VAERSVAX.csv)
'''
def get_list_of_files(dir_name, start_year, end_year, non_domestic_flag):
    current_year = start_year
    all_files = list()
    file_base_names = ['VAERSDATA.csv', 'VAERSSYMPTOMS.csv', 'VAERSVAX.csv']
   
    while current_year <= end_year: 
        files_to_append = []
        
        for file_name in file_base_names:
            full_name = dir_name + str(current_year) + file_name
            files_to_append = add_file_if_exists(full_name, files_to_append)
        if is_file_list_length_matching(len(files_to_append), len(file_base_names), current_year):
            all_files.append(files_to_append)
        
        current_year = current_year + 1

    files_to_append = []
    if non_domestic_flag == True:
        for file_name in file_base_names:
            full_name = dir_name + 'NonDomestic' + file_name
            files_to_append = add_file_if_exists(full_name, files_to_append)

        if is_file_list_length_matching(len(files_to_append), len(file_base_names), current_year):
            all_files.append(files_to_append)
                
    print('all files: ' + str(all_files))
    return all_files   

# Remove problematic chars and replace them with representative string
def scrub_file(in_file, out_dir):
    out_file = out_dir + (in_file.rpartition('/')[2]) 
    print('scrub: ' + in_file)  
    file_encoding = get_file_encoding(in_file)

    if 'VAERSVAX.csv' in in_file:
        dataframe = pd.read_csv(in_file, engine='python', on_bad_lines='skip', usecols=[*range(0,8)], encoding=file_encoding, dtype=str, na_filter=False) #drop records with errors, trim empty cols
    else:
        dataframe = pd.read_csv(in_file, engine='python', on_bad_lines='skip', encoding=file_encoding, dtype=str, na_filter=False) #drop records with errors

    dataframe.set_index('VAERS_ID', inplace=True) #drop the auto-numbered column

    # Replacement
    dataframe = dataframe.astype(str)
    dataframe = dataframe.replace('@', 'at', regex=True) 
    dataframe = dataframe.replace('#', 'hashtag', regex=True)
    dataframe = dataframe.replace('\'', 'quote', regex=True)
    dataframe = dataframe.replace('\"', 'quote', regex=True) 
    dataframe = dataframe.replace('&', 'and', regex=True) 
    dataframe = dataframe.replace('-', 'minus', regex=True)
    dataframe = dataframe.replace(';', 'semicolon', regex=True)
    dataframe = dataframe.replace(':', 'colon', regex=True)
    dataframe = dataframe.replace('~', ' ', regex=True)

    # Create the clean copy of the file
    print(out_file)
    dataframe.to_csv(out_file)

# Combine the cleaned files by year - Creates one file for each year of VAERS data
def combine_files(in_files, out_dir):
    for file_group in in_files:
        #Combine files for each year, join on VAERS_ID
        dataframe = pd.DataFrame()
        first_run = True
        prefix = None

        for file in file_group:
            print('combining: ' + file)
            file_encoding = get_file_encoding(file)
            df = pd.read_csv(file, engine='python', on_bad_lines='skip', encoding=file_encoding, dtype=str, na_filter=False) #drop records with errors
            if first_run == True:
                dataframe = df              
                first_run = False
            else:                
                dataframe = pd.merge(dataframe, df, how="inner", on="VAERS_ID")
            prefix = file.rpartition('/')[2]  
            prefix = prefix.split('V')[0] 

        #Combine all files with same columns - do this from data frame above        
        dataframe.set_index('VAERS_ID', inplace=True)
        dataframe.to_csv(out_dir + prefix + 'VAERS.csv')

# Combine all the clean files - creates a file with the total VAERS data 
def append_files(in_files, out_dir):
    total_dataframe = pd.DataFrame()
    first_file = True

    for file in in_files:
        print('appending ' + file)
        file_encoding = get_file_encoding(file)
        df = pd.read_csv(file, engine='python', on_bad_lines='skip', encoding=file_encoding) #drop records with errors

        if(first_file):
            total_dataframe = df
            first_file = False
        else:            
            total_dataframe = pd.concat([total_dataframe, df], ignore_index=True) #replaced append

    total_dataframe.set_index("VAERS_ID", inplace=True) 
    total_dataframe.to_csv(out_dir + 'TotalVAERSData.csv')

#Combine multiple vaccination names from the same VAERS_ID to create a single record
def combine_vax_records(file):
    print('processing ' + file)
    headers = ['VAERS_ID', 'VAX_TYPE_1', 'VAX_MANU_1', 'VAX_LOT_1', 'VAX_DOSE_SERIES_1','VAX_ROUTE_1', 'VAX_SITE_1', 'VAX_NAME_1',
               'VAX_TYPE_2', 'VAX_MANU_2', 'VAX_LOT_2', 'VAX_DOSE_SERIES_2','VAX_ROUTE_2', 'VAX_SITE_2', 'VAX_NAME_2',
               'VAX_TYPE_3', 'VAX_MANU_3', 'VAX_LOT_3', 'VAX_DOSE_SERIES_3','VAX_ROUTE_3', 'VAX_SITE_3', 'VAX_NAME_3',
               'VAX_TYPE_4', 'VAX_MANU_4', 'VAX_LOT_4', 'VAX_DOSE_SERIES_4','VAX_ROUTE_4', 'VAX_SITE_4', 'VAX_NAME_4',
               'VAX_TYPE_5', 'VAX_MANU_5', 'VAX_LOT_5', 'VAX_DOSE_SERIES_5','VAX_ROUTE_5', 'VAX_SITE_5', 'VAX_NAME_5',
               'VAX_TYPE_6', 'VAX_MANU_6', 'VAX_LOT_6', 'VAX_DOSE_SERIES_6','VAX_ROUTE_6', 'VAX_SITE_6', 'VAX_NAME_6']

    df_out = pd.DataFrame(columns=headers)
    file_encoding = get_file_encoding(file)
    df = pd.read_csv(file, engine='python', on_bad_lines='skip', encoding=file_encoding, dtype=str, na_filter=False) #drop records with errors  
          
    # get a unique list of the IDs
    id_list = list(df['VAERS_ID'])
    id_list = list(dict.fromkeys(id_list))

    in_rows = pd.DataFrame()    
    # for each record, write the row if it's the only one found for that ID. Otherwise combine the rows
    for record in id_list:
        out_rows = pd.DataFrame(columns=headers)
        in_rows = df.loc[df['VAERS_ID'] == record] 
        in_rows.set_index("VAERS_ID", inplace=True) 
        count = 1
        for index, row in in_rows.iterrows(): 
            if count == 1:
                out_rows.at[record,'VAERS_ID'] = record
                out_rows.at[record,'VAX_TYPE_1'] = in_rows.iat[0,0]
                out_rows.at[record,'VAX_MANU_1'] = in_rows.iat[0,1]
                out_rows.at[record,'VAX_LOT_1'] = in_rows.iat[0,2]
                out_rows.at[record,'VAX_DOSE_SERIES_1'] = in_rows.iat[0,3]
                out_rows.at[record,'VAX_ROUTE_1'] = in_rows.iat[0,4]
                out_rows.at[record,'VAX_SITE_1'] = in_rows.iat[0,5]
                out_rows.at[record,'VAX_NAME_1'] = in_rows.iat[0,6]
                df_out = pd.concat([df_out,out_rows], ignore_index=True) #append
            else:
                if count > 6:
                    print('error - more than 6 vaccines for this id ' + str(record))
                else:
                    # map the current record to the combined record
                    str_count = str(count)
                    vax_type = 'VAX_TYPE_' + str_count
                    vax_manu = 'VAX_MANU_' + str_count
                    vax_lot = 'VAX_LOT_' + str_count
                    vax_series = 'VAX_DOSE_SERIES_' + str_count
                    vax_route = 'VAX_ROUTE_' + str_count
                    vax_site = 'VAX_SITE_' + str_count 
                    vax_name = 'VAX_NAME_' + str_count

                    count_index = count - 1
                    location = 0

                    # combine the data for record to be writen to the new file
                    df_out.at[record,vax_type] = in_rows.iat[count_index,location]
                    df_out.at[record,vax_manu] = in_rows.iat[count_index,location+1]
                    df_out.at[record,vax_lot] = in_rows.iat[count_index,location+2]
                    df_out.at[record,vax_series] = in_rows.iat[count_index,location+3]
                    df_out.at[record,vax_route] = in_rows.iat[count_index,location+4]
                    df_out.at[record,vax_site] = in_rows.iat[count_index,location+5]
                    df_out.at[record,vax_name] = in_rows.iat[count_index,location+6]
            
            count += 1                
            
    #change to new dataframe   
    df_out.set_index("VAERS_ID", inplace=True) 
    df_out.to_csv(file)

# Need to combine symptoms from multiple entries to a single entry. Supports 25 symptoms
def combine_symptoms(file):
    additional_headers = ['SYMPTOM6', 'SYMPTOMVERSION6',
                        'SYMPTOM7', 'SYMPTOMVERSION7',
                        'SYMPTOM8', 'SYMPTOMVERSION8',
                        'SYMPTOM9', 'SYMPTOMVERSION9',
                        'SYMPTOM10', 'SYMPTOMVERSION10',
                        'SYMPTOM11', 'SYMPTOMVERSION11',
                        'SYMPTOM12', 'SYMPTOMVERSION12',
                        'SYMPTOM13', 'SYMPTOMVERSION13',
                        'SYMPTOM14', 'SYMPTOMVERSION14',
                        'SYMPTOM15', 'SYMPTOMVERSION15',
                        'SYMPTOM16', 'SYMPTOMVERSION16',
                        'SYMPTOM17', 'SYMPTOMVERSION17',
                        'SYMPTOM18', 'SYMPTOMVERSION19',
                        'SYMPTOM20', 'SYMPTOMVERSION20',
                        'SYMPTOM21', 'SYMPTOMVERSION21',
                        'SYMPTOM22', 'SYMPTOMVERSION22',
                        'SYMPTOM23', 'SYMPTOMVERSION23',
                        'SYMPTOM24', 'SYMPTOMVERSION24',
                        'SYMPTOM25', 'SYMPTOMVERSION25',
                        'SYMPTOM26', 'SYMPTOMVERSION26',
                        'SYMPTOM27', 'SYMPTOMVERSION27',
                        'SYMPTOM28', 'SYMPTOMVERSION28',
                        'SYMPTOM29', 'SYMPTOMVERSION29',
                        'SYMPTOM30', 'SYMPTOMVERSION30',
                        'SYMPTOM31', 'SYMPTOMVERSION31',
                        'SYMPTOM32', 'SYMPTOMVERSION32',
                        'SYMPTOM33', 'SYMPTOMVERSION33',
                        'SYMPTOM34', 'SYMPTOMVERSION34',
                        'SYMPTOM35', 'SYMPTOMVERSION35']
    
    print('processing ' + file)
    file_encoding = get_file_encoding(file)
    df = pd.read_csv(file, engine='python', on_bad_lines='skip', encoding=file_encoding, dtype=str) #drop records with errors

    # get a unique list of the IDs
    id_list = list(df['VAERS_ID'])
    id_list = list(dict.fromkeys(id_list)) 

    # add the new headers to support 25 symptoms per record
    new_columns = df.columns.values  
    new_columns = np.append(new_columns,additional_headers) 
    df_out = pd.DataFrame(columns=new_columns)
    out_rows = pd.DataFrame(columns=new_columns)
    in_rows = pd.DataFrame()

    # for each record, write the row if it's the only one found for that ID. Otherwise combine the rows 
    for record in id_list:
        in_rows = pd.DataFrame(df.loc[df['VAERS_ID'] == record])         
        in_rows.set_index("VAERS_ID", inplace=True) 
        count = 0  
        out_rows = pd.DataFrame(columns=new_columns) 
        out_rows.set_index("VAERS_ID", inplace=True)  
        for index, row in in_rows.iterrows(): 
            if count == 0:
                out_rows.at[record,'VAERS_ID'] = record
                out_rows.at[record,'SYMPTOM1'] = in_rows.iat[0,0]
                out_rows.at[record,'SYMPTOMVERSION1'] = in_rows.iat[0,1]
                out_rows.at[record,'SYMPTOM2'] = in_rows.iat[0,2]
                out_rows.at[record,'SYMPTOMVERSION2'] = in_rows.iat[0,3]
                out_rows.at[record,'SYMPTOM3'] = in_rows.iat[0,4]
                out_rows.at[record,'SYMPTOMVERSION3'] = in_rows.iat[0,5]
                out_rows.at[record,'SYMPTOM4'] = in_rows.iat[0,6]                
                out_rows.at[record,'SYMPTOMVERSION4'] = in_rows.iat[0,7]
                out_rows.at[record,'SYMPTOM5'] = in_rows.iat[0,8]                
                out_rows.at[record,'SYMPTOMVERSION5'] = in_rows.iat[0,9]
            else:
                if count == 1:
                    header_range = range(6,10)
                    start_index = 6
                elif count == 2:
                    header_range = range(11,15)
                    start_index = 11
                elif count == 3:
                    header_range = range(16,20)
                    start_index = 16
                elif count == 4:
                    header_range = range(21,25)
                    start_index = 21
                elif count == 5:
                    header_range = range(26,30)
                    start_index = 26
                elif count == 6:
                    header_range = range(31,35)
                    start_index = 31
                else:
                    print('error - more than 35 symptoms for this id ' + str(record))

                # map the five symptoms from the current record to the combined record
                for i in header_range:
                    symptom_header = 'SYMPTOM' + str(i)
                    version_header = 'SYMPTOMVERSION' + str(i)

                    # get the indices to map it to
                    if i == start_index:
                        symptom_location = 0
                        version_location = 1

                    # combine the data for record to be writen to the new file
                    new_symptom = in_rows.iat[count,symptom_location]
                    new_version = in_rows.iat[count,version_location]                 
                    out_rows.at[record,symptom_header] = new_symptom
                    out_rows.at[record,version_header] = new_version
                    
                    symptom_location += 2
                    version_location += 2

            count += 1 

        df_out = pd.concat([df_out, out_rows], ignore_index=True) #append

    #change to new dataframe   
    df_out.set_index("VAERS_ID", inplace=True)
    df_out.to_csv(file)
#
def is_file_list_length_matching(files_to_append_length, file_base_name_length, file_description):
    if files_to_append_length != file_base_name_length:
        print('Warning: Excluding files due to length was ' + str(files_to_append_length) + ", expected " + str(file_base_name_length) + " for " + str(file_description)) #TODO better formatting
        return False
    else:
        return True

# Take a file path and list, add the file to the list if it exists, return the list
def add_file_if_exists(the_path, file_list):
    if os.path.exists(the_path):
        file_list.append(the_path)
    else:
        print("File not found while creating list " + the_path)
    
    return file_list

# If the given directory does not exist, create it
def add_directory_if_not_exists(dir_name):
    if not os.path.exists(dir_name):
        os.mkdir(dir_name)

# Return a list of the file names that contain the desired string
def get_file_names_containing(the_str, the_list):
    matching_files = []
    for i in the_list:
        if the_str in i:
            matching_files.append(i)
    return matching_files

def add_trailing_slash(the_str):
    if not the_str.endswith('/'):
        the_str = the_str + '/'
    return the_str

# Correct for common mistakes
def correct_for_common_errors():
    global original_dir_name
    global clean_dir_name
    global output_dir_name
    global begin_year
    global stop_year
    # Add trailing slash for the directory to ensure proper concatenation    
    original_dir_name = add_trailing_slash(original_dir_name)
    clean_dir_name = add_trailing_slash(clean_dir_name)
    output_dir_name = add_trailing_slash(output_dir_name)

    # Create the output directories if they don't exist
    add_directory_if_not_exists(clean_dir_name)
    add_directory_if_not_exists(output_dir_name)

    # Set default years if none are provided and verify they are valid.
    if begin_year == None:
        begin_year = 1990
    if stop_year == None:
        stop_year = datetime.now().year
    if begin_year < 1990 or begin_year > datetime.now().year:
        print('Data is only available starting with the year 1990 up to the current year. Please verify your begin_year variable. The value provided is invalid: ' + str(begin_year))
        sys.exit(__error_begin_year_validation__)
    if stop_year < 1990 or stop_year > datetime.now().year:
        print('Data is only available starting with the year 1990 up to the current year. Please verify your stop_year variable. The value provided is invalid: ' + str(stop_year))
        sys.exit(__error_stop_year_validation__)

def get_file_encoding(file):
    rawdata = open(file, "rb").read()
    result = chardet.detect(rawdata)
    return result['encoding']

#Main function for program execution starts here
def main(): 
    print("Starting at " + datetime.now().strftime('%H:%M:%S'))     
    global original_dir_name
    global clean_dir_name
    global output_dir_name
    global begin_year
    global stop_year    

    # parser = argparse.ArgumentParser(description ='Process some integers.')
    # parser.add_argument('integers', metavar ='N', 
    #                     type = int, nargs ='+',
    #                     help ='an integer for the accumulator')

    # parser.add_argument(dest ='accumulate', 
    #                     action ='store_const',
    #                     const = sum, 
    #                     help ='sum the integers')

    # args = parser.parse_args()

    correct_for_common_errors()
          
    # Get the list of all original files for the run
    list_of_files = get_list_of_files(original_dir_name, begin_year, stop_year, non_domestic_flag)
    if len(list_of_files) < 1:
        print('No files have been found to process. Please check that you have downloaded and unzipped the VAERS files in the directory provided in the original_dir_name variable.')
        sys.exit(__error_missing_files__)

   
      
    # Create clean copies of the files 
    # if multi_thread:
        # pool = Pool()
        # scrub_file1=partial(scrub_file, out_dir=clean_dir_name)
        # pool.map(scrub_file1, list_of_files)         
        # pool.close()
    # else:
    for sub_list in list_of_files:
        for file in sub_list:
            scrub_file(file, clean_dir_name)

    # Create the list of clean files
    list_of_clean_files = get_list_of_files(clean_dir_name, begin_year, stop_year, non_domestic_flag)
    if len(list_of_files) < 1:
        print('No clean files have been found to combine. Please check the data, original_dir_name variable, and clean_dir_name variable.')
        sys.exit(__error_missing_files__)

    # Create a flat list of all the files to use in combining them
    flat_list_of_files =  list(chain.from_iterable(list_of_clean_files))
    
          #TODO fix duplicates
    # Combine the records of the Vax file to remove duplicates TODO combining: C:/Users/Giant/Desktop/Vaccine2/Data/2017VAERSDATA.csv should be using clean data list
    print("Starting vax files at " + datetime.now().strftime('%H:%M:%S'))
    vax_files = get_file_names_containing('VAERSVAX.csv', list_of_clean_files)
    #TODO get list of files from list_of_files 
    # if multi_thread:
        # pool2 = Pool()
        # pool2.map(combine_vax_records, vax_files, chunksize=1)  
        # pool2.close()
    # else:
    for sub_list in vax_files:
        for file in sub_list:
            combine_vax_records(file)
    
    # Combine the symptom records so they are all on one line
    print("Starting symptom files at " + datetime.now().strftime('%H:%M:%S'))
    symptom_files = get_file_names_containing('VAERSSYMPTOMS.csv', list_of_clean_files)
    # if multi_thread:
        # pool1 = Pool()
        # pool1.map(combine_symptoms, symptom_files, chunksize=1)         
        # pool1.close()
    # else:
    for sub_list in symptom_files:
        for file in sub_list:
            combine_symptoms(file)
    
    # Combine the three yearly files into one
    print("Combining files at " + datetime.now().strftime('%H:%M:%S'))
    combine_files(list_of_clean_files, output_dir_name)
    
    # Append all the files to create one total VAERS file
    print("Appending files at " + datetime.now().strftime('%H:%M:%S'))
    append_files(flat_list_of_files, output_dir_name)

    print("Finished at " + datetime.now().strftime('%H:%M:%S'))
      
if __name__ == '__main__':
    main()