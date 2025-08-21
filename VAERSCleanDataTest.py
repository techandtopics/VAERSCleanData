'''
===================  VAERSCleanDataTest  ===================
    This file is meant to test the VAERSCleanData program. These 
    tests are meant to document and test the expected behavior for 
    the methods in the program. 
    
    Test failure indicates a possible issue with the code, data, or
    especially the libraries. If you encounter a test failure, please
    use the requirmeents.txt file in this project to ensure your libraries
    are the same as what this program was successfully tested with. 
    
    This code is open source and licensed under the GNU GPL v3 at:
    https://www.gnu.org/licenses/gpl-3.0.en.html.

    Copyright (c) 2025 Things and Stuff LTD
'''

from datetime import datetime
import unittest
from pyfakefs.fake_filesystem_unittest import TestCase
import VAERSCleanData
import os
import pandas as pd
import fsspec
import numpy as np

class VAERSCleanDataTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()
        self.fs.add_real_directory(os.path.dirname('./TestData'), lazy_read=False, read_only=False)
        VAERSCleanData.original_dir_name = './TestData/Data/'
        VAERSCleanData.clean_dir_name = './TestData/CleanData/'
        VAERSCleanData.output_dir_name = './TestData/TotalCleanData/'
        VAERSCleanData.begin_year = 2019
        VAERSCleanData.stop_year = 2020 
        VAERSCleanData.non_domestic_flag = True

    def tearDown(self):
        pass
    
    def test_get_list_of_files_one_year(self):
        expected_file_list = [['./TestData/Data/2019VAERSDATA.csv','./TestData/Data/2019VAERSSYMPTOMS.csv','./TestData/Data/2019VAERSVAX.csv']]
        dir_name = './TestData/Data/'
        start_year = 2019
        end_year = 2019
        non_domestic_flag = False
        file_list = VAERSCleanData.get_list_of_files(dir_name, start_year, end_year, non_domestic_flag)
        self.assertEqual(expected_file_list, file_list)

    def test_get_list_of_files_multi_year(self):
        expected_file_list = [['./TestData/Data/2019VAERSDATA.csv','./TestData/Data/2019VAERSSYMPTOMS.csv','./TestData/Data/2019VAERSVAX.csv'],['./TestData/Data/2020VAERSDATA.csv','./TestData/Data/2020VAERSSYMPTOMS.csv','./TestData/Data/2020VAERSVAX.csv']]
        dir_name = './TestData/Data/'
        start_year = 2019
        end_year = 2020
        non_domestic_flag = False
        file_list = VAERSCleanData.get_list_of_files(dir_name, start_year, end_year, non_domestic_flag)        
        self.assertEqual(expected_file_list, file_list)

    def test_get_list_of_files_non_domestic(self):
        self.maxDiff = None
        expected_file_list = [['./TestData/Data/2019VAERSDATA.csv','./TestData/Data/2019VAERSSYMPTOMS.csv','./TestData/Data/2019VAERSVAX.csv'],['./TestData/Data/NonDomesticVAERSDATA.csv','./TestData/Data/NonDomesticVAERSSYMPTOMS.csv','./TestData/Data/NonDomesticVAERSVAX.csv']]
        dir_name = './TestData/Data/'
        start_year = 2019
        end_year = 2019
        non_domestic_flag = True
        file_list = VAERSCleanData.get_list_of_files(dir_name, start_year, end_year, non_domestic_flag)        
        self.assertEqual(expected_file_list, file_list)

    def test_scrub_file_vax(self):
        out_dir = 'C://fake_dir/'
        os.mkdir(out_dir)
        in_file = './TestData/Data/testVAERSVAX.csv'
        expected_file = 'C://fake_dir/testVAERSVAX.csv'

        VAERSCleanData.scrub_file(in_file, out_dir)         
        self.assertTrue(os.path.exists(expected_file))    

        test_items = [['@', '@ was not replaced'],
                        ['#', '# was not replaced'],
                        ['\"', '\" was not replaced'],
                        ['\'', '\' was not replaced'],
                        ['&', '& was not replaced'],
                        ['-', '- was not replaced'],
                        [';', '; was not replaced'],
                        [':', ': was not replaced'],
                        ['~', '~ was not replaced']]

        dataframe = pd.read_csv(expected_file)
        dataframe = dataframe.astype(str)     
        
        for item in test_items:
            filter = np.column_stack([dataframe[col].str.contains(item[0], na=False) for col in dataframe]) 
            result = dataframe.loc[filter.any(axis=1)]
            if len(result) > 0:
                self.fail(item[1])

    def test_scrub_file_symptom(self):
        out_dir = 'C://fake_dir/'
        os.mkdir(out_dir)
        in_file = './TestData/Data/testOther.csv'
        expected_file = 'C://fake_dir/testOther.csv'

        VAERSCleanData.scrub_file(in_file, out_dir)         
        self.assertTrue(os.path.exists(expected_file))    

        test_items = [['@', '@ was not replaced'],
                        ['#', '# was not replaced'],
                        ['\"', '\" was not replaced'],
                        ['\'', '\' was not replaced'],
                        ['&', '& was not replaced'],
                        ['-', '- was not replaced'],
                        [';', '; was not replaced'],
                        [':', ': was not replaced'],
                        ['~', '~ was not replaced']]

        dataframe = pd.read_csv(expected_file)
        dataframe = dataframe.astype(str)     
        
        for item in test_items:
            filter = np.column_stack([dataframe[col].str.contains(item[0], na=False) for col in dataframe]) 
            result = dataframe.loc[filter.any(axis=1)]
            print(str(result))
            if len(result) > 0:
                self.fail(item[1])


    def test_append_files_single(self):
        out_dir = 'C://fake_dir/'
        in_files = ['./TestData/CleanData/2019VAERSDATA.csv']
        os.mkdir(out_dir)
        VAERSCleanData.append_files(in_files, out_dir)
        self.assertTrue(os.path.exists('C://fake_dir/TotalVAERSData.csv'))
        #TODO make sure data is there
    
    def test_append_files_multiple(self):
        out_dir = 'C://fake_dir/'
        in_files = ['./TestData/CleanData/2019VAERSDATA.csv']
        os.mkdir(out_dir)
        VAERSCleanData.append_files(in_files, out_dir)
        self.assertTrue(os.path.exists('C://fake_dir/TotalVAERSData.csv'))
        #TODO make sure data is there

    def test_combine_files(self):
        out_dir = 'C://fake_dir/'
        in_files = [['./TestData/CleanData/2019VAERSDATA.csv','./TestData/CleanData/2019VAERSSYMPTOMS.csv','./TestData/CleanData/2019VAERSVAX.csv'],['./TestData/CleanData/NonDomesticVAERSDATA.csv','./TestData/CleanData/NonDomesticVAERSSYMPTOMS.csv','./TestData/CleanData/NonDomesticVAERSVAX.csv']]
        os.mkdir(out_dir)
        expected_file_2019 = 'C://fake_dir/2019VAERS.csv'
        expected_file_non_domestic = 'C://fake_dir/NonDomesticVAERS.csv'

        VAERSCleanData.combine_files(in_files, out_dir)
        self.assertTrue(os.path.exists(expected_file_2019))
        self.assertTrue(os.path.exists(expected_file_non_domestic))

        df_2019 = pd.read_csv(expected_file_2019, engine='python', on_bad_lines='skip')
        columns_2019 = df_2019.columns.to_list()
        #self.assertEqual()

        df_non_domestic = pd.read_csv(expected_file_non_domestic, engine='python', on_bad_lines='skip')
        columns_non_domestic = df_non_domestic.columns.tolist()
        #self.assertEqual()
        #TODO make sure data is there

    def test_combine_symptoms(self):
        pass
        # VAERSCleanData.combine_symptoms('./TestData/CleanData/2019VAERSSYMPTOMS.csv') TODO make sure call doesn't overwrite test data
        # expect headers 
        # expect no duplicates
        # expect data to be there

        #     def combine_symptoms(file):
        # additional_headers = ['SYMPTOM6', 'SYMPTOMVERSION6',
        #                     'SYMPTOM7', 'SYMPTOMVERSION7',
        #                     'SYMPTOM8', 'SYMPTOMVERSION8',
        #                     'SYMPTOM9', 'SYMPTOMVERSION9',
        #                     'SYMPTOM10', 'SYMPTOMVERSION10',
        #                     'SYMPTOM11', 'SYMPTOMVERSION11',
        #                     'SYMPTOM12', 'SYMPTOMVERSION12',
        #                     'SYMPTOM13', 'SYMPTOMVERSION13',
        #                     'SYMPTOM14', 'SYMPTOMVERSION14',
        #                     'SYMPTOM15', 'SYMPTOMVERSION15',
        #                     'SYMPTOM16', 'SYMPTOMVERSION16',
        #                     'SYMPTOM17', 'SYMPTOMVERSION17',
        #                     'SYMPTOM18', 'SYMPTOMVERSION19',
        #                     'SYMPTOM20', 'SYMPTOMVERSION20',
        #                     'SYMPTOM21', 'SYMPTOMVERSION21',
        #                     'SYMPTOM22', 'SYMPTOMVERSION22',
        #                     'SYMPTOM23', 'SYMPTOMVERSION23',
        #                     'SYMPTOM24', 'SYMPTOMVERSION24',
        #                     'SYMPTOM25', 'SYMPTOMVERSION25',
        #                     'SYMPTOM26', 'SYMPTOMVERSION26',
        #                     'SYMPTOM27', 'SYMPTOMVERSION27',
        #                     'SYMPTOM28', 'SYMPTOMVERSION28',
        #                     'SYMPTOM29', 'SYMPTOMVERSION29',
        #                     'SYMPTOM30', 'SYMPTOMVERSION30',
        #                     'SYMPTOM31', 'SYMPTOMVERSION31',
        #                     'SYMPTOM32', 'SYMPTOMVERSION32',
        #                     'SYMPTOM33', 'SYMPTOMVERSION33',
        #                     'SYMPTOM34', 'SYMPTOMVERSION34',
        #                     'SYMPTOM35', 'SYMPTOMVERSION35']
        
        # df = pd.read_csv(file, engine='python', on_bad_lines='skip', encoding=file_encoding, dtype=str, na_filter=False) #drop records with errors
        # # get a unique list of the IDs
        # id_list = list(df['VAERS_ID'])
        # id_list = list(dict.fromkeys(id_list)) 
        # # add the new headers to support 25 symptoms per record
        # new_columns = df.columns.values  
        # new_columns = np.append(new_columns,additional_headers)      
        # df_out = pd.DataFrame(columns=new_columns)
        # out_rows = pd.DataFrame(columns=new_columns)
        # in_rows = pd.DataFrame()
        # # for each record, write the row if it's the only one found for that ID. Otherwise combine the rows TODO find more effiecent
        # for record in id_list:
        #     in_rows = df.loc[df['VAERS_ID'] == record]         
        #     in_rows.set_index("VAERS_ID", inplace=True) 
        #     count = 0  
        #     out_rows = pd.DataFrame(columns=new_columns) 
        #     out_rows.set_index("VAERS_ID", inplace=True)   
        #     for index, row in in_rows.iterrows(): 
        #         if count == 0:
        #             out_rows.at[record,'VAERS_ID'] = record
        #             out_rows.at[record,'SYMPTOM1'] = in_rows.iat[0,0]
        #             out_rows.at[record,'SYMPTOMVERSION1'] = in_rows.iat[0,1]
        #             out_rows.at[record,'SYMPTOM2'] = in_rows.iat[0,2]
        #             out_rows.at[record,'SYMPTOMVERSION2'] = in_rows.iat[0,3]
        #             out_rows.at[record,'SYMPTOM3'] = in_rows.iat[0,4]
        #             out_rows.at[record,'SYMPTOMVERSION3'] = in_rows.iat[0,5]
        #             out_rows.at[record,'SYMPTOM4'] = in_rows.iat[0,6]                
        #             out_rows.at[record,'SYMPTOMVERSION4'] = in_rows.iat[0,7]
        #             out_rows.at[record,'SYMPTOM5'] = in_rows.iat[0,8]                
        #             out_rows.at[record,'SYMPTOMVERSION5'] = in_rows.iat[0,9]
        #         else:
        #             if count == 1:
        #                 header_range = range(6,10)
        #                 start_index = 6
        #             elif count == 2:
        #                 header_range = range(11,15)
        #                 start_index = 11
        #             elif count == 3:
        #                 header_range = range(16,20)
        #                 start_index = 16
        #             elif count == 4:
        #                 header_range = range(21,25)
        #                 start_index = 21
        #             elif count == 5:
        #                 header_range = range(26,30)
        #                 start_index = 26
        #             elif count == 6:
        #                 header_range = range(31,35)
        #                 start_index = 31
        #             else:
        #                 print('error - more than 35 symptoms for this id ' + str(record))

        #             # map the five symptoms from the current record to the combined record
        #             for i in header_range:
        #                 symptom_header = 'SYMPTOM' + str(i)
        #                 version_header = 'SYMPTOMVERSION' + str(i)

        #                 # get the indices to map it to
        #                 if i == start_index:
        #                     symptom_location = 0
        #                     version_location = 1

        #                 # combine the data for record to be writen to the new file
        #                 new_symptom = in_rows.iat[count,symptom_location]
        #                 new_version = in_rows.iat[count,version_location]                 
        #                 out_rows.at[record,symptom_header] = new_symptom
        #                 out_rows.at[record,version_header] = new_version
                        
        #                 symptom_location += 2
        #                 version_location += 2

        #         count += 1 

        #     pd.concat([df_out, out_rows], ignore_index=True) #append

        # df_out.set_index("VAERS_ID", inplace=True) 
        # df_out.to_csv(file) 

    def test_combine_vax_records(self):
        pass
        # VAERSCleanData.combine_vax_records('./TestData/CleanData/NonDomesticVAERSVAX.csv') TODO make sure call doesn't overwrite test data
        # expect headers 
        # expect no duplicates
        # expect data to be there

        # def combine_vax_records(file):
        # print('processing ' + file)
        # headers = ['VAERS_ID', 'VAX_TYPE_1', 'VAX_MANU_1', 'VAX_LOT_1', 'VAX_DOSE_SERIES_1','VAX_ROUTE_1', 'VAX_SITE_1', 'VAX_NAME_1',
        #            'VAX_TYPE_2', 'VAX_MANU_2', 'VAX_LOT_2', 'VAX_DOSE_SERIES_2','VAX_ROUTE_2', 'VAX_SITE_2', 'VAX_NAME_2',
        #            'VAX_TYPE_3', 'VAX_MANU_3', 'VAX_LOT_3', 'VAX_DOSE_SERIES_3','VAX_ROUTE_3', 'VAX_SITE_3', 'VAX_NAME_3',
        #            'VAX_TYPE_4', 'VAX_MANU_4', 'VAX_LOT_4', 'VAX_DOSE_SERIES_4','VAX_ROUTE_4', 'VAX_SITE_4', 'VAX_NAME_4',
        #            'VAX_TYPE_5', 'VAX_MANU_5', 'VAX_LOT_5', 'VAX_DOSE_SERIES_5','VAX_ROUTE_5', 'VAX_SITE_5', 'VAX_NAME_5',
        #            'VAX_TYPE_6', 'VAX_MANU_6', 'VAX_LOT_6', 'VAX_DOSE_SERIES_6','VAX_ROUTE_6', 'VAX_SITE_6', 'VAX_NAME_6']

        # df_out = pd.DataFrame(columns=headers)
        # file_encoding = get_file_encoding(file)
        # df = pd.read_csv(file, engine='python', on_bad_lines='skip', encoding=file_encoding, dtype=str, na_filter=False) #drop records with errors  
            
        # # get a unique list of the IDs
        # id_list = list(df['VAERS_ID'])
        # id_list = list(dict.fromkeys(id_list)) 

        # in_rows = pd.DataFrame()    
        # # for each record, write the row if it's the only one found for that ID. Otherwise combine the rows
        # for record in id_list:
        #     out_rows = pd.DataFrame(columns=headers)
        #     in_rows = df.loc[df['VAERS_ID'] == record] 
        #     in_rows.set_index("VAERS_ID", inplace=True)
        #     count = 1
        #     for index, row in in_rows.iterrows(): 
        #         if count == 1:
        #             out_rows.at[record,'VAERS_ID'] = record
        #             out_rows.at[record,'VAX_TYPE_1'] = in_rows.iat[0,0]
        #             out_rows.at[record,'VAX_MANU_1'] = in_rows.iat[0,1]
        #             out_rows.at[record,'VAX_LOT_1'] = in_rows.iat[0,2]
        #             out_rows.at[record,'VAX_DOSE_SERIES_1'] = in_rows.iat[0,3]
        #             out_rows.at[record,'VAX_ROUTE_1'] = in_rows.iat[0,4]
        #             out_rows.at[record,'VAX_SITE_1'] = in_rows.iat[0,5]
        #             out_rows.at[record,'VAX_NAME_1'] = in_rows.iat[0,6]
        #             df_out = pd.concat([df_out,out_rows], ignore_index=True) #append
        #         else:
        #             if count > 6:
        #                 print('error - more than 6 vaccines for this id ' + str(record))
        #             else:
        #                 # map the current record to the combined record
        #                 str_count = str(count)
        #                 vax_type = 'VAX_TYPE_' + str_count
        #                 vax_manu = 'VAX_MANU_' + str_count
        #                 vax_lot = 'VAX_LOT_' + str_count
        #                 vax_series = 'VAX_DOSE_SERIES_' + str_count
        #                 vax_route = 'VAX_ROUTE_' + str_count
        #                 vax_site = 'VAX_SITE_' + str_count 
        #                 vax_name = 'VAX_NAME_' + str_count

        #                 count_index = count - 1
        #                 location = 0

        #                 # combine the data for record to be writen to the new file
        #                 df_out.at[record,vax_type] = in_rows.iat[count_index,location]
        #                 df_out.at[record,vax_manu] = in_rows.iat[count_index,location+1]
        #                 df_out.at[record,vax_lot] = in_rows.iat[count_index,location+2]
        #                 df_out.at[record,vax_series] = in_rows.iat[count_index,location+3]
        #                 df_out.at[record,vax_route] = in_rows.iat[count_index,location+4]
        #                 df_out.at[record,vax_site] = in_rows.iat[count_index,location+5]
        #                 df_out.at[record,vax_name] = in_rows.iat[count_index,location+6]
                
        #         count += 1                
                
        # #change to new dataframe   
        # df_out.set_index("VAERS_ID", inplace=True) 
        # df_out.to_csv(file)

    def test_add_file_if_exists_new(self):
        expected_file_list = ['C://fake_dir/test_file.csv']
        os.mkdir('C://fake_dir')
        file_path = 'C://fake_dir/test_file.csv'
        with open(file_path, 'w') as f:
            f.write('test')

        self.assertEqual(expected_file_list, VAERSCleanData.add_file_if_exists('C://fake_dir/test_file.csv', []))
        
    def test_add_file_if_exists_existing(self):
        expected_file_list = ['C://fake_dir/random_file.csv', 'C://fake_dir/test_file.csv']
        os.mkdir('C://fake_dir')
        file_path = 'C://fake_dir/test_file.csv'
        with open(file_path, 'w') as f:
            f.write('test')

        self.assertEqual(expected_file_list, VAERSCleanData.add_file_if_exists('C://fake_dir/test_file.csv', ['C://fake_dir/random_file.csv']))

    def test_add_file_if_exists_negative(self):
        expected_file_list = []
        os.mkdir('C://fake_dir')
        file_path = 'C:/fake_dir/wrong_file.csv'
        with open(file_path, 'w') as f:
            f.write('test')

        self.assertEqual(expected_file_list, VAERSCleanData.add_file_if_exists('C:/fake_dir/test_file.csv', []))
    
    def test_add_directory_if_not_exists_existing(self):
        the_dir = 'C://fake_dir'
        os.mkdir(the_dir)
        VAERSCleanData.add_directory_if_not_exists(the_dir)
        self.assertTrue(os.path.exists(the_dir))

    def test_add_directory_if_not_exists_not_existing(self):
        the_dir = 'C://fake_dir'
        VAERSCleanData.add_directory_if_not_exists(the_dir)
        self.assertTrue(os.path.exists(the_dir))

    def test_add_trailing_slash_existing(self):
        expected_str = 'C:/fake_dir/'
        result_str = VAERSCleanData.add_trailing_slash(expected_str)
        self.assertEqual(result_str, expected_str)

    def test_add_trailing_slash_not_existing(self):
        expected_str = 'C:/fake_dir/'
        test_str = 'C:/fake_dir'
        result_str = VAERSCleanData.add_trailing_slash(test_str)
        self.assertEqual(result_str, expected_str)

    def test_correct_for_common_errors_begin_year_none(self):
        VAERSCleanData.begin_year = None
        VAERSCleanData.correct_for_common_errors()
        self.assertEqual(VAERSCleanData.begin_year, 1990)

    def test_correct_for_common_errors_stop_year_none(self):
        VAERSCleanData.stop_year = None
        VAERSCleanData.correct_for_common_errors()
        self.assertEqual(VAERSCleanData.stop_year, datetime.now().year)

    def test_correct_for_common_errors_begin_year_future(self):
        with self.assertRaises(SystemExit) as cm:
            VAERSCleanData.begin_year = (datetime.now().year + 1)
            VAERSCleanData.correct_for_common_errors()
            self.assertEqual(cm.exception, VAERSCleanData.__error_begin_year_validation__)

    def test_correct_for_common_errors_stop_year_future(self):
         with self.assertRaises(SystemExit) as cm:
            VAERSCleanData.stop_year = (datetime.now().year + 1)
            VAERSCleanData.correct_for_common_errors()
            self.assertEqual(cm.exception, VAERSCleanData.__error_stop_year_validation__)

    def test_correct_for_common_errors_begin_year_past(self):
         with self.assertRaises(SystemExit) as cm:
            VAERSCleanData.begin_year = 1989
            VAERSCleanData.correct_for_common_errors()
            self.assertEqual(cm.exception, VAERSCleanData.__error_begin_year_validation__)

    def test_correct_for_common_errors_stop_year_past(self):
        with self.assertRaises(SystemExit) as cm:
            VAERSCleanData.stop_year = 1989
            VAERSCleanData.correct_for_common_errors()
            self.assertEqual(cm.exception, VAERSCleanData.__error_stop_year_validation__)

    def test_get_file_encoding(self):
        the_encoding = 'ascii'
        os.mkdir('C://fake_dir')
        file_path = 'C:/fake_dir/test_file.txt'
        with open(file_path, 'w', the_encoding) as f:
            f.write('test')
        
        self.assertEqual(VAERSCleanData.get_file_encoding(file_path), the_encoding)

    def test_is_file_list_length_matching_true_year(self):
        file_length = 3
        name_length = 3
        description = 1991
        self.assertTrue(VAERSCleanData.is_file_list_length_matching(file_length, name_length, description))    

    def test_is_file_list_length_matching_true_non_domestic(self):
        file_length = 3
        name_length = 3
        description = 'nonDomestic'
        self.assertTrue(VAERSCleanData.is_file_list_length_matching(file_length, name_length, description))

    def test_is_file_list_length_matching_false_under(self):
        file_length = 2
        name_length = 3
        description = 1991
        self.assertFalse(VAERSCleanData.is_file_list_length_matching(file_length, name_length, description))

    def test_is_file_list_length_matching_false_over(self):
        file_length = 4
        name_length = 3
        description = 1991
        self.assertFalse(VAERSCleanData.is_file_list_length_matching(file_length, name_length, description))
    
    def test_get_file_names_containing_none(self):
        the_str = 'VAERSVAX'
        the_list = []
        expected_result = []
        result = VAERSCleanData.get_file_names_containing(the_str, the_list)
        self.assertEqual(expected_result, result)

    def test_get_file_names_containing_multi(self):
        the_str = 'VAERSVAX'
        the_list = []
        expected_result = []
        result = VAERSCleanData.get_file_names_containing(the_str, the_list)
        self.assertEqual(expected_result, result)
        

if __name__ == '__main__':
    unittest.main()