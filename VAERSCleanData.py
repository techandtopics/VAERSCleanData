'''
===================  VAERSCleanData  ===================
    This program is meant to clean and combine VAERS data files for
    use in a data analysis tool, such as WEKA. 

    WARNING: This application is very resource intensive and could max
    out your CPU and memory. It is recommended that you close all other
    applications.
    
    Please verify your directory structure (folders must exist) and results! 
    The code currently drops a small number of records
    which have errors (approximately 15). This code is also dropping
    columns and combining records in the ...VAERSVAX.csv files 
    except for VAERS_ID and VAX_TYPE. Future plans are to fix the
    error records and include the additional data in the 
    ...VAERSVAX.csv files. 

    For best performance run CPU 4+ core 2.5GHz+, 4GB+ RAM, 3GB Disk
    Should run OK on slower hardware, but time will greatly increase.
    Run time is approximately 6 hours on the hardware mentioned above.
    
    This code is open source and licensed under the GNU GPL v3 at:
    https://www.gnu.org/licenses/gpl-3.0.en.html.

    Copyright (c) 2020 Things and Stuff LTD
'''
import os
import re
import pandas as pd
import numpy as np
from datetime import datetime
from multiprocessing import Process
from multiprocessing import Pool
from functools import partial

'''
Get the List of all files in the directory tree 
Optional: start and end year to get files prefixed with those years
Optional: file name suffix to get one of the three file types (eg. VAERSVAX.csv)
'''
def getListOfFiles(dirName, startYear, endYear, fileNameSuffix, nonDomestic):
    listOfFile = os.listdir(dirName)
    allFiles = list()

    # Full file list portion
    if startYear == None and endYear == None and fileNameSuffix == None:
        for entry in listOfFile:
            fullPath = os.path.join(dirName, entry)

            # If entry is a directory then get the list of files in this directory 
            if os.path.isdir(fullPath):
                allFiles = allFiles + getListOfFiles(fullPath, None, None, None, nonDomestic)
            else:
                allFiles.append(fullPath)

    # Years portion
    if startYear != None and endYear != None and fileNameSuffix == None:
        currYear = startYear
        allFiles = list()
        while currYear <= endYear:
            filesToAppend = []
            dataFile = dirName + str(currYear) + 'VAERSDATA.csv'
            filesToAppend = addFileIfExists(dataFile, filesToAppend)
            symptomFile = dirName + str(currYear) + 'VAERSSYMPTOMS.csv'
            filesToAppend = addFileIfExists(symptomFile, filesToAppend)
            vaxFile = dirName + str(currYear) + 'VAERSVAX.csv'
            filesToAppend = addFileIfExists(vaxFile, filesToAppend)

            if len(filesToAppend) > 0:
                allFiles.append(filesToAppend)
            currYear = currYear + 1

        if nonDomestic == True:
            dataFile = dirName + 'NonDomesticVAERSDATA.csv'
            filesToAppend = addFileIfExists(dataFile, filesToAppend)
            symptomFile = dirName + 'NonDomesticVAERSSYMPTOMS.csv'
            filesToAppend = addFileIfExists(symptomFile, filesToAppend)
            vaxFile = dirName + 'NonDomesticVAERSVAX.csv'
            filesToAppend = addFileIfExists(vaxFile, filesToAppend)

            if len(filesToAppend) > 0:
                allFiles.append(filesToAppend)

    # fileNameSuffix portion
    if fileNameSuffix != None:
        if startYear == None:
            startYear = 1990
        if endYear == None or endYear > datetime.now().year:
            endYear = datetime.now().year
        currYear = startYear
        allFiles = list()
        while currYear <= endYear:
            vaxFile = dirName + str(currYear) + fileNameSuffix
            allFiles = addFileIfExists(vaxFile, allFiles)
            currYear = currYear + 1
        if nonDomestic == True:
            vaxFile = dirName + 'NonDomestic' + fileNameSuffix
            allFiles = addFileIfExists(vaxFile, allFiles)
                
    return allFiles   

# Remove problematic chars and replace them with representative string
def scrubFile(inFile, outDir):
    outFile = outDir + (inFile.rpartition('\\')[2]) 
    print(inFile)
    if 'VAERSVAX.csv' in inFile:
        dataFrame = pd.read_csv(inFile, engine='python', error_bad_lines=False,
            usecols=[*range(0,10)]) #drop records with errors, trim empty cols
    else:
        dataFrame = pd.read_csv(inFile, engine='python', error_bad_lines=False) #drop records with errors
    dataFrame.set_index('VAERS_ID', inplace=True) #drop the auto-numbered column

    # Replacement
    dataFrame = dataFrame.astype(str)
    dataFrame = dataFrame.replace('@', 'at', regex=True)
    dataFrame = dataFrame.replace('#', 'hashtag', regex=True)
    dataFrame = dataFrame.replace('\'', 'quote', regex=True)
    dataFrame = dataFrame.replace('\"', 'quote', regex=True) 
    dataFrame = dataFrame.replace('&', 'and', regex=True) 
    dataFrame = dataFrame.replace('-', 'minus', regex=True)
    dataFrame = dataFrame.replace(';', 'semicolon', regex=True)
    dataFrame = dataFrame.replace(':', 'colon', regex=True)
    dataFrame = dataFrame.replace('~', ' ', regex=True)

    # Create the clean copy of the file
    dataFrame.to_csv(outFile)

# Combine the cleaned files by year - Creates one file for each year of VAERS data
def combineFiles(inFiles, outDir):
    for fileGroup in inFiles:
        #Combine files for each year, join on VAERS_ID
        dataFrame = pd.DataFrame()
        firstRun = True
        prefix = None

        for file in fileGroup:
            print('combining ' + file)
            df = pd.read_csv(file, engine='python', error_bad_lines=False) #drop records with errors
            if firstRun == True:
                dataFrame = df              
                firstRun = False
            else:                
                dataFrame = pd.merge(dataFrame, df, how="inner", on="VAERS_ID")
            prefix = str(file)            
            prefix = prefix.split('V')
            prefix = prefix[1].split('/', 2)
            #os.remove(file)

        #Combine all files with same columns - do this from data frame above        
        dataFrame.set_index('VAERS_ID', inplace=True)
        dataFrame.to_csv(outDir + prefix[2] + 'VAERS.csv')

# Combine all the clean files - creates a file with the total VAERS data 
def appendFiles(inFiles, outDir):
    totalDataFrame = pd.DataFrame()
    firstFile = True

    for file in inFiles:
        print('appending ' + file)
        df = pd.read_csv(file, engine='python', error_bad_lines=False) #drop records with errors

        if(firstFile):
            totalDataFrame = df
            firstFile = False
        else:            
            totalDataFrame.append(df)

    totalDataFrame.set_index("VAERS_ID", inplace=True)
    totalDataFrame.to_csv(outDir + 'TotalVAERSData.csv')
'''
old way to do it just keeping vax type - much faster
#Combine multiple vaccination names from the same VAERS_ID to create a single record
def combineVaxRecords(file):
    print('processing ' + file)
    df = pd.read_csv(file, engine='python', error_bad_lines=False) #drop records with errors        
    df['VAX_TYPE'] = df.groupby(['VAERS_ID'])['VAX_TYPE'].transform(lambda x: ' '.join(x))

    df.drop_duplicates(subset=['VAERS_ID'], inplace=True) 
    df = df[['VAERS_ID','VAX_TYPE']]
    df.set_index("VAERS_ID", inplace=True)
    df.to_csv(file)
'''
#Combine multiple vaccination names from the same VAERS_ID to create a single record
def combineVaxRecords(file):
    print('processing ' + file)
    headers = ['VAERS_ID', 'VAX_TYPE_1', 'VAX_MANU_1', 'VAX_LOT_1', 'VAX_DOSE_SERIES_1','VAX_ROUTE_1', 'VAX_SITE_1', 'VAX_NAME_1',
               'VAX_TYPE_2', 'VAX_MANU_2', 'VAX_LOT_2', 'VAX_DOSE_SERIES_2','VAX_ROUTE_2', 'VAX_SITE_2', 'VAX_NAME_2',
               'VAX_TYPE_3', 'VAX_MANU_3', 'VAX_LOT_3', 'VAX_DOSE_SERIES_3','VAX_ROUTE_3', 'VAX_SITE_3', 'VAX_NAME_3',
               'VAX_TYPE_4', 'VAX_MANU_4', 'VAX_LOT_4', 'VAX_DOSE_SERIES_4','VAX_ROUTE_4', 'VAX_SITE_4', 'VAX_NAME_4',
               'VAX_TYPE_5', 'VAX_MANU_5', 'VAX_LOT_5', 'VAX_DOSE_SERIES_5','VAX_ROUTE_5', 'VAX_SITE_5', 'VAX_NAME_5',
               'VAX_TYPE_6', 'VAX_MANU_6', 'VAX_LOT_6', 'VAX_DOSE_SERIES_6','VAX_ROUTE_6', 'VAX_SITE_6', 'VAX_NAME_6']

    dfOut = pd.DataFrame(columns=headers)
    df = pd.read_csv(file, engine='python', error_bad_lines=False) #drop records with errors  
          
    # get a unique list of the IDs
    idList = list(df['VAERS_ID'])
    idList = list(dict.fromkeys(idList))

    inRows = pd.DataFrame()    
    # for each record, write the row if it's the only one found for that ID. Otherwise combine the rows
    for record in idList:
        outRow = pd.DataFrame(columns=headers)
        inRows = df.loc[df['VAERS_ID'] == record] 
        inRows.set_index("VAERS_ID", inplace=True)
        count = 1
        for index, row in inRows.iterrows(): 
            if count == 1:
                outRow.at[record,'VAERS_ID'] = record
                outRow.at[record,'VAX_TYPE_1'] = inRows.iat[0,0]
                outRow.at[record,'VAX_MANU_1'] = inRows.iat[0,1]
                outRow.at[record,'VAX_LOT_1'] = inRows.iat[0,2]
                outRow.at[record,'VAX_DOSE_SERIES_1'] = inRows.iat[0,3]
                outRow.at[record,'VAX_ROUTE_1'] = inRows.iat[0,4]
                outRow.at[record,'VAX_SITE_1'] = inRows.iat[0,5]
                outRow.at[record,'VAX_NAME_1'] = inRows.iat[0,6]
                dfOut = dfOut.append(outRow)
            else:
                if count > 6:
                    print('error - more than 6 vaccines for this id ' + str(record))
                else:
                    # map the current record to the combined record
                    strCount = str(count)
                    vaxType = 'VAX_TYPE_' + strCount
                    vaxMenu = 'VAX_MANU_' + strCount
                    vaxLot = 'VAX_LOT_' + strCount
                    vaxSeries = 'VAX_DOSE_SERIES_' + strCount
                    vaxRoute = 'VAX_ROUTE_' + strCount
                    vaxSite = 'VAX_SITE_' + strCount 
                    vaxName = 'VAX_NAME_' + strCount

                    countIndex = count - 1
                    location = 0

                    # combine the data for record to be writen to the new file
                    dfOut.at[record,vaxType] = inRows.iat[countIndex,location]
                    dfOut.at[record,vaxMenu] = inRows.iat[countIndex,location+1]
                    dfOut.at[record,vaxLot] = inRows.iat[countIndex,location+2]
                    dfOut.at[record,vaxSeries] = inRows.iat[countIndex,location+3]
                    dfOut.at[record,vaxRoute] = inRows.iat[countIndex,location+4]
                    dfOut.at[record,vaxSite] = inRows.iat[countIndex,location+5]
                    dfOut.at[record,vaxName] = inRows.iat[countIndex,location+6]
            
            count += 1                
            
    #change to new dataframe   
    dfOut.set_index("VAERS_ID", inplace=True)
    dfOut.to_csv(file)

# Need to combine symptoms from multiple entries to a single entry. Supports 25 symptoms
def combineSymptoms(file):
    additionalHeaders = ['SYMPTOM6', 'SYMPTOMVERSION6',
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
    df = pd.read_csv(file, engine='python', error_bad_lines=False) #drop records with errors
    # get a unique list of the IDs
    idList = list(df['VAERS_ID'])
    idList = list(dict.fromkeys(idList))
    # add the new headers to support 25 symptoms per record
    newColumns = df.columns.values  
    newColumns = np.append(newColumns,additionalHeaders)      
    dfOut = pd.DataFrame(columns=newColumns)
    outRow = pd.DataFrame(columns=newColumns)
    inRows = pd.DataFrame()
    # for each record, write the row if it's the only one found for that ID. Otherwise combine the rows
    for record in idList:
        inRows = df.loc[df['VAERS_ID'] == record]         
        inRows.set_index("VAERS_ID", inplace=True)
        count = 0  
        outRow = pd.DataFrame(columns=newColumns) 
        outRow.set_index("VAERS_ID", inplace=True)     
        for index, row in inRows.iterrows(): 
            if count == 0:
                outRow.at[record,'VAERS_ID'] = record
                outRow.at[record,'SYMPTOM1'] = inRows.iat[0,0]
                outRow.at[record,'SYMPTOMVERSION1'] = inRows.iat[0,1]
                outRow.at[record,'SYMPTOM2'] = inRows.iat[0,2]
                outRow.at[record,'SYMPTOMVERSION2'] = inRows.iat[0,3]
                outRow.at[record,'SYMPTOM3'] = inRows.iat[0,4]
                outRow.at[record,'SYMPTOMVERSION3'] = inRows.iat[0,5]
                outRow.at[record,'SYMPTOM4'] = inRows.iat[0,6]                
                outRow.at[record,'SYMPTOMVERSION4'] = inRows.iat[0,7]
                outRow.at[record,'SYMPTOM5'] = inRows.iat[0,8]                
                outRow.at[record,'SYMPTOMVERSION5'] = inRows.iat[0,9]
            else:
                if count == 1:
                    headerRange = range(6,10)
                    startIndex = 6
                elif count == 2:
                    headerRange = range(11,15)
                    startIndex = 11
                elif count == 3:
                    headerRange = range(16,20)
                    startIndex = 16
                elif count == 4:
                    headerRange = range(21,25)
                    startIndex = 21
                elif count == 5:
                    headerRange = range(26,30)
                    startIndex = 26
                elif count == 6:
                    headerRange = range(31,35)
                    startIndex = 31
                else:
                    print('error - more than 35 symptoms for this id ' + str(record))

                # map the five symptoms from the current record to the combined record
                for i in headerRange:
                    symptomHeader = 'SYMPTOM' + str(i)
                    versionHeader = 'SYMPTOMVERSION' + str(i)

                    # get the indices to map it to
                    if i == startIndex:
                        symptomLocation = 0
                        versionLocation = 1

                    # combine the data for record to be writen to the new file
                    newSymptom = inRows.iat[count,symptomLocation]
                    newVersion = inRows.iat[count,versionLocation]                 
                    outRow.at[record,symptomHeader] = newSymptom
                    outRow.at[record,versionHeader] = newVersion
                    
                    symptomLocation += 2
                    versionLocation += 2

            count += 1 

        dfOut.append(outRow) 

    #change to new dataframe   
    dfOut.set_index("VAERS_ID", inplace=True)
    dfOut.to_csv(file) #TODO file is empty, so are total files

# Take a file path and list, add the file to the list if it exists, return the list
def addFileIfExists(thePath, fileList):
    if os.path.exists(thePath):
        fileList.append(thePath)
    else:
        print("File not found while creating list " + thePath)
    
    return fileList

def main():    
    origDirName = 'E:/Desktop/Vaccine2/Data'
    cleanDirName = 'E:/Desktop/Vaccine2/CleanData/'
    outputDirName = 'E:/Desktop/Vaccine2/TotalCleanData/'
    beginYear = 1991
    stopYear = 1991
    nonDomestic = False

    print("Starting at " + datetime.now().strftime('%H:%M:%S'))
    '''  
    # Get the list of all original files
    listOfFiles = getListOfFiles(origDirName, None, None, None, nonDomestic)
    
    # Get the list of all files in directory tree at given path
    listOfFiles = list()
    for (dirpath, dirnames, filenames) in os.walk(origDirName):
        listOfFiles += [os.path.join(dirpath, file) for file in filenames]
       
    # Create clean copies of the files 
    pool = Pool()
    scrubFile1=partial(scrubFile, outDir=cleanDirName)
    pool.map(scrubFile1, listOfFiles)         
    pool.close()
          
    # Combine the records of the Vax file to remove duplicates 
    # (drops Vax info other than VAX_TYPE and VAERS_ID)  
    print("Starting vax files at " + datetime.now().strftime('%H:%M:%S'))
    vaxFiles = getListOfFiles(cleanDirName, beginYear, stopYear, 'VAERSVAX.csv', nonDomestic) 
    pool2 = Pool()
    pool2.map(combineVaxRecords, vaxFiles, chunksize=1)  
    pool2.close()
    '''
    # Combine the symptom records so they are all on one line
    print("Starting symptom files at " + datetime.now().strftime('%H:%M:%S'))
    symptomFiles = getListOfFiles(cleanDirName, beginYear, stopYear, 'VAERSSYMPTOMS.csv', nonDomestic)
    pool1 = Pool()
    pool1.map(combineSymptoms, symptomFiles, chunksize=1)         
    pool1.close()
    
    # Combine the three yearly files into one
    print("Combining files at " + datetime.now().strftime('%H:%M:%S'))
    listOfFiles = getListOfFiles(cleanDirName, beginYear, stopYear, None, nonDomestic)
    combineFiles(listOfFiles, outputDirName)
    
    # Append all the files to create one total VAERS file
    print("Appending files at " + datetime.now().strftime('%H:%M:%S'))
    listOfFiles = getListOfFiles(outputDirName, None, None, None, nonDomestic)
    appendFiles(listOfFiles, outputDirName)

    print("Finished at " + datetime.now().strftime('%H:%M:%S'))
      
if __name__ == '__main__':
    main()