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

    For best performance run CPU 4+ core 2.5GHz+, 8GB+ RAM, 3GB Disk
    Should run OK on slower hardware, but time will greatly increase.
    Run time is approximately 5 hours on the hardware mentioned above.
    
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

'''
Get the List of all files in the directory tree 
Optional: start and end year to get files prefixed with those years
Optional: file name suffix to get one of the three file types (eg. VAERSVAX.csv)
'''
def getListOfFiles(dirName, startYear, endYear, fileNameSuffix):
    listOfFile = os.listdir(dirName)
    allFiles = list()

    # Full file list portion
    if startYear == None and endYear == None and fileNameSuffix == None:
        for entry in listOfFile:
            fullPath = os.path.join(dirName, entry)

            # If entry is a directory then get the list of files in this directory 
            if os.path.isdir(fullPath):
                allFiles = allFiles + getListOfFiles(fullPath, None, None, None)
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

        filesToAppend = []
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
        dataFrame.set_index("VAERS_ID", inplace=True)
        dataFrame.to_csv(outDir + prefix[2] + 'VAERS.csv')

# Combine all the clean files - creates a file with the total VAERS data 
def appendFiles(inFiles, outDir):
    totalDataFrame = pd.DataFrame()

    for file in inFiles:
        print('appending ' + file)
        df = pd.read_csv(file, engine='python', error_bad_lines=False) #drop records with errors
        totalDataFrame.append(df)

    totalDataFrame.to_csv(outDir + 'TotalVAERSData.csv')

#Combine multiple vaccination names from the same VAERS_ID to create a single record
def combineVaxRecords(file):
    print('processing ' + file)
    df = pd.read_csv(file, engine='python', error_bad_lines=False) #drop records with errors        
    df['VAX_TYPE'] = df.groupby(['VAERS_ID'])['VAX_TYPE'].transform(lambda x: ' '.join(x))

    df.drop_duplicates(subset=['VAERS_ID'], inplace=True) 
    df = df[['VAERS_ID','VAX_TYPE']]
    df.set_index("VAERS_ID", inplace=True)
    df.to_csv(file)

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
    dfOut = pd.DataFrame()
    # get a unique list of the IDs
    idList = list(df['VAERS_ID'])
    idList = list(dict.fromkeys(idList))
    # add the new headers to support 25 symptoms per record
    newColumns = df.columns.values  
    newColumns = np.append(newColumns,additionalHeaders)      
    dfOut = pd.concat([dfOut,pd.DataFrame(columns=newColumns)])
    inRows = pd.DataFrame()
    # for each record, write the row if it's the only one found for that ID. Otherwise combine the rows
    for record in idList:
        inRows = df.loc[df['VAERS_ID'] == record] 
        count = 0
        for index, row in inRows.iterrows(): 
            if count == 0:
                outRow = row
            else:
                if count == 1:
                    headerRange = range(6,10)
                elif count == 2:
                    headerRange = range(11,15)
                elif count == 3:
                    headerRange = range(16,20)
                elif count == 4:
                    headerRange = range(21,25)
                elif count == 5:
                    headerRange = range(26,30)
                elif count == 6:
                    headerRange = range(31,35)
                else:
                    print('error - more than 35 symptoms for this id ' + str(record))

                # map the five symptoms from the current record to the combined record
                for i in headerRange:
                    symptomHeader = 'SYMPTOM' + str(i)
                    versionHeader = 'SYMPTOMVERSION' + str(i)
                    # get the indices to map it to
                    versionLocation = headerRange.index(i)
                    if versionLocation == 0:
                        versionLocation = 2
                    else:
                        versionLocation = versionLocation *2
                    symptomLocation = versionLocation - 1
                    # combine the data for record to be writen to the new file
                    outRow[symptomHeader] = inRows.iat[count,symptomLocation]
                    outRow[versionHeader] = inRows.iat[count,versionLocation]

        #write the outRow to new df here
        dfOut = dfOut.append(outRow) 
        count += 1                
            
    #change to new dataframe   
    dfOut.set_index("VAERS_ID", inplace=True)
    dfOut.to_csv(file)

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
    beginYear = 1990
    stopYear = 2020
    
    # Get the list of all original files
    listOfFiles = getListOfFiles(origDirName, None, None, None)
    
    # Get the list of all files in directory tree at given path
    listOfFiles = list()
    for (dirpath, dirnames, filenames) in os.walk(origDirName):
        listOfFiles += [os.path.join(dirpath, file) for file in filenames]
       
    # Create clean copies of the files  
    processes = list() 
    count = 0
    for elem in listOfFiles:
        processes.append(Process(target=scrubFile, args=(elem, cleanDirName)))
        processes[count].start()        
        count += 1
    
    for p in processes:
        p.join()

    # Combine the records of the Vax file to remove duplicates 
    # (drops Vax info other than VAX_TYPE and VAERS_ID)
    vaxProcesses = list()
    vaxFiles = list()
    vaxCount = 0
    vaxFiles = getListOfFiles(cleanDirName, beginYear, stopYear, 'VAERSVAX.csv') 

    for elem in vaxFiles:
        vaxProcesses.append(Process(target=combineVaxRecords, args=(elem,)))    
        vaxProcesses[vaxCount].start()
        vaxCount += 1
    
    for p in vaxProcesses:
        p.join()

    # Combine the symptom records so they are all on one line
    symptomFiles = list()
    symptomProcesses = list()
    symptomCount = 0
    symptomFiles = getListOfFiles(cleanDirName, beginYear, stopYear, 'VAERSSYMPTOMS.csv')
    for elem in symptomFiles:
        symptomProcesses.append(Process(target=combineSymptoms, args=(elem,)))
        symptomProcesses[symptomCount].start()
        symptomCount += 1

    for p in symptomProcesses:
        p.join()

    # Combine the three yearly files into one
    listOfFiles = getListOfFiles(cleanDirName, beginYear, stopYear, None)
    combineFiles(listOfFiles, outputDirName)

    # Append all the files to create one total VAERS file
    listOfFiles = getListOfFiles(cleanDirName, None, None, None)
    appendFiles(listOfFiles, outputDirName)
       
if __name__ == '__main__':
    main()