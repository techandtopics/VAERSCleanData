'''
===================  DownloadVAERSData  ===================
    This program is meant to download and unzip VAERS data. 
    
    Please verify your results! 
    
    These numbers will grow with each year.
    Estimated file download size: 170MB
    Estimated unzipped size: 750MB
    
    This code is open source and licensed under the GNU GPL v3 at:
    https://www.gnu.org/licenses/gpl-3.0.en.html.

    Copyright (c) 2020 Things and Stuff LTD
'''
import requests
import zipfile
import io
from datetime import datetime

def main():
    urlPattern = 'https://vaers.hhs.gov/eSubDownload/index.jsp?fn=*VAERSData.zip'

    startYear = 1990
    endYear = datetime.now().year
    path = 'E:/Desktop/Vaccine2/Data3/'

    # Build the list of files and URLs to download from
    urlList = []
    fileList = []
    currYear = startYear
    while currYear < endYear:
        url = urlPattern.replace('*', str(currYear))
        urlList.append(url)        
        currYear += 1

    url = 'https://vaers.hhs.gov/eSubDownload/index.jsp?fn=NonDomesticVAERSData.zip'
    urlList.append(url)
    
    # Download the files
    for url in urlList:
        print(f'Getting file from {url}')
        fileName = url.rpartition('=')[2]
        fileName = fileName.rpartition('.')[0]
        fileList.append(fileName)
        try:
            r = requests.get(url, stream=True)
            z = zipfile.ZipFile(io.BytesIO(r.content))
            z.extractall(path + fileName)
            r.raise_for_status()
        except requests.HTTPError as http_err:
            print(f'HTTP error occurred: {http_err}')
        except Exception as err:
            print(f'Other error occurred: {err}')
    

    # Unzip the files and clean data
    for file in fileList:
        completeFilePath = path + file + '.csv'
        theFile = open(completeFilePath, "r")
        outFile = open(completeFilePath.rpartition(".")[0] + "2.csv", "a")
        lines = theFile.readlines()
        for line in lines:
            commaCount = 0
            for c in line:
                if c == ',':
                    commaCount = commaCount + 1
                if commaCount > 10:
                    line = line.rpartition(',')[0]
                outFile.writelines(line)
        
        outFile.close()
        theFile.close()

if __name__ == '__main__':
    main()