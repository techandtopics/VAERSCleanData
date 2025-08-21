# VAERSCleanData
Python code to clean VAERS data for import to WEKA or similar data analysis platform.

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

    Requirements file generated with python -m pipreqs.pipreqs ./ --encoding=utf-8