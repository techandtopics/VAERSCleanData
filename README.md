# VAERSCleanData
Python code to clean VAERS data for import to WEKA or similar data analysis platform.

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

    Link to VAERS data: https://vaers.hhs.gov/data.html