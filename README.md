# accidentanalysis
This repository is for analysing the US accident data as part of a case study. 


We have 8 questions that need to answered by utilizing the data present in Data.zip file. 

The solution to all the questions was arrived using Sparks Dataframe API. 

Case Study:

Application should perform below analysis and store the results for each analysis.

1.	Analytics 1: Find the number of crashes (accidents) in which number of persons killed are male?
2.	Analysis 2: How many two wheelers are booked for crashes? 
3.	Analysis 3: Which state has highest number of accidents in which females are involved? 
4.	Analysis 4: Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death
5.	Analysis 5: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style  
6.	Analysis 6: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
7.	Analysis 7: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
8.	Analysis 8: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)


The path of Input files, Output files and the format of the output file are configurable using the "code/external.config" file. 

Data.zip needs to be unzipped into the "code" reporsitory. 

Execution: 

After cloning the repo, 
This folder should contain the below files:
  1. main.py
  2. lib.zip
  3. external.config
  4. Data.zip
Launch Command prompt from inside the "code" folder. 

Execture the below command:



spark-submit --master "local[*]" --py-files Data.zip, lib.zip --files external.config main.py
