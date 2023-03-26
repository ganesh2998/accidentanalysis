#!/usr/bin/env python
# coding: utf-8

# In[ ]:


"""

This script reads the input and output details from config file in the directory and then calls the function
which execute spark dataframe commands to generate required results for the questions mentioned in the case study. 

"""


# In[7]:


####  Importing libraries, functions required 


from pyspark.sql.functions import *

from pyspark.sql import *

import os
import sys


# In[8]:


from zipfile import ZipFile
with ZipFile("./lib.zip",'r') as zObject:
  
    # Extracting all the members of the zip 
    # into a specific location.
    zObject.extractall(
        path="./")
    
with ZipFile("./Data.zip",'r') as zObject:
  
    # Extracting all the members of the zip 
    # into a specific location.
    zObject.extractall(
        path="./")    


# In[9]:


from lib import lib_functions as f


# In[ ]:





# In[10]:


class AccidentAnalysis:
    def __init__(self, path_config_file):
        
        #print(path_config_file)
        
        input_file_paths = f.config_reader(path_config_file).get("Input_File_Paths")
        
        self.df_charges = spark.read.option("header",True) \
     .csv(r"{0}".format(input_file_paths.get("Charges"))).dropDuplicates()
        
        self.df_damages = spark.read.option("header",True) \
     .csv(r"{0}".format(input_file_paths.get("Damages"))).dropDuplicates()
        
        self.df_endorse = spark.read.option("header",True) \
     .csv(r"{0}".format(input_file_paths.get("Endorse"))).dropDuplicates()
        
        self.df_primary_person = spark.read.option("header",True) \
     .csv(r"{0}".format(input_file_paths.get("Primary_Person"))).dropDuplicates()
        
        self.df_units = spark.read.option("header",True) \
     .csv(r"{0}".format(input_file_paths.get("Units"))).dropDuplicates()
        
        self.df_restrict = spark.read.option("header",True) \
     .csv(r"{0}".format(input_file_paths.get("Restrict"))).dropDuplicates()
        
        
        
    def analysis_1(self,op,f_format):
        
        """
        Function to solve Question 1 
        Parameters : {
        op : output file path 
        f_format : output file format }
    
        """
        df_q1 = self.df_primary_person
        
        df_1_op = df_q1.filter((col("PRSN_INJRY_SEV_ID")=="KILLED") & (col("PRSN_GNDR_ID")=="MALE")).dropDuplicates(["CRASH_ID"]).select("CRASH_ID")
        
        res = df_1_op.count()
        
        f.writer(df_1_op,op,f_format)
        
        print( """
        Writing CRASH_IDs where deaths involved in crash are male
        """)
        
        return res
    
    def analysis_2(self,op,f_format):
        """
        Function to solve Question 2 
        Parameters : {
        op : output file path 
        f_format : output file format }
    
        """        
        df_q2 = self.df_units
        
        df_2_op = df_q2.filter(col("VEH_BODY_STYL_ID").contains("MOTORCYCLE")).select("CRASH_ID")
        
        res = df_2_op.count()
        print( """
        Writing CRASH_IDs which involved two wheelers to output file 
        """)
        f.writer(df_2_op,op,f_format)
        
        return res
    
    
    def analysis_3(self,op,f_format):
        
        """
        Function to solve Question 3
        Parameters : {
        op : output file path 
        f_format : output file format }
    
        """ 
        
        
        df_q3 = self.df_primary_person
        
        win_3 = Window.partitionBy().orderBy(col("no_of_females").desc())
        
        df_3_op=df_q3.filter((col("PRSN_GNDR_ID")=="FEMALE") & (~col("DRVR_LIC_STATE_ID").isin(["NA","Unknown"]))).groupBy("DRVR_LIC_STATE_ID").agg(count("DRVR_LIC_STATE_ID").alias("no_of_females")).withColumn("rn",row_number().over(win_3)).filter(col("rn")==1).select("DRVR_LIC_STATE_ID")
        
        f.writer(df_3_op,op,f_format)
        
        print(""" 
        
        Writing state (DRVR_LIC_STATE_ID)  to output file 
        
        """)
        
        res = [state[0] for state in df_3_op.collect()]

        return res
    
    def analysis_4(self,op,f_format):
        
        """
        Function to solve Question 4
        Parameters : {
        op : output file path 
        f_format : output file format }
    
        """
        
        df_q4 = self.df_units
        
        df_4=df_q4.filter(col("VEH_MAKE_ID")!="NA").withColumn("TOTAL_INJRY_DEATH_CNT",col("TOT_INJRY_CNT")+col("DEATH_CNT")).groupBy("VEH_MAKE_ID").agg(sum("TOTAL_INJRY_DEATH_CNT").alias("TOTAL_INJRY_DEATH_CNT_AGG")).select("VEH_MAKE_ID",col("TOTAL_INJRY_DEATH_CNT_AGG").cast("int"))

        win_4  = Window.partitionBy().orderBy(col("TOTAL_INJRY_DEATH_CNT_AGG").desc())
        
        df_4_op = df_4.withColumn("row_number",row_number().over(win_4)).filter((col("row_number")>=5) &  (col("row_number")<=15)).select("VEH_MAKE_ID")
        
        f.writer(df_4_op,op,f_format)
        
        print("""
        Writing Top 5th to 15th VEH_MAKE_IDs  to output file 
        """)
        
        res = [make[0] for make in df_4_op.collect()]
        
        return res
    
    def analysis_5(self,op,f_format):
        
        """
        Function to solve Question 5
        Parameters : {
        op : output file path 
        f_format : output file format }
    
        """
        
        df_units_5 = self.df_units
        df_primary_person_5 = self.df_primary_person
        
        df_5 = df_units_5.join(df_primary_person_5,["CRASH_ID"],"inner").filter(col("VEH_BODY_STYL_ID")!="NA").select("VEH_BODY_STYL_ID","PRSN_ETHNICITY_ID").groupBy("VEH_BODY_STYL_ID","PRSN_ETHNICITY_ID").agg(count("PRSN_ETHNICITY_ID").alias("ethnic_count"))

        win_5  = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(col("ethnic_count").desc())

        df_5_op = df_5.withColumn("rn",row_number().over(win_5)).filter(col("rn")==1).select("VEH_BODY_STYL_ID","PRSN_ETHNICITY_ID")
        
        ### write df_5_rn to output file
        f.writer(df_5_op,op,f_format)
        
        print("""
        writing the top ethnic user group of each unique body style  to the output file 
        """)
        
        
        
        res = [[make[0],make[1]] for make in df_5_op.collect()]      
        
        return res
    
    def analysis_6(self,op,f_format):
        
        """
        Function to solve Question 6
        Parameters : {
        op : output file path 
        f_format : output file format }
    
        """
        
        df_units_6 = self.df_units
        df_primary_person_6 = self.df_primary_person
        
        df_6 = df_units_6.join(df_primary_person_6,["CRASH_ID"],"inner").filter((col("CONTRIB_FACTR_1_ID").contains("ALCOHOL")) | (col("CONTRIB_FACTR_2_ID").contains("ALCOHOL")) | (col("CONTRIB_FACTR_P1_ID").contains("ALCOHOL"))).select("DRVR_ZIP").groupBy(col("DRVR_ZIP")).agg(count("DRVR_ZIP").alias("crsh_cnt"))

        win_6 = Window.partitionBy().orderBy(col("crsh_cnt").desc())


        df_6_op = df_6.withColumn("rn", row_number().over(win_6)).filter(col("rn")<6).select("DRVR_ZIP")
        
        
        ### write df_6_op to output file 
        
        f.writer(df_6_op,op,f_format)
        
        print( """writing the Top 5 Zip Codes  to output file           
        """) 
        
        res = [zip[0] for zip in df_6_op.collect()]
        
        return res
    
    
    def analysis_7(self,op,f_format):
        
        """
        Function to solve Question 7
        Parameters : {
        op : output file path 
        f_format : output file format }
    
        """
        
        df_units_7 = self.df_units
        
        df_damages_7 = self.df_damages
        
        """
        damage_range is a list of damage ranges to be considered 
        damage_list is a list of all the values where there are no damages 
        insured is list of all values which convey that Insurance is available 
        
        """
        damage_range = ["DAMAGED 5", "DAMAGED 6", "DAMAGED 7 HIGHEST"]
        damage_list = ["NONE", "NONE1","NO DAMAGES TO THE CITY POLE 214385"]
        insured = ["PROOF OF LIABILITY INSURANCE", "LIABILITY INSURANCE POLICY"]
        df_7_op = df_units_7.join(df_damages_7,["CRASH_ID"],"inner").filter((col("VEH_DMAG_SCL_1_ID").isin(damage_range)) & (col("VEH_DMAG_SCL_2_ID").isin(damage_range)) & (col("FIN_RESP_TYPE_ID").isin(insured)) & (col("DAMAGED_PROPERTY").isin(damage_list))).select("CRASH_ID").dropDuplicates()
        
        f.writer(df_7_op,op,f_format)
        
        print("""
        
        writing Distinct Crash IDs  to output file 
        """)
        res = [crash_id[0] for crash_id in df_7_op.collect()]
        
        return res
    
    def analysis_8(self,op,f_format):
        
        """
        Function to solve Question 8
        Parameters : {
        op : output file path 
        f_format : output file format }
    
        """
        
        df_units_8 = self.df_units
        
        df_charges_8 = self.df_charges
        
        df_primary_person_8 = self.df_primary_person
        
        #### deriving list of top 25 states with mmost no of offenses 
        
        df_25_st = df_units_8.join(df_charges_8,["CRASH_ID"],"inner").filter((col("CHARGE").contains("OFFENSE")) & (col("VEH_LIC_STATE_ID").cast("double").isNull())).select("VEH_LIC_STATE_ID").groupBy("VEH_LIC_STATE_ID").agg(count("VEH_LIC_STATE_ID").alias("off_count")).orderBy(col("off_count").desc())

        window_8 = Window.partitionBy().orderBy(col("off_count").desc())
        top_25_state=[state[0] for state in df_25_st.withColumn("rn",row_number().over(window_8)).filter(col("rn")<26).select("VEH_LIC_STATE_ID").collect()]

        
        """ top_25_state is the list of states with most no of offenses  """ 
        
        ##### deriving top 10 vehicle colors 
        
        
        df_10_color = df_units_8.filter(col("VEH_COLOR_ID")!= "NA").select("VEH_COLOR_ID").groupBy("VEH_COLOR_ID").agg(count("VEH_COLOR_ID").alias("color_cnt")).orderBy(col("color_cnt").desc())
        window_8_color = Window.partitionBy().orderBy(col("color_cnt").desc())

        top_10_colors = [ color[0] for color in df_10_color.withColumn("rn",row_number().over(window_8_color)).filter(col("rn")<11).select("VEH_COLOR_ID").collect()]

        """ top_10_colors is the list of top 10 vehicle colors """ 
        

        drvr_lic_typ = ["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."]
        
        #### drvr_lic_typ is the list of values which indicate the driver has license 
        
        df_8 = df_units_8.join(df_charges_8,["CRASH_ID"],"inner").join(df_primary_person_8,["CRASH_ID"],"inner").filter((col("CHARGE").contains("SPEED") & (col("VEH_COLOR_ID").isin(top_10_colors)) & (col("VEH_LIC_STATE_ID").isin(top_25_state)) & (col("DRVR_LIC_TYPE_ID").isin(drvr_lic_typ)) & (col("VEH_MAKE_ID") != "NA") )).select("VEH_MAKE_ID").groupBy("VEH_MAKE_ID").agg(count("VEH_MAKE_ID").alias("make_cnt")).orderBy(col("make_cnt").desc())

        win_8 = Window.partitionBy().orderBy(col("make_cnt").desc())

        df_8_op = df_8.withColumn("rn",row_number().over(win_8)).filter(col("rn")<6).select("VEH_MAKE_ID")  
        
        f.writer(df_8_op,op,f_format)
        
        print("""
        
        Top 5 Vehicle Makes details to output file
        
        """)
        
        res = [brand[0] for brand in df_8_op.collect()]
        
        return res 
    
    
        
        
        


# In[11]:


if __name__ == '__main__':
    # creating the spark session
    spark = SparkSession \
        .builder \
        .appName("AccidentAnalysis") \
        .getOrCreate()

    path_config_file = "external.config"
    spark.sparkContext.setLogLevel("ERROR")

    obj_AccidentAnalysis = AccidentAnalysis(path_config_file)
    
    output_file_paths = f.config_reader(path_config_file).get("Output_File_Paths")
    Output_File_Format = f.config_reader(path_config_file).get("Output_File_Format")

    # 1. Analysis 1: Find the number of crashes (accidents) in which number of persons killed are male?
    #print("1. Result:", obj_AccidentAnalysis.count_male_accidents(output_file_paths.get(1), file_format.get("Output")))
    print("Analysis 1: Find the number of crashes (accidents) in which number of persons killed are male?")

    op = output_file_paths.get('output_path_1')
    
    result1 = obj_AccidentAnalysis.analysis_1(op,Output_File_Format)
    
    print("Result: ", result1)
    print("\n\n\n")
    
    # 2. Analysis 2: How many two wheelers are booked for crashes? 
    
    print("Analysis 2: How many two wheelers are booked for crashes?  ")
    
    op = output_file_paths.get('output_path_2')
    
    result2 = obj_AccidentAnalysis.analysis_2(op,Output_File_Format)
    
    print("Result: ", result2)
    print("\n\n\n")
    
    
    # 3. Analysis 3: Which state has highest number of accidents in which females are involved? 
    
    print("Analysis 3: Which state has highest number of accidents in which females are involved? ")
    
    op = output_file_paths.get('output_path_3')
    
    result3 = obj_AccidentAnalysis.analysis_3(op,Output_File_Format)
    
    print("Result: ", result3)
    print("\n\n\n")
    
    # 4. Analysis 4: Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death
    
    print("Analysis 4: Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death ?")
    
    op = output_file_paths.get('output_path_4')
    
    result4 = obj_AccidentAnalysis.analysis_4(op,Output_File_Format)
    
    print("Result: ", result4)
    print("\n\n\n")
    
    # 5. Analysis 5: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style
    
    print("""Analysis 5: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style
             
             Note: crashes where uniquie body stlye is Not available are excluded from the output 
             
             """)
    
    op = output_file_paths.get('output_path_5')
    
    result5 = obj_AccidentAnalysis.analysis_5(op,Output_File_Format)
    
    print("Result: ", result5)   
    print("\n\n\n")
    
    
    # 6. Analysis 6: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
    
    print("""Analysis 6: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code) """)
    
    op = output_file_paths.get('output_path_6')
    
    result6 = obj_AccidentAnalysis.analysis_6(op,Output_File_Format)
    
    
    print("Result: ", result6) 
    print("\n\n\n")
    
    
    # 7. Analysis 7: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
    
    print(""" Analysis 7: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance """)
    
    op = output_file_paths.get('output_path_7')
    
    result7 = obj_AccidentAnalysis.analysis_7(op,Output_File_Format)
    
    print("Result: ", result7)   
    print("\n\n\n")
    
    
    # 8. Analysis 8: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)
    
    print(""" Analysis 8: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, 
    used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences 
    (to be deduced from the data) """)
    
    op = output_file_paths.get('output_path_8')
    
    result8 = obj_AccidentAnalysis.analysis_8(op,Output_File_Format)
    
    print("Result: ", result8)   
    
    print("\n\n\n")
    
    


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




