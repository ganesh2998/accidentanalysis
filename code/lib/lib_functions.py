#!/usr/bin/env python
# coding: utf-8

# In[ ]:


def config_reader(filepath):
    filename = filepath
    contents = open(filename).read()
    config = eval(contents)
    
    return config


def writer(df,path,f_format):
    
    df.write.format(f_format).mode('overwrite').option("header", "true").save(path)

