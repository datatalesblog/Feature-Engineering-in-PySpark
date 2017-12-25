
# coding: utf-8

# In[26]:

from pyspark.sql.functions import *
from pyspark import SparkContext, SQLContext

#initializing sparkcontext object
sc = SparkContext()
sql = SQLContext(sc)


# In[2]:

#input data
data_all = sql.read.format("com.databricks.spark.csv").option("header", "true").load("C:\\Users\\Amit Bhalerao\\Documents\\UIC Course work Fall17\\Big Data\\Project\\SINCE 1961.csv")


# In[12]:

#show top 5 rows
data_all.show(5)


# In[3]:

#calculate missing values% for each column
data_all.select(*((100*sum(col(c).isNull().cast("int"))/data_all.count()).alias(c) for c in data_all.columns)).show()


# In[3]:

#drop the column with high missing value%
data_clean = data_all.drop('costat','saley_dc','sppey_dc','atq_dc','chq_dc','cogsq_dc','cshiq_dc','dlttq_dc','ltq_dc','niq_dc','revtq_dc','saleq_dc','chechy_dc', 'cogsy_dc','oancfy_dc','recchy_dc','revty_dc','chechy','cogsy','recchy','revty','saley','sppey','chechy','cogsy','recchy','revty','saley','sppey').filter(data_all.cshtrq>0).withColumn('Return on Assets', data_all.niq/data_all.atq).withColumn('Current Ratio', data_all.atq/data_all.ltq).withColumn('Gross Margin', data_all.revtq/data_all.cogsq).withColumn('Return of Equity', data_all.niq/data_all.teqq)


# In[4]:

#input data to calculate bm ratio
data_bmratio = sql.read.format("com.databricks.spark.csv").option("header", "true").load("C:\\Users\\Amit Bhalerao\\Documents\\UIC Course work Fall17\\Big Data\\Project\\PB Ratio Data.csv")


# In[7]:

#check missing value% in each column
data_bmratio.select(*((100*sum(col(c).isNull().cast("int"))/data_bmratio.count()).alias(c) for c in data_bmratio.columns)).show()


# In[5]:

#list of final columns in dataset
data_bmratio.columns


# In[6]:

#calculate bm ratio in a new column
data_bm16 = data_bmratio.na.drop().withColumn('BMRatio', data_bmratio['Book Value per share']/data_bmratio['Market value per share']).filter(data_bmratio['Year']==2016)


# In[7]:

#count of rows in the dataset
data_bm16.count()


# In[7]:

#filter the rows with top quartile BM ratio values
bmq = data_bm16.approxQuantile('BMRatio',[0.75],0)
data_bm_q = data_bm16.filter(data_bm16['BMRatio'] > bmq[0])


# In[8]:

#using first dataset to calculate F-scores
data_Fscore = data_clean.drop('datadate','fqtr','indfmt','consol','popsrc','datafmt','curcdq','datacqtr','datafqtr','chq','cshiq','saleq','teqq','prccq','Return on Assets','Current Ratio','Gross Margin','Return of Equity')
data_F1516 = data_Fscore.filter((col("fyearq")=='2016') | (col("fyearq") =='2015'))
data_F1516.columns


# In[9]:

#pivoting the dataset
from pyspark.sql import functions as F
data_Fpivot = data_F1516.groupBy('tic').pivot('fyearq', [2015, 2016]).agg(F.sum(data_F1516.atq),F.sum(data_F1516.cogsq),F.sum(data_F1516.dlttq),F.sum(data_F1516.ltq),F.sum(data_F1516.niq),F.sum(data_F1516.revtq),F.mean(data_F1516.oancfy),F.sum(data_F1516.cshtrq))


# In[10]:

data_Fpivot.columns


# In[12]:

#calculating scores and adding it to the dataset
from pyspark.sql.functions import expr, when
score1 = when(col("2016_sum(CAST(niq AS DOUBLE))")/col("2016_sum(CAST(atq AS DOUBLE))")>0, 1).otherwise(0)
score2 = when(col("2016_avg(CAST(oancfy AS DOUBLE))")/col("2016_sum(CAST(atq AS DOUBLE))")>0, 1).otherwise(0)
score3 = when(col("2016_sum(CAST(niq AS DOUBLE))")/col("2016_sum(CAST(atq AS DOUBLE))") > col("2015_sum(CAST(niq AS DOUBLE))")/col("2015_sum(CAST(atq AS DOUBLE))"), 1).otherwise(0)
score4 = when(col("2016_avg(CAST(oancfy AS DOUBLE))")/col("2016_sum(CAST(atq AS DOUBLE))") > col("2016_sum(CAST(niq AS DOUBLE))")/col("2016_sum(CAST(atq AS DOUBLE))"), 1).otherwise(0)              
score5 = when(col("2016_sum(CAST(dlttq AS DOUBLE))")/col("2016_sum(CAST(atq AS DOUBLE))") > col("2015_sum(CAST(dlttq AS DOUBLE))")/col("2015_sum(CAST(atq AS DOUBLE))"), 0).otherwise(1)
score6 = when(col("2016_sum(CAST(atq AS DOUBLE))")/col("2016_sum(CAST(ltq AS DOUBLE))") > col("2015_sum(CAST(atq AS DOUBLE))")/col("2015_sum(CAST(ltq AS DOUBLE))"), 1).otherwise(0)
score7 = when(col("2016_sum(CAST(cshtrq AS DOUBLE))") > col("2015_sum(CAST(cshtrq AS DOUBLE))"), 0).otherwise(1)
score8 = when((col("2016_sum(CAST(revtq AS DOUBLE))")-col("2016_sum(CAST(cogsq AS DOUBLE))"))/col("2016_sum(CAST(revtq AS DOUBLE))") > (col("2015_sum(CAST(revtq AS DOUBLE))")-col("2015_sum(CAST(cogsq AS DOUBLE))"))/col("2015_sum(CAST(revtq AS DOUBLE))"), 1).otherwise(0)
score9 = when(col("2016_sum(CAST(revtq AS DOUBLE))")/col("2016_sum(CAST(atq AS DOUBLE))") > col("2015_sum(CAST(revtq AS DOUBLE))")/col("2015_sum(CAST(atq AS DOUBLE))"), 1).otherwise(0)
Fscore = score1+score2+score3+score4+score5+score6+score7+score8+score9
data_Fpivot = data_Fpivot.withColumn("Fscore",Fscore)


# In[13]:

#joining the two datasets with F-score and BM ratio
data_bm_F = data_bm_q.join(data_Fpivot, data_bm_q['Ticker'] == data_Fpivot['tic'], 'inner')


# In[13]:

#list of columns in new dataset
data_bm_F.columns


# In[14]:

#dropping the unwanted columns
data_bm_F = data_bm_F.drop('GVKEY',
 'tic','2015_sum(CAST(atq AS DOUBLE))',
 '2015_sum(CAST(cogsq AS DOUBLE))',
 '2015_sum(CAST(dlttq AS DOUBLE))',
 '2015_sum(CAST(ltq AS DOUBLE))',
 '2015_sum(CAST(niq AS DOUBLE))',
 '2015_sum(CAST(revtq AS DOUBLE))',
 '2015_avg(CAST(oancfy AS DOUBLE))',
 '2015_sum(CAST(cshtrq AS DOUBLE))',
 '2016_sum(CAST(atq AS DOUBLE))',
 '2016_sum(CAST(cogsq AS DOUBLE))',
 '2016_sum(CAST(dlttq AS DOUBLE))',
 '2016_sum(CAST(ltq AS DOUBLE))',
 '2016_sum(CAST(niq AS DOUBLE))',
 '2016_sum(CAST(revtq AS DOUBLE))',
 '2016_avg(CAST(oancfy AS DOUBLE))',
 '2016_sum(CAST(cshtrq AS DOUBLE))')


# In[16]:

#filtered the row with top half F-scores
bmd = data_bm_F.approxQuantile('Fscore',[0.50],0)
Top_decile = when(col("Fscore")>=bmd[0], 1).otherwise(0)
data_bm_F = data_bm_F.withColumn("Top Decile",Top_decile)


# In[55]:

#input the third dataset with daily prices
data_dailyprice = sql.read.format("com.databricks.spark.csv").option("header", "true").load("C:\\Users\\Amit Bhalerao\\Documents\\UIC Course work Fall17\\Big Data\\Project\\daily price_cleaned.csv")


# In[57]:

#looking at the head of the dataset
data_dailyprice.head()


# In[58]:

data_base = data_dailyprice.withColumn("Ticker Name",data_dailyprice.TICKER)


# In[59]:

#function to calculate annual prices from daily prices
import numpy as np
def calc_annual_returns(daily_returns):
    grouped = np.exp(daily_returns.groupby(lambda date: date.year).sum())-1
    #grouped = np.exp(daily_returns.groupby(daily_returns.year).sum())-1
    return grouped


# In[60]:

#joining the top F-score dataset and daily prices dataset
data_bm_F2 = data_bm_F.drop('Date','Book Value per share','Market value per share','BMRatio','Year')
data_base = data_base.join(data_bm_F2, data_base['Ticker Name'] == data_bm_F2['Ticker'], 'inner')
data_base = data_base.filter(col('Top Decile')==1)
data_base = data_base.drop('Ticker','PERMNO','Company name','Top Decile','Fscore')
data_base.columns
data_base.dtypes


# In[61]:

#pivoting the joined dataset
import numpy as np
import pandas as pd

data_basepivot = data_base.groupBy('date').pivot('Ticker Name').agg(F.mean(data_base.PRC.cast('float')))
#data_base.write.csv('C:\\Users\\Amit Bhalerao\\Documents\\UIC Course work Fall17\\Big Data\\Project\\data_base.csv')
#data_basepivot.toPandas().to_csv('C:\\Users\\Amit Bhalerao\\Documents\\UIC Course work Fall17\\Big Data\\Project\\data_base.csv')


# In[46]:

data_basepivot = data_basepivot.select(*(col(c).cast("float").alias(c) for c in data_basepivot.columns))
data_basepivot.dtypes


# In[62]:

data_basepivot2 = data_basepivot.toPandas()
data_basepivot2 = data_basepivot2.fillna(data_basepivot2.mean())

data_basepivot2 = data_basepivot2.sort_values(['date'], ascending=True)
data_basepivot2['date'] = pd.to_datetime(data_basepivot2['date'])
data_basepivot2 = data_basepivot2.set_index('date')
data_basepivot2


# In[63]:

#function to calculate the log ratio of annual prices
from datetime import datetime, timedelta

def calc_daily_returns(closes):
    return np.log(closes/closes.shift(1))


# In[66]:

#calculate daily returns and output it to csv file
daily_returns = calc_daily_returns(data_basepivot2)
daily_returns.to_csv('C:\\Users\\Amit Bhalerao\\Documents\\UIC Course work Fall17\\Big Data\\Project\\daily_returns.csv')
daily_returns


# In[69]:

#output the annual prices to csv file
annual_returns = calc_annual_returns(daily_returns)
annual_returns.to_csv('C:\\Users\\Amit Bhalerao\\Documents\\UIC Course work Fall17\\Big Data\\Project\\annual_returns.csv')
annual_returns

