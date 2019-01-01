#######################################################################################################################################################
#1.Problem Statement/Business Understanding
#2.Broad Assumptions
#3.Initialise Environment,load libraries & data
#4.Understanding
#5.Assignment Tasks - Examining Data
#6.Assignment Tasks - Aggregation tasks
#7.Closure
#######################################################################################################################################################

#######################################################################################################################################################
#1.Problem Statement/Business Understanding
#######################################################################################################################################################
#NYC Police Department has collected data for parking tickets.We have been provided with data files for 2015,2016 and 2017
#The purpose of this case study is to conduct an exploratory data analysis that helps to understand the data. 
#The scope of this analysis, we wish to compare the phenomenon related to parking tickets over three different years - 2015, 2016, 2017
#It's reccomended to do analysis over fiscal year however it's fine to use calendar year approach as well.
#
#######################################################################################################################################################
#2.Broad Assumptions
#-We will be using calendar year instead of fiscal year- as permitted in the problem statement.
#-We'll load all files together for analysis and perform required analysis.This will cause all data pertaining to calendar years 2015,2016, 2017
#-to be considered valid for case study.Some of this data would have been invalid in other approach/es.
#-Since the purpose of this case study is EDA itself, we have performed EDA only as needed and cleanup is performed only in case it affects the analysis.
#-We are assuming that all required libraries are installed in the environment prior to execution
#-It was observed that 2017 file contained 8 less columns however names of the columns were same in all 3 years hence it was decided 
# to use combined data load instead of individual data frames per year to avoid repetative code.
#######################################################################################################################################################

#######################################################################################################################################################
#3.Initialise Environment, load libraries & data
#######################################################################################################################################################
#File Location
#'/common_folder/nyc_parking/Parking_Violations_Issued_-_Fiscal_Year_201x.csv'

# Load SparkR
spark_path <- '/usr/local/spark'
if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
  Sys.setenv(SPARK_HOME = spark_path)
}
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))

# Initialise the sparkR session
sparkR.session(master = "yarn-client", sparkConfig = list(spark.driver.memory = "1g"))

#Ensure that all libraries below are installed before loading
#Sample install command
#install.packages("sparklyr")

library(sparklyr)
library(dplyr)
library(ggplot2)
library(ggrepel)
library(tidyr)

#connect to spark session
#sc <- spark_connect(master = "yarn-client")

# Before executing any hive-sql query from RStudio, you need to add a jar file in RStudio 
sql("ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-hcatalog-core-1.1.0-cdh5.11.2.jar")

#filePath <- "hdfs:///common_folder/nyc_parking/Parking_Violations_Issued_-_Fiscal_Year_201*.csv"

#Load all data files at once - combined data
NYC_Ticket_Base<-SparkR::read.df("hdfs:///common_folder/nyc_parking/Parking_Violations_Issued_-_Fiscal_Year_201*.csv", "CSV", header="true", inferSchema = "true")

#Load individual year files
NYC_Ticket_Base_2015<-SparkR::read.df("hdfs:///common_folder/nyc_parking/Parking_Violations_Issued_-_Fiscal_Year_2015.csv", "CSV", header="true", inferSchema = "true")
NYC_Ticket_Base_2016<-SparkR::read.df("hdfs:///common_folder/nyc_parking/Parking_Violations_Issued_-_Fiscal_Year_2016.csv", "CSV", header="true", inferSchema = "true")
NYC_Ticket_Base_2017<-SparkR::read.df("hdfs:///common_folder/nyc_parking/Parking_Violations_Issued_-_Fiscal_Year_2017.csv", "CSV", header="true", inferSchema = "true")


#######################################################################################################################################################
#4.Understanding/Examining Data
#######################################################################################################################################################
ncol(NYC_Ticket_Base_2015) #51 columns
nrow(NYC_Ticket_Base_2015) #11809233 records/rows

ncol(NYC_Ticket_Base_2016) #51 columns
nrow(NYC_Ticket_Base_2016) #10626899 records/rows

ncol(NYC_Ticket_Base_2017) #43 columns
nrow(NYC_Ticket_Base_2017) #10803028 records/rows

ncol(NYC_Ticket_Base)#51 columns
nrow(NYC_Ticket_Base)#33239160 records/rows

colnames(NYC_Ticket_Base_2015)
#[1] "Summons Number"                    "Plate ID"                          "Registration State"               
#[4] "Plate Type"                        "Issue Date"                        "Violation Code"                   
#[7] "Vehicle Body Type"                 "Vehicle Make"                      "Issuing Agency"                   
#[10] "Street Code1"                      "Street Code2"                      "Street Code3"                     
#[13] "Vehicle Expiration Date"           "Violation Location"                "Violation Precinct"               
#[16] "Issuer Precinct"                   "Issuer Code"                       "Issuer Command"                   
#[19] "Issuer Squad"                      "Violation Time"                    "Time First Observed"              
#[22] "Violation County"                  "Violation In Front Of Or Opposite" "House Number"                     
#[25] "Street Name"                       "Intersecting Street"               "Date First Observed"              
#[28] "Law Section"                       "Sub Division"                      "Violation Legal Code"             
#[31] "Days Parking In Effect    "        "From Hours In Effect"              "To Hours In Effect"               
#[34] "Vehicle Color"                     "Unregistered Vehicle?"             "Vehicle Year"                     
#[37] "Meter Number"                      "Feet From Curb"                    "Violation Post Code"              
#[40] "Violation Description"             "No Standing or Stopping Violation" "Hydrant Violation"                
#[43] "Double Parking Violation"          "Latitude"                          "Longitude"                        
#[46] "Community Board"                   "Community Council "                "Census Tract"                     
#[49] "BIN"                               "BBL"                               "NTA"                 


str(NYC_Ticket_Base_2015)
# 'SparkDataFrame': 51 variables:                                                 
# $ Summons Number                   : num 8002531292 8015318440 7611181981 7445908067 7037692864 7704791394
# $ Plate ID                         : chr "EPC5238" "5298MD" "FYW2775" "GWE1987" "T671196C" "JJF6834"
# $ Registration State               : chr "NY" "NY" "NY" "NY" "NY" "PA"
# $ Plate Type                       : chr "PAS" "COM" "PAS" "PAS" "PAS" "PAS"
# $ Issue Date                       : chr "10/01/2014" "03/06/2015" "07/28/2014" "04/13/2015" "05/19/2015" "11/20/2014"
# $ Violation Code                   : int 21 14 46 19 19 21
# $ Vehicle Body Type                : chr "SUBN" "VAN" "SUBN" "4DSD" "4DSD" "4DSD"
# $ Vehicle Make                     : chr "CHEVR" "FRUEH" "SUBAR" "LEXUS" "CHRYS" "NISSA"
# $ Issuing Agency                   : chr "T" "T" "T" "T" "T" "T"
# $ Street Code1                     : int 20390 27790 8130 59990 36090 74230
# $ Street Code2                     : int 29890 19550 5430 16540 10410 37980
# $ Street Code3                     : int 31490 19570 5580 16790 24690 38030
# $ Vehicle Expiration Date          : chr "01/01/20150111 12:00:00 PM" "01/01/88888888 12:00:00 PM" "01/01/20160524 12:0
# $ Violation Location               : int 7 25 72 102 28 67
# $ Violation Precinct               : int 7 25 72 102 28 67
# $ Issuer Precinct                  : int 7 25 72 102 28 67
# $ Issuer Code                      : int 345454 333386 331845 355669 341248 357104
# $ Issuer Command                   : chr "T800" "T103" "T302" "T402" "T103" "T302"
# $ Issuer Squad                     : chr "A2" "B" "L" "D" "X" "A"
# $ Violation Time                   : chr "0011A" "0942A" "1020A" "0318P" "0410P" "0839A"
# $ Time First Observed              : chr "NA" "NA" "NA" "NA" "NA" "NA"
# $ Violation County                 : chr "NY" "NY" "K" "Q" "NY" "K"
# $ Violation In Front Of Or Opposite: chr "F" "F" "F" "F" "F" "F"
# $ House Number                     : chr "133" "1916" "184" "120-20" "66" "1013"
# $ Street Name                      : chr "Essex St" "Park Ave" "31st St" "Queens Blvd" "W 116th St" "Rutland Rd"
# $ Intersecting Street              : chr "NA" "NA" "NA" "NA" "NA" "NA"
# $ Date First Observed              : chr "01/05/0001 12:00:00 PM" "01/05/0001 12:00:00 PM" "01/05/0001 12:00:00 PM" "01
# $ Law Section                      : int 408 408 408 408 408 408
# $ Sub Division                     : chr "d1" "c" "f1" "c3" "c3" "d1"
# $ Violation Legal Code             : chr "NA" "NA" "NA" "NA" "NA" "NA"
# $ Days Parking In Effect           : chr "Y Y Y" "YYYYY" "NA" "YYYYY" "YYYYYYY" "Y"
# $ From Hours In Effect             : chr "1200A" "0700A" "NA" "0300P" "NA" "0830A"
# $ To Hours In Effect               : chr "0300A" "1000A" "NA" "1000P" "NA" "0900A"
# $ Vehicle Color                    : chr "BL" "BROWN" "BLACK" "GY" "BLACK" "WHITE"
# $ Unregistered Vehicle?            : int NA NA NA NA NA NA
# $ Vehicle Year                     : int 2005 0 2010 2015 0 0
# $ Meter Number                     : chr "NA" "NA" "NA" "NA" "NA" "NA"
# $ Feet From Curb                   : int 0 0 0 0 0 0
# $ Violation Post Code              : chr "A 77" "CC3" "J 32" "01 4" "19 7" "C 32"
# $ Violation Description            : chr "21-No Parking (street clean)" "14-No Standing" "46A-Double Parking (Non-COM)"
# $ No Standing or Stopping Violation: chr "NA" "NA" "NA" "NA" "NA" "NA"
# $ Hydrant Violation                : chr "NA" "NA" "NA" "NA" "NA" "NA"
# $ Double Parking Violation         : chr "NA" "NA" "NA" "NA" "NA" "NA"
# $ Latitude                         : chr "NA" "NA" "NA" "NA" "NA" "NA"
# $ Longitude                        : chr "NA" "NA" "NA" "NA" "NA" "NA"
# $ Community Board                  : chr "NA" "NA" "NA" "NA" "NA" "NA"
# $ Community Council                : chr "NA" "NA" "NA" "NA" "NA" "NA"
# $ Census Tract                     : chr "NA" "NA" "NA" "NA" "NA" "NA"
# $ BIN                              : chr "NA" "NA" "NA" "NA" "NA" "NA"
# $ BBL                              : chr "NA" "NA" "NA" "NA" "NA" "NA"
# $ NTA                              : chr "NA" "NA" "NA" "NA" "NA" "NA"

setdiff(colnames(NYC_Ticket_Base_2015),colnames(NYC_Ticket_Base_2016))
#Both data sets have same columns
setdiff(colnames(NYC_Ticket_Base_2015),colnames(NYC_Ticket_Base_2017))
#NYC_Ticket_Base_2017 does not have below columns
# [1] "Latitude"           "Longitude"          "Community Board"    "Community Council " "Census Tract"       "BIN"               
# [7] "BBL"                "NTA"

#This shows that all data files contain same columns- 2017 has 8 less columns however those are not requrired for analysis
#Hence we will use the combined data set that contains data for all years for further analysis.

# Finding number of null values in each column
nullcount <- SparkR::select(NYC_Ticket_Base, lapply(columns(NYC_Ticket_Base), function(c) 
  alias(sum(cast(isNull(NYC_Ticket_Base[[c]]), "integer")), c))) 

nullcount %>% as.data.frame %>% gather(NYC_Ticket_Base, sum_null)
# NYC_Ticket_Base sum_null
# 1                     Summons Number        0
# 2                           Plate ID        4
# 3                 Registration State        0
# 4                         Plate Type        0
# 5                         Issue Date        0
# 6                     Violation Code        0
# 7                  Vehicle Body Type   127700
# 8                       Vehicle Make   212135
# 9                     Issuing Agency        0
# 10                      Street Code1        0
# 11                      Street Code2        0
# 12                      Street Code3        0
# 13           Vehicle Expiration Date        1
# 14                Violation Location  5740226
# 15                Violation Precinct        1
# 16                   Issuer Precinct        1
# 17                       Issuer Code        1
# 18                    Issuer Command  5704460
# 19                      Issuer Squad  5706033
# 20                    Violation Time     6058
# 21               Time First Observed 30042504
# 22                  Violation County  3594319
# 23 Violation In Front Of Or Opposite  5985339
# 24                      House Number  6312797
# 25                       Street Name    18338
# 26               Intersecting Street 23509505
# 27               Date First Observed        2
# 28                       Law Section        2
# 29                      Sub Division     5008
# 30              Violation Legal Code 27532215
# 31        Days Parking In Effect      8418387
# 32              From Hours In Effect 15613695
# 33                To Hours In Effect 15613692
# 34                     Vehicle Color   415377
# 35             Unregistered Vehicle? 29595048
# 36                      Vehicle Year        4
# 37                      Meter Number 27290553
# 38                    Feet From Curb        4
# 39               Violation Post Code  9349625
# 40             Violation Description  3647162
# 41 No Standing or Stopping Violation 33239159
# 42                 Hydrant Violation 33239159
# 43          Double Parking Violation 33239159
# 44                          Latitude 33239160
# 45                         Longitude 33239160
# 46                   Community Board 33239160
# 47                Community Council  33239160
# 48                      Census Tract 33239160
# 49                               BIN 33239160
# 50                               BBL 33239160
# 51                               NTA 33239160
# A lot of columns towards end of data frame are completely empty
# Based on questions, we will not need these column for analysis hence we can remove these.
# There are some columns with high number of nulls.These are not needed for analysis hence we decided to leave those as it is.
# `violation location` and `Violation Time` are needed for analysis and have nulls to be checked as part of analysis
# Hence we'll treat those nulls later during the analysis as needed for this case study

NYC_Ticket_Base<-SparkR::dropna(NYC_Ticket_Base,how="any", cols=c("Vehicle Body Type","Vehicle Make","Violation Precinct","Issuer Precinct"))
NYC_Ticket_Base$`No Standing or Stopping Violation`<-NULL
NYC_Ticket_Base$`Hydrant Violation`<-NULL
NYC_Ticket_Base$`Double Parking Violation`<-NULL
NYC_Ticket_Base$`Latitude`<-NULL
NYC_Ticket_Base$`Longitude`<-NULL
NYC_Ticket_Base$`Community Board`<-NULL
NYC_Ticket_Base$`Community Council `<-NULL
NYC_Ticket_Base$`Census Tract`<-NULL
NYC_Ticket_Base$`BIN`<-NULL
NYC_Ticket_Base$`BBL`<-NULL
NYC_Ticket_Base$`NTA`<-NULL

#unique values
unique_NYC_Ticket<-SparkR:::lapply(names(NYC_Ticket_Base),function(x) alias(countDistinct(NYC_Ticket_Base[[x]]), x))
head(do.call(agg, c(x = NYC_Ticket_Base, unique_NYC_Ticket)))
# 
# Summons Number Plate ID Registration State Plate Type Issue Date Violation Code Vehicle Body Type
# 1       31864311  6021216                 69         89       3379            100              3824
# Vehicle Make Issuing Agency Street Code1 Street Code2 Street Code3 Vehicle Expiration Date
# 1        12679             20         7024         7319         7072                    9555
# Violation Location Violation Precinct Issuer Precinct Issuer Code Issuer Command Issuer Squad
# 1                579                580             847       60900           5977           50
# Violation Time Time First Observed Violation County Violation In Front Of Or Opposite House Number
# 1           2169                2547               19                                 6        78608
# Street Name Intersecting Street Date First Observed Law Section Sub Division Violation Legal Code
# 1      189521              407674                1517           9          144                    5
# Days Parking In Effect     From Hours In Effect To Hours In Effect Vehicle Color Unregistered Vehicle?
#   1                        190                  784                905          4418                     4
# Vehicle Year Meter Number Feet From Curb Violation Post Code Violation Description
# 1          100        54500             17                1234                   110
#It is observed that there are nearly 1.1M(32932223 vs 31864311 unique) records with same Summons Numbers.
#Summon Number is expected to be unique  hence based on https://learn.upgrad.com/v/course/126/question/99083
#We will remove these records

createOrReplaceTempView(NYC_Ticket_Base, "NYC_Ticket_Base_tab")
NYC_Ticket_Base<-SparkR::sql("select * from NYC_Ticket_Base_tab where `Summons Number` in ( select `Summons Number` from NYC_Ticket_Base_tab group by `Summons Number`   having count(*) = 1 )")
ncol(NYC_Ticket_Base)#40
nrow(NYC_Ticket_Base)#30842504

NYC_Ticket_Base$`Issue Date`<-SparkR::to_date(NYC_Ticket_Base$`Issue Date`, "MM/dd/yyyy")
NYC_Ticket_Base$Issue_Date_Year<-SparkR::year(NYC_Ticket_Base$`Issue Date`)
#Creating a table for quering
createOrReplaceTempView(NYC_Ticket_Base, "NYC_Ticket_Base_tab")

#######################################################################################################################################################
#5.Assignment Tasks - Examining Data
#######################################################################################################################################################

#------------------------------------------------------------------------------------------------------------------------------------------------------
#A1. Find the total number of tickets for each year.

#We'll check if the data is clean first before getting answers
Year_Wise_Record <- SparkR::sql("SELECT Issue_Date_Year, count(*) REC_COUNT FROM NYC_Ticket_Base_tab group by Issue_Date_Year")
head(Year_Wise_Record)
# Issue_Date_Year REC_COUNT                                                     
# 1            1990         3
# 2            2025        40
# 3            1975         1
# 4            1977         1
# 5            2027        48
# 6            2003         5
# looks like the year spred is more than the expected 3 years
#For the scope of this analysis, we wish to compare the phenomenon related to parking tickets over three different years - 2015, 2016, 2017
nrow(Year_Wise_Record)
#71 seems lie a lot of years data is present, lets filter out unwanted years
NYC_Ticket_Base <- SparkR::sql("SELECT *  FROM NYC_Ticket_Base_tab where Issue_Date_Year in (2015, 2016, 2017)")
createOrReplaceTempView(NYC_Ticket_Base, "NYC_Ticket_Base_tab")

#let's check yearwise number of tickets in the data
Year_Wise_Record <- SparkR::sql("SELECT Issue_Date_Year, count(*) REC_COUNT FROM NYC_Ticket_Base_tab group by Issue_Date_Year")
head(Year_Wise_Record)
# Issue_Date_Year REC_COUNT                                                     
# 1            2015  10008087
# 2            2016  10146977
# 3            2017   5377542


#The ticket counts have decreased over the years, specially 2017 seems to have very low numbers

#Alternate -Let's check this on a plot
c <- SparkR::count(groupBy(NYC_Ticket_Base, "Issue_Date_Year"))
c.r <- SparkR::collect(c)
year_count <- c.r[c.r$Issue_Date_Year %in% c(2015,2016,2017), 1:2]

# plot showing yearly number of tickets
g <- ggplot(year_count, aes(x=year_count$Issue_Date_Year, y=year_count$count))
g + geom_bar(stat = "identity") + geom_text(label = year_count$count, position = position_stack(vjust = 0.5))

#------------------------------------------------------------------------------------------------------------------------------------------------------
#A2. Find out the number of unique states from where the cars that got parking tickets came from.
#(Hint: Use the column 'Registration State')
# There is a numeric entry in the column which should be corrected. 
#Replace it with the state having maximum entries. Give the number of unique states for each year again.

#First check  the data for cleanup needs- as mentioned, there is numeric entry in the data that needs to be fixed.
State_Wise<-SparkR::sql("select `Registration State`,Count(*) Record_count from NYC_Ticket_Base_tab group by `Registration State` order by Count(*) desc")

nrow(State_Wise)
#Number of states in overall data - 69.This contains 99 which is invalid.

head(State_Wise,69)
#Maximum entries    NY     20024087
#Numeric entries    99        73154

#Fix the numeric state records with NY
NYC_Ticket_Base <- SparkR::sql("SELECT NYC_Ticket_Base_tab.*, case when `Registration State`=99 then 'NY' else `Registration State` end Registration_State   FROM NYC_Ticket_Base_tab ")
createOrReplaceTempView(NYC_Ticket_Base, "NYC_Ticket_Base_tab")
Year_State<- SparkR::sql("select Issue_Date_Year, count(distinct Registration_State) REC_COUNT FROM NYC_Ticket_Base_tab group by Issue_Date_Year")

head(Year_State,nrow(Year_State))
#Number of unique states in parking tickets data
# Issue_Date_Year REC_COUNT                                                     
# 1            2015        68
# 2            2016        67
# 3            2017        64


#Plot - Number of tickets by Registration State
rs <- SparkR::count(groupBy(NYC_Ticket_Base, "Registration State"))
rs <- SparkR::collect(rs)
rs <- (arrange(rs, desc(rs$count)))

# plot showing number of tickets based on Registration State
g <- ggplot(rs, aes(x=reorder(rs$`Registration State`,-rs$count), y=rs$count))
g + geom_bar(stat = "identity") + theme(axis.text.x = element_text(angle = 90, hjust = 1))
head(rs)
#   Registration State    count
# 1                 NY 20024087
# 2                 NJ  2266677
# 3                 PA   638387
# 4                 CT   339772
# 5                 FL   330976
# 6                 MA   214649

# Number of tickets by Plate Type
pt <- SparkR::count(groupBy(NYC_Ticket_Base, "Plate Type"))
pt <- SparkR::collect(pt)
pt <- (arrange(pt, desc(pt$count)))

# plot indicating number of tickets based on Plate Type
g <- ggplot(pt, aes(x=reorder(pt$`Plate Type`,-pt$count), y=pt$count))
g + geom_bar(stat = "identity") + theme(axis.text.x = element_text(angle = 90, hjust = 1))
head(pt)
#   Plate Type    count
# 1        PAS 18673869
# 2        COM  4702049
# 3        OMT   939756
# 4        OMS   230251
# 5        SRF   218486
# 6        IRP   138984
#Passenger vehicles in NY have highest violations

#------------------------------------------------------------------------------------------------------------------------------------------------------
#A3. Some parking tickets don't have the address for violation location on them, 
#which is a cause for concern. Write a query to check the number of such tickets.
#The values should not be deleted or imputed here. This is just a check.
violation_location_missing<-SparkR::sql("select count(*) REC_COUNT from NYC_Ticket_Base_tab where `violation location` is null")
head(violation_location_missing)
# REC_COUNT                                                                     
# 4448923
(4448923*100)/nrow(NYC_Ticket_Base)
#17.4% records have violation location missing from the records
violation_location_missing_yearwise<-SparkR::sql("select Issue_Date_Year,count(*) REC_COUNT from NYC_Ticket_Base_tab where `violation location` is null
                                                 group by Issue_Date_Year")
head(violation_location_missing_yearwise)
#Yearwise records - Violation location missing
#   Issue_Date_Year REC_COUNT                                                     
# 1            2015   1555016
# 2            2016   1970527
# 3            2017    923380

#######################################################################################################################################################
#6.Assignment Tasks - Aggregation tasks
#######################################################################################################################################################

#------------------------------------------------------------------------------------------------------------------------------------------------------
#A1.How often does each violation code occur? Display the frequency of the top five violation codes.
violation_code_freq<-SparkR::sql("select `violation code`, count(*) REC_COUNT from NYC_Ticket_Base_tab group by `violation code` order by count(*) desc")
head(violation_code_freq,5)
#  violation code REC_COUNT                                                      
# 1             21   3595056
# 2             36   2964148
# 3             38   2749510
# 4             14   2138952
# 5             37   1594604

#Year wise top 5
violation_code_freq_yearwise<-SparkR::sql("select Issue_Date_Year,`violation code`, count(*) REC_COUNT 
                                          from NYC_Ticket_Base_tab 
                                          group by `violation code`,Issue_Date_Year 
                                          order by count(*) desc")

violation_code_freq_2015<-SparkR::collect(SparkR::filter(violation_code_freq_yearwise, violation_code_freq_yearwise$Issue_Date_Year == 2015))
head(violation_code_freq_2015,5)
#Top 5 violation code in 2015
#   Issue_Date_Year violation code REC_COUNT
# 1            2015             21   1425779
# 2            2015             38   1143343
# 3            2015             36    951024
# 4            2015             14    851119
# 5            2015             37    668394

violation_code_freq_2016<-SparkR::collect(SparkR::filter(violation_code_freq_yearwise, violation_code_freq_yearwise$Issue_Date_Year == 2016))
head(violation_code_freq_2016,5)
#Top 5 violation code in 2016
#   Issue_Date_Year violation code REC_COUNT
# 1            2016             21   1409997
# 2            2016             36   1351297
# 3            2016             38   1066339
# 4            2016             14    815800
# 5            2016             37    633584

violation_code_freq_2017<-SparkR::collect(SparkR::filter(violation_code_freq_yearwise, violation_code_freq_yearwise$Issue_Date_Year == 2017))
head(violation_code_freq_2017,5)
#Top 5 violation code in 2017
#   Issue_Date_Year violation code REC_COUNT
# 1            2017             21    759280
# 2            2017             36    661827
# 3            2017             38    539828
# 4            2017             14    472033
# 5            2017             20    317551


#Plot the violation codes for visual analysis
violation_code_freq_yearwise<-SparkR::collect(SparkR::filter(violation_code_freq_yearwise, violation_code_freq_yearwise$REC_COUNT>50000))
plot <- ggplot(violation_code_freq_yearwise,aes(x = factor(Issue_Date_Year), y = REC_COUNT,col=`violation code`,label=`violation code`)) +
  geom_point() + 
  geom_label_repel(aes(label = `violation code`),
                   box.padding   = 0.35, 
                   point.padding = 0.5,
                   segment.color = 'grey50') +
  theme_classic()+ xlab("violation code") + ylab("REC_COUNT")
plot
#21,38,36,14 and 37 all are top 5 violation codes 

#Let's check number of tickets by violation code through another visualization
vc <- SparkR::count(groupBy(NYC_Ticket_Base, "Violation Code"))
vc <- SparkR::collect(vc)
vc <- (arrange(vc, desc(vc$count)))
# plot indicating number of tickets based on violation code
g <- ggplot(vc, aes(x=reorder(vc$`Violation Code`,-vc$count), y=vc$count))
g + geom_bar(stat = "identity") + theme(axis.text.x = element_text(angle = 90, hjust = 1))
head(vc)
#   Violation Code   count
# 1             21 3595056
# 2             36 2964148
# 3             38 2749510
# 4             14 2138952
# 5             37 1594604
# 6             20 1475446

#21,38,36,14 and 37 all are overall top 5 violation codes 
#------------------------------------------------------------------------------------------------------------------------------------------------------

#A2.How often does each 'vehicle body type' get a parking ticket? 
#How about the 'vehicle make'? (Hint: find the top 5 for both)

#Overall
#Body Type
vehicle_body_type_freq<-SparkR::sql("select `vehicle body type`, count(*) REC_COUNT from NYC_Ticket_Base_tab group by `vehicle body type` order by count(*) desc")
head(vehicle_body_type_freq,5)
#   vehicle body type REC_COUNT                                                   
# 1              SUBN   8551193
# 2              4DSD   7296843
# 3               VAN   3571103
# 4              DELV   1754513
# 5               SDN    994615

#Make
vehicle_make_freq<-SparkR::sql("select `vehicle make`, count(*) REC_COUNT from NYC_Ticket_Base_tab group by `vehicle make` order by count(*) desc")
head(vehicle_make_freq,5)
#     vehicle make REC_COUNT                                                        
# 1         FORD   3155810
# 2        TOYOT   2801819
# 3        HONDA   2486750
# 4        NISSA   2077591
# 5        CHEVR   1799535

## Year wise analysis
#Body Type
vehicle_body_type_year_freq<-SparkR::sql("select `vehicle body type`,Issue_Date_Year, count(*) REC_COUNT 
                                         from NYC_Ticket_Base_tab 
                                         group by `vehicle body type`,Issue_Date_Year order by count(*) desc")

#2015
vehicle_body_type_year_freq_2015<-SparkR::collect(SparkR::filter(vehicle_body_type_year_freq, vehicle_body_type_year_freq$Issue_Date_Year == 2015))
head(vehicle_body_type_year_freq_2015,5)
#Top 5 vehicle body type in 2015
#       vehicle body type Issue_Date_Year REC_COUNT
# 1              SUBN            2015   3245663
# 2              4DSD            2015   2862109
# 3               VAN            2015   1448572
# 4              DELV            2015    734937
# 5               SDN            2015    390372

#2016
vehicle_body_type_year_freq_2016<-SparkR::collect(SparkR::filter(vehicle_body_type_year_freq, vehicle_body_type_year_freq$Issue_Date_Year == 2016))
head(vehicle_body_type_year_freq_2016,5)
#   vehicle body type Issue_Date_Year REC_COUNT
# 1              SUBN            2016   3425471
# 2              4DSD            2016   2888670
# 3               VAN            2016   1403728
# 4              DELV            2016    667754
# 5               SDN            2016    413483

#2017
vehicle_body_type_year_freq_2017<-SparkR::collect(SparkR::filter(vehicle_body_type_year_freq, vehicle_body_type_year_freq$Issue_Date_Year == 2017))
head(vehicle_body_type_year_freq_2017,5)
#   vehicle body type Issue_Date_Year REC_COUNT
# 1              SUBN            2017   1880059
# 2              4DSD            2017   1546064
# 3               VAN            2017    718803
# 4              DELV            2017    351822
# 5               SDN            2017    190760

#Let's visualize these inferences
vehicle_body_type_year_freq<-SparkR::collect(SparkR::filter(vehicle_body_type_year_freq, vehicle_body_type_year_freq$REC_COUNT>50000))
plot <- ggplot(vehicle_body_type_year_freq,aes(x = factor(Issue_Date_Year), y = REC_COUNT,col=`vehicle body type`,label=`vehicle body type`)) +
  geom_point() + 
  geom_label_repel(aes(label = `vehicle body type`),
                   box.padding   = 0.35, 
                   point.padding = 0.5,
                   segment.color = 'grey50') +
  theme_classic()+ xlab("violation code") + ylab("REC_COUNT")
plot
#Year wise SUBN,4DSD,VAN,DELV,SDN are consistant top 5 violating body types

#Make
vehicle_make_year_freq<-SparkR::sql("select `vehicle make`,Issue_Date_Year, count(*) REC_COUNT 
                                    from NYC_Ticket_Base_tab 
                                    group by `vehicle make`,Issue_Date_Year order by count(*) desc")
#Overall Analysis
head(vehicle_make_year_freq,5)
#   vehicle make Issue_Date_Year REC_COUNT                                        
# 1         FORD            2015   1267693
# 2         FORD            2016   1252931
# 3        TOYOT            2016   1132494
# 4        TOYOT            2015   1065459
# 5        HONDA            2016    997047

#Yearwise Analysis
#2015
vehicle_make_year_freq_2015<-SparkR::collect(SparkR::filter(vehicle_make_year_freq, vehicle_make_year_freq$Issue_Date_Year == 2015))
head(vehicle_make_year_freq_2015,5)
#   vehicle make Issue_Date_Year REC_COUNT
# 1         FORD            2015   1267693
# 2        TOYOT            2015   1065459
# 3        HONDA            2015    952177
# 4        NISSA            2015    780454
# 5        CHEVR            2015    748049

#2016
vehicle_make_year_freq_2016<-SparkR::collect(SparkR::filter(vehicle_make_year_freq, vehicle_make_year_freq$Issue_Date_Year == 2016))
head(vehicle_make_year_freq_2016,5)
#   vehicle make Issue_Date_Year REC_COUNT
# 1         FORD            2016   1252931
# 2        TOYOT            2016   1132494
# 3        HONDA            2016    997047
# 4        NISSA            2016    836343
# 5        CHEVR            2016    696322

#2017
vehicle_make_year_freq_2017<-SparkR::collect(SparkR::filter(vehicle_make_year_freq, vehicle_make_year_freq$Issue_Date_Year == 2017))
head(vehicle_make_year_freq_2017,5)
#   vehicle make Issue_Date_Year REC_COUNT
# 1         FORD            2017    635186
# 2        TOYOT            2017    603866
# 3        HONDA            2017    537526
# 4        NISSA            2017    460794
# 5        CHEVR            2017    355164

#Let's visualize these inferences
vehicle_make_year_freq<-SparkR::collect(SparkR::filter(vehicle_make_year_freq, vehicle_make_year_freq$REC_COUNT>50000))
plot <- ggplot(vehicle_make_year_freq,aes(x = factor(Issue_Date_Year), y = REC_COUNT,col=`vehicle make`,label=`vehicle make`)) +
  geom_point() + 
  geom_label_repel(aes(label = `vehicle make`),
                   box.padding   = 0.35, 
                   point.padding = 0.5,
                   segment.color = 'grey50') +
  theme_classic()+ xlab("violation code") + ylab("REC_COUNT")
plot
#Year wise FORD,TOYOT(A),HONDA,NISSA,CHEVR are top 5 brands with violation tickets

#Let's perform overall analysis another way 
# Number of tickets by Vehicle Body Type   
vbt <- SparkR::count(groupBy(NYC_Ticket_Base, "Vehicle Body Type"))
vbt <- SparkR::collect(vbt)
vbt <- (arrange(vbt, desc(vbt$count)))
head(vbt)
#   Vehicle Body Type   count
# 1              SUBN 8551193
# 2              4DSD 7296843
# 3               VAN 3571103
# 4              DELV 1754513
# 5               SDN  994615
# 6              2DSD  667143

# Number of tickets by Vehicle Make     
vm <- SparkR::count(groupBy(NYC_Ticket_Base, "Vehicle Make"))
vm <- SparkR::collect(vm)
vm <- (arrange(vm, desc(vm$count)))
head(vm)
#   Vehicle Make   count
# 1         FORD 3155810
# 2        TOYOT 2801819
# 3        HONDA 2486750
# 4        NISSA 2077591
# 5        CHEVR 1799535
# 6        FRUEH 1022941
#This analysis confirms our earlier findings as well 

#------------------------------------------------------------------------------------------------------------------------------------------------------

#3.A precinct is a police station that has a certain zone of the city under its command. Find the (5 highest) frequency of tickets for each of the following:

#3.1 'Violation Precinct' (this is the precinct of the zone where the violation occurred). 
#Using this, can you make any insights for parking violations in any specific areas of the city?

#Overall Analysis
Violation_Precinct_freq<-SparkR::sql("select `Violation Precinct`, count(*) REC_COUNT from NYC_Ticket_Base_tab group by `Violation Precinct` order by count(*) desc")
head(Violation_Precinct_freq,6)
#   Violation Precinct REC_COUNT                                                  
# 1                  0   4448923
# 2                 19   1320716
# 3                 14    830512
# 4                 18    790596
# 5                  1    750644
# 6                114    712183


#Let's try alternate way
# Number of tickets by Violation Precinct                     
vp <- SparkR::count(groupBy(NYC_Ticket_Base, "Violation Precinct"))
vp <- SparkR::collect(vp)
vp <- (arrange(vp, desc(vp$count)))
head(vp)
#   Violation Precinct   count
# 1                  0 4448923
# 2                 19 1320716
# 3                 14  830512
# 4                 18  790596
# 5                  1  750644
# 6                114  712183

#Year-wise analysis
Violation_Precinct_year_freq<-SparkR::sql("select `Violation Precinct`,Issue_Date_Year, count(*) REC_COUNT 
                                          from NYC_Ticket_Base_tab  where `Violation Precinct` !=0
                                          group by `Violation Precinct`,Issue_Date_Year order by count(*) desc")

#2015
Violation_Precinct_year_freq_2015<-SparkR::collect(SparkR::filter(Violation_Precinct_year_freq, Violation_Precinct_year_freq$Issue_Date_Year == 2015))
head(Violation_Precinct_year_freq_2015,5)
#   Violation Precinct Issue_Date_Year REC_COUNT
# 1                 19            2015    526252
# 2                 18            2015    340438
# 3                 14            2015    334275
# 4                114            2015    286258
# 5                  1            2015    273800

#2016
Violation_Precinct_year_freq_2016<-SparkR::collect(SparkR::filter(Violation_Precinct_year_freq, Violation_Precinct_year_freq$Issue_Date_Year == 2016))
head(Violation_Precinct_year_freq_2016,5)
#   Violation Precinct Issue_Date_Year REC_COUNT
# Violation Precinct Issue_Date_Year REC_COUNT
# 1                 19            2016    522311
# 2                  1            2016    304737
# 3                 14            2016    295558
# 4                 18            2016    284146
# 5                114            2016    279142

#2017
Violation_Precinct_year_freq_2017<-SparkR::collect(SparkR::filter(Violation_Precinct_year_freq, Violation_Precinct_year_freq$Issue_Date_Year == 2017))
head(Violation_Precinct_year_freq_2017,5)
#   Violation Precinct Issue_Date_Year REC_COUNT
# 1                 19            2017    272153
# 2                 14            2017    200679
# 3                  1            2017    172107
# 4                 18            2017    166012
# 5                114            2017    146783

#Let's visualize these inferernces
##Reducing counts of smaller numbers for plotting
Violation_Precinct_year_freq<-SparkR::collect(SparkR::filter(Violation_Precinct_year_freq, Violation_Precinct_year_freq$REC_COUNT>100000))
plot <- ggplot(Violation_Precinct_year_freq,aes(x = factor(Issue_Date_Year), y = REC_COUNT,col=`Violation Precinct`,label=`Violation Precinct`)) +
  geom_point() + 
  geom_label_repel(aes(label = `Violation Precinct`),
                   box.padding   = 0.35, 
                   point.padding = 0.5,
                   segment.color = 'grey50') +
  theme_classic()+ xlab("Violation Precinct") + ylab("REC_COUNT")
plot

# 19,14,18,1,114 are top 5 Violation precincts where violations occur the most with 19 being consistently highest

#3.2 'Issuer Precinct' (this is the precinct that issued the ticket)
#Here you would have noticed that the dataframe has 'Violating Precinct' or 'Issuing Precinct' as '0'. These are the erroneous entries. 
#Hence, provide the record for five correct precincts. (Hint: print top six entries after sorting)

#Overall Analysis
Issuer_Precinct_freq<-SparkR::sql("select `Issuer Precinct`, count(*) REC_COUNT from NYC_Ticket_Base_tab group by `Issuer Precinct` order by count(*) desc")
head(Issuer_Precinct_freq,6)
#   Issuer Precinct REC_COUNT                                                     
# 1               0   5061166
# 2              19   1288533
# 3              14    811162
# 4              18    771257
# 5               1    731608
# 6             114    699980

#Let's try alternate way
# Number of tickets by Issuer Precinct 
ip <- SparkR::count(groupBy(NYC_Ticket_Base, "Issuer Precinct"))
ip <- SparkR::collect(ip)
ip <- (arrange(ip, desc(ip$count)))
head(ip)
#   Issuer Precinct   count
# 1               0 5061166
# 2              19 1288533
# 3              14  811162
# 4              18  771257
# 5               1  731608
# 6             114  699980

#Year-wise analysis
Issuer_Precinct_year_freq<-SparkR::sql("select `Issuer Precinct`,Issue_Date_Year, count(*) REC_COUNT 
                                       from NYC_Ticket_Base_tab  where `Issuer Precinct` !=0
                                       group by `Issuer Precinct`,Issue_Date_Year order by count(*) desc")

#2015
Issuer_Precinct_year_freq_2015<-SparkR::collect(SparkR::filter(Issuer_Precinct_year_freq, Issuer_Precinct_year_freq$Issue_Date_Year == 2015))
head(Issuer_Precinct_year_freq_2015,5)
#   Issuer Precinct Issue_Date_Year REC_COUNT
# 1              19            2015    513583
# 2              18            2015    334355
# 3              14            2015    325592
# 4             114            2015    282353
# 5               1            2015    268316

#2016
Issuer_Precinct_year_freq_2016<-SparkR::collect(SparkR::filter(Issuer_Precinct_year_freq, Issuer_Precinct_year_freq$Issue_Date_Year == 2016))
head(Issuer_Precinct_year_freq_2016,5)
#   Issuer Precinct Issue_Date_Year REC_COUNT
# Issuer Precinct Issue_Date_Year REC_COUNT
# 1              19            2016    510002
# 2               1            2016    296457
# 3              14            2016    287561
# 4              18            2016    276548
# 5             114            2016    274101

#2017
Issuer_Precinct_year_freq_2017<-SparkR::collect(SparkR::filter(Issuer_Precinct_year_freq, Issuer_Precinct_year_freq$Issue_Date_Year == 2017))
head(Issuer_Precinct_year_freq_2017,5)
#   Issuer Precinct Issue_Date_Year REC_COUNT
# 1              19            2017    264948
# 2              14            2017    198009
# 3               1            2017    166835
# 4              18            2017    160354
# 5             114            2017    143526

#Let's visualize these inferernces
##Reducing counts of smaller numbers for plotting
Issuer_Precinct_year_freq<-SparkR::collect(SparkR::filter(Issuer_Precinct_year_freq, Issuer_Precinct_year_freq$REC_COUNT>100000))
plot <- ggplot(Issuer_Precinct_year_freq,aes(x = factor(Issue_Date_Year), y = REC_COUNT,col=`Issuer Precinct`,label=`Issuer Precinct`)) +
  geom_point() + 
  geom_label_repel(aes(label = `Issuer Precinct`),
                   box.padding   = 0.35, 
                   point.padding = 0.5,
                   segment.color = 'grey50') +
  theme_classic()+ xlab("Issuer Precinct") + ylab("REC_COUNT")
plot

# 19,14,18,1,114 are top 5 issuer precincts where violations tickets are issued.The most with 19 being consistently highest
# From the counts it appears that not all tickets are issued in the same precinct where violation occured
#------------------------------------------------------------------------------------------------------------------------------------------------------

#A4. Find the violation code frequency across three precincts which have issued the most number of tickets - 
#do these precinct zones have an exceptionally high frequency of certain violation codes? 
#Are these codes common across precincts? 
#Hint: You can analyse the three precincts together using the 'union all' attribute in SQL view.
#In the SQL view,use the 'where' attribute to filter among three precincts and combine them using 'union all'.

#From data analyzed above,
#Three prestine with most issued tickets
#              19   1372464
#              14    870724
#              18    831708

#Overall analysis
Issuer_Precinct_freq<-SparkR::sql("select `Issuer Precinct`,`violation code`, count(*) REC_COUNT
                                  from NYC_Ticket_Base_tab where `Issuer Precinct` in (19,14,18)
                                  group by `Issuer Precinct`,`violation code` 
                                  order by `Issuer Precinct`,count(*) desc")

Issuer_Precinct_freq_19<-SparkR::collect(SparkR::filter(Issuer_Precinct_freq, Issuer_Precinct_freq$`Issuer Precinct` == 19))
head(Issuer_Precinct_freq_19,5)
#   Issuer Precinct violation code REC_COUNT
# 1              19             38    186785
# 2              19             37    182166
# 3              19             46    177442
# 4              19             14    145387
# 5              19             21    133729

Issuer_Precinct_freq_14<-SparkR::collect(SparkR::filter(Issuer_Precinct_freq, Issuer_Precinct_freq$`Issuer Precinct` == 14))
head(Issuer_Precinct_freq_14,5)
#   Issuer Precinct violation code REC_COUNT
# 1              14             14    166912
# 2              14             69    160205
# 3              14             31     92628
# 4              14             47     66760
# 5              14             42     56359

Issuer_Precinct_freq_18<-SparkR::collect(SparkR::filter(Issuer_Precinct_freq, Issuer_Precinct_freq$`Issuer Precinct` == 18))
head(Issuer_Precinct_freq_18,5)
#   Issuer Precinct violation code REC_COUNT
# 1              18             14    239395
# 2              18             69    107960
# 3              18             47     59707
# 4              18             31     56661
# 5              18             42     38060

#Let's visualize this inferences
Issuer_Precinct_freq<-SparkR::collect(SparkR::filter(Issuer_Precinct_freq, Issuer_Precinct_freq$REC_COUNT>1000))

plot <- ggplot(Issuer_Precinct_freq,aes(x = `violation code`, y = REC_COUNT,col=`Issuer Precinct`)) +
  geom_point() +
  xlab("violation code") + ylab("REC_COUNT")
plot
#violation code 14 is consistently high for all three issue prectine location
#38,37,46,21 are high for 19 precinct
#69,31,47,42 are high for 14 as well as 19 precinct

#Year wise analysis

Issuer_Precinct_freq_yrly<-SparkR::sql("select Issue_Date_Year,`Issuer Precinct`,`violation code`, count(*) REC_COUNT
                                       from NYC_Ticket_Base_tab where `Issuer Precinct` in (19,14,18)
                                       group by Issue_Date_Year,`Issuer Precinct`,`violation code` 
                                       order by Issue_Date_Year,count(*) desc")

#2015
Issuer_Precinct_freq_yrly_2015<-SparkR::collect(SparkR::filter(Issuer_Precinct_freq_yrly, Issuer_Precinct_freq_yrly$Issue_Date_Year == 2015))
head(Issuer_Precinct_freq_yrly_2015,15)
#     Issue_Date_Year Issuer Precinct violation code REC_COUNT
# 1             2015              18             14    104232
# 2             2015              19             38     76862
# 3             2015              19             37     71766
# 4             2015              14             69     71005
# 5             2015              14             14     67082
# 6             2015              19             14     59317
# 7             2015              19             46     57692
# 8             2015              19             21     53514
# 9             2015              19             16     52939
# 10            2015              18             69     50557
# 11            2015              14             31     35147
# 12            2015              19             20     28217
# 13            2015              18             47     26188
# 14            2015              18             31     24848
# 15            2015              14             47     24683

#2016
Issuer_Precinct_freq_yrly_2016<-SparkR::collect(SparkR::filter(Issuer_Precinct_freq_yrly, Issuer_Precinct_freq_yrly$Issue_Date_Year == 2016))
head(Issuer_Precinct_freq_yrly_2016,15)
#     Issue_Date_Year Issuer Precinct violation code REC_COUNT
# 1             2016              18             14     85542
# 2             2016              19             37     74379
# 3             2016              19             38     73734
# 4             2016              19             46     72431
# 5             2016              14             69     58999
# 6             2016              19             14     56463
# 7             2016              14             14     55300
# 8             2016              19             21     51844
# 9             2016              19             16     46045
# 10            2016              18             69     37369
# 11            2016              14             31     34980
# 12            2016              19             20     26752
# 13            2016              14             47     23770
# 14            2016              14             42     22608
# 15            2016              18             31     19959

#2017
Issuer_Precinct_freq_yrly_2017<-SparkR::collect(SparkR::filter(Issuer_Precinct_freq_yrly, Issuer_Precinct_freq_yrly$Issue_Date_Year == 2017))
head(Issuer_Precinct_freq_yrly_2017,15)
#     Issue_Date_Year Issuer Precinct violation code REC_COUNT
# 1             2017              18             14     49621
# 2             2017              19             46     47319
# 3             2017              14             14     44530
# 4             2017              19             38     36189
# 5             2017              19             37     36021
# 6             2017              14             69     30201
# 7             2017              19             14     29607
# 8             2017              19             21     28371
# 9             2017              14             31     22501
# 10            2017              18             69     20034
# 11            2017              14             47     18307
# 12            2017              19             20     14601
# 13            2017              18             47     14050
# 14            2017              18             31     11854
# 15            2017              19             40     11380

#14,37,38,46,69,21,31,47,42 are top violation codes occuring across 2015,2016 and 2017 across precincts

#------------------------------------------------------------------------------------------------------------------------------------------------------

#5. You'd want to find out the properties of parking violations across different times of the day:
#Find a way to deal with missing values, if any.
#Hint: Check for the null values using 'isNull' under the SQL.
#Also, to remove the null values, check the 'dropna' command in the API documentation.
#The Violation Time field is specified in a strange format. 
#Find a way to make this into a time attribute that you can use to divide into groups.
#Divide 24 hours into six equal discrete bins of time.
#The intervals you choose are at your discretion. For each of these groups, 
#find the three most commonly occurring violations.
#Hint: Use the CASE-WHEN in SQL view to segregate into bins. 
#For finding the most commonly occurring violations, 
#a similar approach can be used as mention in the hint for question 4.
#Now, try another direction. For the 3 most commonly occurring violation codes, 
#find the most common time of the day (in terms of the bins from the previous part)

Violation_Time_cnt<-SparkR::sql("select count(*) REC_COUNT
                                from NYC_Ticket_Base_tab where `Violation Time` is null")
head(Violation_Time_cnt)
#1314 violation time fields are null , lets remove them with dropna
NYC_Ticket_Base<-SparkR::dropna(NYC_Ticket_Base,how="all", cols="Violation Time")

createOrReplaceTempView(NYC_Ticket_Base, "NYC_Ticket_Base_tab")

Violation_Time_cnt<-SparkR::sql("select count(*) REC_COUNT
                                from NYC_Ticket_Base_tab where `Violation Time` is null")
head(Violation_Time_cnt)
#0 Violation Time records are null

head(NYC_Ticket_Base[,"Violation Time"])
#   Violation Time
# 1          1002A
# 2          0820P
# 3          0240P
# 4          0749A
# 5          0848A
# 6          1010P

# converting these values into army hours
NYC_Ticket_Base<-SparkR::sql("select NYC_Ticket_Base_tab.*,
                             substr(`Violation Time`,0,2) + case when  substr(`Violation Time`,-1)=='A' then 0 else 12 end Violation_Time_Hour
                             from NYC_Ticket_Base_tab ");
createOrReplaceTempView(NYC_Ticket_Base, "NYC_Ticket_Base_tab")
head(NYC_Ticket_Base[,"Violation_Time_Hour"])
#   Violation_Time_Hour                                                           
# 1                  10
# 2                  20
# 3                  14
# 4                   7
# 5                   8
# 6                  22
min_max_hour<-SparkR::sql("select max(Violation_Time_Hour) max_hour, min(Violation_Time_Hour) min_hour from NYC_Ticket_Base_tab")
head(min_max_hour)
#max_hour min_hour                                                             
# 99        0
#There seems to be invalid values in the data
garbage_hour<-SparkR::sql("select count(*) from NYC_Ticket_Base_tab where Violation_Time_Hour>23")
head(garbage_hour)
#2343248 records
2343248/nrow(NYC_Ticket_Base)
##9.2% records contain invalid data, lets remove them
NYC_Ticket_Base<-SparkR::sql("select * from NYC_Ticket_Base_tab where Violation_Time_Hour<=23")
createOrReplaceTempView(NYC_Ticket_Base, "NYC_Ticket_Base_tab")

#validate the data
min_max_hour<-SparkR::sql("select max(Violation_Time_Hour) max_hour, min(Violation_Time_Hour) min_hour from NYC_Ticket_Base_tab")
head(min_max_hour)
#max_hour min_hour                                                             
#  23        0
#hours ranging from 00-23 - army time
#Creating 6 buckets 4 hours each
NYC_Ticket_Base<-SparkR::sql("select NYC_Ticket_Base_tab.*,
                             case 
                             when Violation_Time_Hour between 0 and 3 then 1
                             when  Violation_Time_Hour between 4 and 7 then 2
                             when  Violation_Time_Hour between 8 and 11 then 3
                             when  Violation_Time_Hour between 12 and 15 then 4
                             when  Violation_Time_Hour between 16 and 19 then 5
                             when  Violation_Time_Hour between 20 and 23 then 6 end Violation_Time_bucket
                             from NYC_Ticket_Base_tab")
createOrReplaceTempView(NYC_Ticket_Base, "NYC_Ticket_Base_tab")

Violation_Time_bucket_Codes<-SparkR::sql("select Violation_Time_bucket,`violation code`, count(*) REC_COUNT
                                         from NYC_Ticket_Base_tab group by Violation_Time_bucket,`violation code` ")

head(Violation_Time_bucket_Codes)
#   Violation_Time_bucket violation code REC_COUNT                                
# 1                     6             19     42520
# 2                     4             54         5
# 3                     2             46     74526
# 4                     1             12        14
# 5                     5             93         8
# 6                     4             89      2222

#Time of the day for most 3 commonly occuring codes
commoncode_violation_time_buckets <- SparkR::collect(Violation_Time_bucket_Codes) 
commoncode_violation_time_buckets<- arrange(commoncode_violation_time_buckets, desc(commoncode_violation_time_buckets$REC_COUNT))
head(commoncode_violation_time_buckets,3)
#Time bucket for 3 top codes
#     Violation_Time_bucket violation code REC_COUNT
# 1                     3             21   2825191
# 2                     3             36   1483548
# 3                     3             38    928803
#It appears that the top violation codes occur in time bucket 3

#filtering for higher values for plotting
Violation_Time_bucket_Codes<-SparkR::collect(SparkR::filter(Violation_Time_bucket_Codes, Violation_Time_bucket_Codes$REC_COUNT>50000))
plot <- ggplot(Violation_Time_bucket_Codes,aes(x = Violation_Time_bucket, y = REC_COUNT,col=`violation code`,label=`violation code`)) +
  geom_point() + 
  geom_label_repel(aes(label = `violation code`),
                   box.padding   = 0.35, 
                   point.padding = 0.5,
                   segment.color = 'grey50') +
  theme_classic()+ xlab("Time Bucket") + ylab("REC_COUNT")
plot
# for 0-3 21,40,78 are the top violation code
# for 4-7  14,21,40
# for 8-11 21, 36,38
# for 12-15 38,36,37
# for 15-19 38, 37,14
# for  19-23 14, 38, 7
#36-38 , 40 , 14, 21 are mostly common across time buckets


#Year wise analysis
#Query based
Violation_Time_bucket_Year_Codes<-SparkR::sql(" select * from (select Year_time_bucket,
                                              `violation code`,REC_COUNT ,
                                              dense_rank() OVER (PARTITION BY Year_time_bucket ORDER BY REC_COUNT DESC) as rank    
                                              from (
                                              select concat(Violation_Time_bucket,'_',Issue_Date_Year) Year_time_bucket,
                                              `violation code`, count(*) REC_COUNT
                                              from NYC_Ticket_Base_tab 
                                              group by concat(Violation_Time_bucket,'_',Issue_Date_Year),
                                              `violation code`)T)V where rank<=3 ")
head(Violation_Time_bucket_Year_Codes,nrow(Violation_Time_bucket_Year_Codes))
#     Year_time_bucket violation code REC_COUNT rank                               
# 1            3_2016             21   1099915    1
# 2            3_2016             36    686587    2
# 3            3_2016             38    355058    3
# 4            5_2016             38    205800    1
# 5            5_2016             37    155279    2
# 6            5_2016             14    131121    3
# 7            4_2017             38    184105    1
# 8            4_2017             36    184050    2
# 9            4_2017             37    130466    3
# 10           4_2015             38    367090    1
# 11           4_2015             37    289704    2
# 12           4_2015             36    282136    3
# 13           6_2015              7     59612    1
# 14           6_2015             38     55596    2
# 15           6_2015             40     42432    3
# 16           1_2015             21     59280    1
# 17           1_2015             40     33684    2
# 18           1_2015             78     27319    3
# 19           2_2017             14     73567    1
# 20           2_2017             40     60397    2
# 21           2_2017             21     56737    3
# 22           3_2017             21    592259    1
# 23           3_2017             36    347650    2
# 24           3_2017             38    175693    3
# 25           1_2017             21     33956    1
# 26           1_2017             40     23216    2
# 27           1_2017             14     13866    3
# 28           2_2015             14    131702    1
# 29           2_2015             21    101552    2
# 30           2_2015             40     86878    3
# 31           2_2016             14    131765    1
# 32           2_2016             21    107374    2
# 33           2_2016             40     93228    3
# 34           5_2017             38    102533    1
# 35           5_2017             14     75000    2
# 36           5_2017             37     70223    3
# 37           4_2016             36    382783    1
# 38           4_2016             38    348430    2
# 39           4_2016             37    278588    3
# 40           6_2017              7     26238    1
# 41           6_2017             40     22011    2
# 42           6_2017             14     20778    3
# 43           3_2015             21   1133017    1
# 44           3_2015             36    449311    2
# 45           3_2015             38    398052    3
# 46           1_2016             21     66975    1
# 47           1_2016             40     38369    2
# 48           1_2016             78     27160    3
# 49           5_2015             38    196455    1
# 50           5_2015             37    151789    2
# 51           5_2015             14    130146    3
# 52           6_2016              7     59420    1
# 53           6_2016             38     47491    2
# 54           6_2016             14     42775    3
#Plot based
plot <- ggplot(SparkR::collect(Violation_Time_bucket_Year_Codes),
               aes(x = Year_time_bucket , y = REC_COUNT,col=`violation code`,label=`violation code`)) +
  geom_point() + 
  geom_label_repel(aes(label = `violation code`),
                   box.padding   = 0.35, 
                   point.padding = 0.5,
                   segment.color = 'grey50') +
  theme_classic()+ xlab("Year_time_bucket") + ylab("REC_COUNT")
plot
#Please zoom the plot for better viewing

# We can see that 3rd bucket that is 8-11 morning hrs the counts are really high for all three years
# 21,36,38 being the key violation codes, atlthough 2017 has seen improment overall and these codes are repeated here as well
# For the first window 21, 40 are common one and two, and 78 is replaced by 14 in 2017 post 2015 & 2016
# For 2nd window 14,21,40 are first three across years
# For 4th window 36,37,38 are interchanging across three  years
# 38 is common top for 5th window
# 37 is 2nd for first two year and 3rd for the last one
# 7 for 1st and 14 other two yearas is filling up the remaing positions 
# 7 is common 1st in the 6th bucket 
# 38,14 40 are filling up the other postions across the years

#So in a day violations start with codes 21,36,38 as time passed 14, 40 appears then afterwords again 36,37,38 dominate 
#follwed by introduction of 7 and finishing with 38,14,40

# All 3 top violations happened in 3rd time window in 2016
# Maximum violation in code 36,38 happened in 4th time window in 2015 and 2017
# Maximum violation in code 21 happened in 1st time window in 2015
# Maximum violation in code 21 happened in  2nd time window in 2017


#------------------------------------------------------------------------------------------------------------------------------------------------------

#6.Let's try and find some seasonality in this data
#First, divide the year into some number of seasons, 
#and find frequencies of tickets for each season.
#(Hint: Use Issue Date to segregate into seasons)
#Then, find the three most common violations for each of these seasons.
#(Hint: A similar approach can be used as mention in the hint for question 4.)

#lets divide the year into 4 set of month , 1-3,4-6,7-9 & 1-12
NYC_Ticket_Base$Issue_Date_Month<-SparkR::month(NYC_Ticket_Base$"Issue Date")
createOrReplaceTempView(NYC_Ticket_Base, "NYC_Ticket_Base_tab")

#Overall analysis
NYC_Ticket_Base<-SparkR::sql("select NYC_Ticket_Base_tab.*,
                             case 
                             when Issue_Date_Month between 1 and 3 then 1
                             when  Issue_Date_Month between 4 and 6 then 2
                             when  Issue_Date_Month between 7 and 9 then 3
                             when  Issue_Date_Month between 10 and 12 then 4 end Month_bucket
                             from NYC_Ticket_Base_tab")
createOrReplaceTempView(NYC_Ticket_Base, "NYC_Ticket_Base_tab")

# Season wise spread across the three years
Month_Wise<-SparkR::sql("select Month_bucket,count(*) as REC_COUNT from NYC_Ticket_Base_tab group by Month_bucket")
Month_Wise <- SparkR::arrange(Month_Wise, Month_Wise$Month_bucket)
head(Month_Wise,4)
# Month_bucket REC_COUNT                                                        
            1   6489169
            2   7108573
            3   4692555
            4   4897743

# Season wise violation code analysis across all the three years 
Month_Wise_Violation_1 <- filter(Month_Wise_Violation, Month_Wise_Violation$Month_bucket == 1)
head(arrange(Month_Wise_Violation_1, desc(Month_Wise_Violation_1$REC_COUNT)))
#Month_bucket violation code REC_COUNT
#            1             21    824866
#            1             38    759550
#            1             36    688665
#            1             14    571222
#            1             37    404433
#            1             20    397134
            
Month_Wise_Violation_2 <- filter(Month_Wise_Violation, Month_Wise_Violation$Month_bucket == 2)
head(arrange(Month_Wise_Violation_2, desc(Month_Wise_Violation_2$REC_COUNT)))
# Month_bucket violation code REC_COUNT
#            2             21   1033621
#            2             36    740325
#            2             38    727504
#            2             14    632942
#            2             37    427841
#            2             20    418218            

Month_Wise_Violation_3 <- filter(Month_Wise_Violation, Month_Wise_Violation$Month_bucket == 3)
head(arrange(Month_Wise_Violation_3, desc(Month_Wise_Violation_3$REC_COUNT)))
# Month_bucket violation code REC_COUNT
#            3             21    713757
#            3             38    493050
#            3             14    405934
#            3             36    364038
#            3             37    293070
#            3             20    277801  
          
Month_Wise_Violation_4 <- filter(Month_Wise_Violation, Month_Wise_Violation$Month_bucket == 4)
head(arrange(Month_Wise_Violation_4, desc(Month_Wise_Violation_4$REC_COUNT)))
#  Month_bucket violation code REC_COUNT
#            4             36    751179
#            4             21    699299
#            4             38    482932
#            4             14    392281
#            4             20    277341
#            4             37    270932
            
Month_Wise_Violation<-SparkR::sql("select Month_bucket,`violation code` ,count(*) REC_COUNT
                                  from NYC_Ticket_Base_tab group by Month_bucket,`violation code` ")

Month_Wise_Violation<-SparkR::collect(SparkR::filter(Month_Wise_Violation, Month_Wise_Violation$REC_COUNT>50000))
plot <- ggplot(Month_Wise_Violation,aes(x = Month_bucket, y = REC_COUNT,col=`violation code`,label=`violation code`)) +
  geom_point() + 
  geom_label_repel(aes(label = `violation code`),
                   box.padding   = 0.35, 
                   point.padding = 0.5,
                   segment.color = 'grey50') +
  theme_classic()+ xlab("Season") + ylab("REC_COUNT")
plot
#all seasons/months have 21,38,36, 14 as most common in differnt order

#Year wise analysis
Month_year_Wise_Violation<-SparkR::sql("select * from (
                                       select Month_year_Bucket,`violation code`,REC_COUNT,
                                       dense_rank() OVER (PARTITION BY Month_year_Bucket ORDER BY REC_COUNT DESC) as Rank
                                       from (
                                       select concat(Month_bucket,'_',Issue_Date_Year) Month_year_Bucket,
                                       `violation code` ,count(*) REC_COUNT
                                       from NYC_Ticket_Base_tab
                                       group by concat(Month_bucket,'_',Issue_Date_Year),`violation code` ) T) V
                                       where  Rank<=3
                                       ")
head(Month_year_Wise_Violation,nrow(Month_year_Wise_Violation))
#Yearly Month/Seasonwise violation codes and records
#     Month_year_Bucket violation code REC_COUNT Rank                              
# 1             3_2016             21    345564    1
# 2             3_2016             38    220875    2
# 3             3_2016             36    204171    3
# 4             4_2017             46       209    1
# 5             4_2017             40       132    2
# 6             4_2017             21       116    3
# 7             4_2015             21    388242    1
# 8             4_2015             36    375921    2
# 9             4_2015             38    245213    3
# 10            1_2015             38    226022    1
# 11            1_2015             21    175707    2
# 12            1_2015             14    159197    3
# 13            2_2017             21    354076    1
# 14            2_2017             36    266183    2
# 15            2_2017             14    232710    3
# 16            3_2017             21       243    1
# 17            3_2017             46       202    2
# 18            3_2017             40       112    3
# 19            1_2017             21    333263    1
# 20            1_2017             36    293799    2
# 21            1_2017             38    256831    3
# 22            2_2015             21    369613    1
# 23            2_2015             38    276722    2
# 24            2_2015             14    214645    3
# 25            2_2016             21    309932    1
# 26            2_2016             36    285092    2
# 27            2_2016             38    223546    3
# 28            4_2016             36    375258    1
# 29            4_2016             21    310941    2
# 30            4_2016             38    237713    3
# 31            3_2015             21    367950    1
# 32            3_2015             38    272166    2
# 33            3_2015             14    217383    3
# 34            1_2016             21    315896    1
# 35            1_2016             36    294616    2
# 36            1_2016             38    276697    3

#Let's visualize this inference
plot <- ggplot(SparkR::collect(Month_year_Wise_Violation),aes(x = Month_year_Bucket, y = REC_COUNT,col=`violation code`,label=`violation code`)) +
  geom_point() + 
  geom_label_repel(aes(label = `violation code`),
                   box.padding   = 0.35, 
                   point.padding = 0.5,
                   segment.color = 'grey50') +
  theme_classic()+ xlab("Month_year_Bucket") + ylab("REC_COUNT")
plot
# 
#Violation codes analysis
#21 38 are common for first 3 months across years
#14 for first year followed by 36 for remaining months are the norm
#
#21 is the category for 2nd set of month in all the three years 
#with differnce being remarkably high in 2015 & 2017
# 38,36,14 share 2 slot each across the years
#
#Year 2017 have very low counts for this month
# 21,38 again feature as dominating the for years 2015 and 2016
# 14, 36 are in 3rd ranking for these two years respectively
#
# 46 and 40 are the odd one for 2017
#2017 observation for 4th set is same as for 3rd month 
#21,36,38 are the are interchangely presnt for 2015 and 2016

#------------------------------------------------------------------------------------------------------------------------------------------------------

#7.The fines collected from all the parking violation constitute a revenue source for the NYC police department. Let’s take an example of estimating that for the three most commonly occurring codes.
#Find total occurrences of the three most common violation codes
#Then, visit the website:
#  http://www1.nyc.gov/site/finance/vehicles/services-violation-codes.page
#It lists the fines associated with different violation codes.
#They're divided into two categories, one for the highest-density locations of the city, 
#the other for the rest of the city. For simplicity, take an average of the two.
#Using this information, find the total amount collected for the three violation codes with maximum tickets. 
#State the code which has the highest total collection.
#What can you intuitively infer from these findings?

TOP_3_violation_code<-SparkR::sql("select `violation code`,count(*) REC_COUNT 
                                  from NYC_Ticket_Base_tab 
                                  group by `violation code` order by count(*) desc limit 3")

head(TOP_3_violation_code,3)
#violation code REC_COUNT  Manhattan_fine   all_others avg_fine    Definition                                     
#21              3271543         65             45        55       Street Cleaning: No parking where parking is not allowed by sign, street marking or traffic control device.
#36              2544207         50             50        50       Exceeding the posted speed limit in or near a designated school zone.  
#38              2463036         65             35        50       (38) Failing to show a receipt or tag in the windshield.Drivers get a 5-minute grace period past the expired time on parking meter receipts.

#Total amount collected for top 3 violations - calculation
#Violation code 21
3271543*55
#179934865

#Violation code 36
2544207*50
#127210350

#Violation code 38
2463036*50
#123151800

#Parking related violation are most common type and are a good source of revenue

#Year wise analysis
TOP_3_violation_code_year<-SparkR::sql("select Issue_Date_Year,`violation code`,count(*) REC_COUNT 
                                       from NYC_Ticket_Base_tab 
                                       group by Issue_Date_Year,`violation code` order by count(*) desc")

#2015
TOP_3_violation_code_year_2015<-SparkR::collect(SparkR::filter(TOP_3_violation_code_year, TOP_3_violation_code_year$Issue_Date_Year == 2015))
head(TOP_3_violation_code_year_2015,3)
#Top 3 violation codes in 2015
#   Issue_Date_Year violation code REC_COUNT
# 1            2015             21   1301512
# 2            2015             38   1020123
# 3            2015             36    825088

#2016
TOP_3_violation_code_year_2016<-SparkR::collect(SparkR::filter(TOP_3_violation_code_year, TOP_3_violation_code_year$Issue_Date_Year == 2016))
head(TOP_3_violation_code_year_2016,3)
#Top 3 violation codes in 2016
#   Issue_Date_Year violation code REC_COUNT
# 1            2016             21   1282333
# 2            2016             36   1159137
# 3            2016             38    958831

#2017
TOP_3_violation_code_year_2017<-SparkR::collect(SparkR::filter(TOP_3_violation_code_year, TOP_3_violation_code_year$Issue_Date_Year == 2017))
head(TOP_3_violation_code_year_2017,3)
#Top 3 violation codes in 2017
#   Issue_Date_Year violation code REC_COUNT
# 1            2017             21    687698
# 2            2017             36    559982
# 3            2017             38    484082

#Violation codes 21,36,38 are common across all 3 years

#Let's visualize these inferernces
##Reducing counts of smaller numbers for plotting
TOP_3_violation_code_year<-SparkR::collect(SparkR::filter(TOP_3_violation_code_year, TOP_3_violation_code_year$REC_COUNT>100000))

plot <- ggplot(TOP_3_violation_code_year,aes(x = Issue_Date_Year, y = REC_COUNT,col=`violation code`,label=`violation code`)) +
  geom_point() + 
  geom_label_repel(aes(label = `violation code`),
                   box.padding   = 0.35, 
                   point.padding = 0.5,
                   segment.color = 'grey50') +
  theme_classic()+ xlab("Issue_Date_Year") + ylab("REC_COUNT")
plot

#21,36,38 are top violation codes across the years

#Yearly amounts

#2015-21
1301512*55
#71583160

#2016-21
1282333*55
#70528315

#2017-21
687698*55
#37823390

#2015-36
825088*50
#41254400

#2016-36
1159137*50
#57956850

#2016-36
559982*50
#27999100

#2015-38
1020123*50
#51006150

#2016-38
958831*50
#47941550

#2017-38
484082*50
#24204100

#2015+2016+2017-total amount collected for 3 top violation codes
#Violation code 21
(1301512+1282333+687698)*55
#179934865

#Violation code 36
(825088+1159137+559982)*50
#127210350

#Violation code 38
(1020123+958831+484082)*50
#123151800
#Parking related violation are most common violation type and are a good source of revenue

#######################################################################################################################################################
#7.Closure
#######################################################################################################################################################
sparkR.stop()