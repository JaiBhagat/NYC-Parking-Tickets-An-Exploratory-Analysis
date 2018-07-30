library(SparkR)
library(dplyr)
sparkR.session(master='local')
rm(list=ls())
nyc_2015 <- read.df("s3://bigdata-analytics-assignment/nyc_parking_case_study/Parking_Violations_Issued_-_Fiscal_Year_2015.csv",
                    source = "csv",inferSchema="true",header="true")

nyc_2016 <- read.df("s3://bigdata-analytics-assignment/nyc_parking_case_study/Parking_Violations_Issued_-_Fiscal_Year_2016.csv",
                    source = "csv",inferSchema="true",header="true")

nyc_2017 <- read.df("s3://bigdata-analytics-assignment/nyc_parking_case_study/Parking_Violations_Issued_-_Fiscal_Year_2017.csv",
                    source = "csv",inferSchema="true",header="true")



######################################################################################################### 
#                               Data Cleaning
##########################################################################################################

# checking the number of rows in the 3 files in the unique column..Assuming the summons number as unique column 
nrow(nyc_2015) #11809233
head(select(nyc_2015, countDistinct(nyc_2015$`Summons Number`))) #10951256 

nrow(nyc_2016) #10626899
head(select(nyc_2016, countDistinct(nyc_2016$`Summons Number`))) #10626899 

nrow(nyc_2017) #10803028
head(select(nyc_2017, countDistinct(nyc_2017$`Summons Number`))) #10803028 

# checking for the number of columns 
ncol(nyc_2015) #51 cols
ncol(nyc_2016) #51 cols
ncol(nyc_2017) #43 cols

# structure of three datasets
head(nyc_2015)
head(nyc_2016)
head(nyc_2017)

# Checking the column names and removing if anything not required
names(nyc_2015)

# [1] "Summons Number"                    "Plate ID"                          "Registration State"               
# [4] "Plate Type"                        "Issue Date"                        "Violation Code"                   
# [7] "Vehicle Body Type"                 "Vehicle Make"                      "Issuing Agency"                   
# [10] "Street Code1"                      "Street Code2"                      "Street Code3"                     
# [13] "Vehicle Expiration Date"           "Violation Location"                "Violation Precinct"               
# [16] "Issuer Precinct"                   "Issuer Code"                       "Issuer Command"                   
# [19] "Issuer Squad"                      "Violation Time"                    "Time First Observed"              
# [22] "Violation County"                  "Violation In Front Of Or Opposite" "House Number"                     
# [25] "Street Name"                       "Intersecting Street"               "Date First Observed"              
# [28] "Law Section"                       "Sub Division"                      "Violation Legal Code"             
# [31] "Days Parking In Effect    "        "From Hours In Effect"              "To Hours In Effect"               
# [34] "Vehicle Color"                     "Unregistered Vehicle?"             "Vehicle Year"                     
# [37] "Meter Number"                      "Feet From Curb"                    "Violation Post Code"              
# [40] "Violation Description"             "No Standing or Stopping Violation" "Hydrant Violation"                
# [43] "Double Parking Violation"          "Latitude"                          "Longitude"                        
# [46] "Community Board"                   "Community Council "                "Census Tract"                     
# [49] "BIN"                               "BBL"                               "NTA" 

names(nyc_2017)

# [1] "Summons Number"                    "Plate ID"                          "Registration State"               
# [4] "Plate Type"                        "Issue Date"                        "Violation Code"                   
# [7] "Vehicle Body Type"                 "Vehicle Make"                      "Issuing Agency"                   
# [10] "Street Code1"                      "Street Code2"                      "Street Code3"                     
# [13] "Vehicle Expiration Date"           "Violation Location"                "Violation Precinct"               
# [16] "Issuer Precinct"                   "Issuer Code"                       "Issuer Command"                   
# [19] "Issuer Squad"                      "Violation Time"                    "Time First Observed"              
# [22] "Violation County"                  "Violation In Front Of Or Opposite" "House Number"                     
# [25] "Street Name"                       "Intersecting Street"               "Date First Observed"              
# [28] "Law Section"                       "Sub Division"                      "Violation Legal Code"             
# [31] "Days Parking In Effect    "        "From Hours In Effect"              "To Hours In Effect"               
# [34] "Vehicle Color"                     "Unregistered Vehicle?"             "Vehicle Year"                     
# [37] "Meter Number"                      "Feet From Curb"                    "Violation Post Code"              
# [40] "Violation Description"             "No Standing or Stopping Violation" "Hydrant Violation"                
# [43] "Double Parking Violation"  

# Taking the backup of every year files for safe side
backup_2015 <- nyc_2015
backup_2016 <- nyc_2016
backup_2017 <- nyc_2017

#Deleting all the null columns and taking all of them into equal number of columns to combine
nyc_2015 <- nyc_2015[,-c(38:51)]
nyc_2016 <- nyc_2016[,-c(38:51)]
nyc_2017 <- nyc_2017[,-c(38:43)]

#Now we will rowbind the above 3 dataframes to create one master dataframe

nyc_data <- rbind(nyc_2015, nyc_2016, nyc_2017)

# As we are here to analyse only three years of data (2015,2016,2017) we are filtering those datasets with the years
# Filtering only 2015 data
createOrReplaceTempView(nyc_data, "tble_view2015")
parking_nyc_2015 <- SparkR::sql("select * from tble_view2015 where year(TO_DATE(CAST(UNIX_TIMESTAMP(`Issue Date`,'MM/dd/yyyy') AS TIMESTAMP))) = 2015")
head(parking_nyc_2015)

# Filtering only 2016 data
createOrReplaceTempView(nyc_data, "tble_view2016")
parking_nyc_2016 <- SparkR::sql("select * from tble_view2016 where year(TO_DATE(CAST(UNIX_TIMESTAMP(`Issue Date`,'MM/dd/yyyy') AS TIMESTAMP))) = 2016")
head(parking_nyc_2016)

# Filtering only 2017 data
createOrReplaceTempView(nyc_data, "tble_view2017")
parking_nyc_2017 <- SparkR::sql("select * from tble_view2017 where year(TO_DATE(CAST(UNIX_TIMESTAMP(`Issue Date`,'MM/dd/yyyy') AS TIMESTAMP))) = 2017 ")
head(parking_nyc_2017)



######################################################################################################################################
#                                         Examine the data
######################################################################################################################################
######################################## Question No - 1 ##################################################
#1. Find total number of tickets for each year.
# Use select() to view the column returned by countDistinct()
head(select(parking_nyc_2015, countDistinct(parking_nyc_2015$`Summons Number`)))  #10903411

head(select(parking_nyc_2016, countDistinct(parking_nyc_2016$`Summons Number`)))  #10241012

head(select(parking_nyc_2017, countDistinct(parking_nyc_2017$`Summons Number`)))  # 5433018  

######################################## Question No - 2 ##################################################
#2. Find out how many unique states the cars which got parking tickets came from.


head(select(parking_nyc_2015, countDistinct(parking_nyc_2015$`Registration State`)))
#69 states

head(select(parking_nyc_2016, countDistinct(parking_nyc_2016$`Registration State`))) 
#68 states

head(select(parking_nyc_2017, countDistinct(parking_nyc_2017$`Registration State`)))
#65 states

#Now we will rowbind the above 3 filtered dataframes to perform the entire analysis

nyc_filtered_data <- rbind(parking_nyc_2015, parking_nyc_2016, parking_nyc_2017)

createOrReplaceTempView(nyc_filtered_data,"nyc_data_tbl")
######################################## Question No - 3 ##################################################
#3. Some parking tickets don't have addresses on them, which is cause for concern.
#Find out how many such tickets there are.

#Assuming the House Number and Street Name as the address and checking whether they are null or not

missing_address <- SparkR::sql("select count(*) from nyc_data_tbl where `House Number` IS NULL and `Street Name` IS NULL")
head(missing_address)
# Ans : 11029 entries dont have address


######################################################################################################################################
#                                                   Aggregation Tasks
######################################################################################################################################
######################################## Question No - 1 ##################################################
# 1.How often does each violation code occur? (frequency of violation codes - find the top 5)

nyc_violation_cd_cnt <- summarize(groupBy(nyc_filtered_data,nyc_filtered_data$`Violation Code`), 
                                  count=n(nyc_filtered_data$`Violation Code`))
head(arrange(nyc_violation_cd_cnt,desc(nyc_violation_cd_cnt$count)))

# Ans : Top 5 violation codes 21,36,38,14,37
# Violation Code   count
# 1             21 3869197
# 2             36 3111439
# 3             38 2952526
# 4             14 2286502
# 5             37 1699486
# 6             20 1574394
######################################## Question No - 2 ##################################################

# 2.How often does each vehicle body type get a parking ticket? How about the vehicle make? 
# (find the top 5 for both)

nyc_body_type_cnt <- summarize(groupBy(nyc_filtered_data,nyc_filtered_data$`Vehicle Body Type`), 
                               count=n(nyc_filtered_data$`Vehicle Body Type`))
head(arrange(nyc_body_type_cnt,desc(nyc_body_type_cnt$count)))

#   Vehicle Body Type   count
# 1              SUBN 9100113
# 2              4DSD 7768379
# 3               VAN 3809438
# 4              DELV 1903396
# 5               SDN 1097903
# 6              2DSD  712067

# How about the vehicle make?  (find the top 5 for both)
nyc_vehc_make_cnt <- summarize(groupBy(nyc_filtered_data,nyc_filtered_data$`Vehicle Make`), 
                               count=n(nyc_filtered_data$`Vehicle Make`))
head(arrange(nyc_vehc_make_cnt,desc(nyc_vehc_make_cnt$count)))

#   Vehicle Make   count
# 1         FORD 3368392
# 2        TOYOT 2984933
# 3        HONDA 2650193
# 4        NISSA 2213081
# 5        CHEVR 1924180
# 6        FRUEH 1083513
######################################## Question No - 3 ##################################################

#3. A precinct is a police station that has a certain zone of the city under its command.
#Find the (5 highest) frequencies of:
#  3.1. Violating Precincts (this is the precinct of the zone where the violation occurred)

vio_precinct = summarize(groupBy(nyc_filtered_data, nyc_filtered_data$`Violation Precinct`), count = n(nyc_filtered_data$`Violation Precinct`))
head(arrange(vio_precinct, desc(vio_precinct$count)), 10)
#    Violation Precinct   count
# 1                   0 4742644 --> No precinct named 0 is there in NYC.So considering them as blank values
# 2                  19 1410169
# 3                  14  894392
# 4                  18  854023
# 5                   1  805149
# 6                 114  756198
# 7                  13  695920
# 8                 109  582650
# 9                  17  520659
# 10                 70  481034

# Violation precincts for the year 2015
vio_2015_precinct = summarize(groupBy(parking_nyc_2015, parking_nyc_2015$`Violation Precinct`), count = n(parking_nyc_2015$`Violation Precinct`))
head(arrange(vio_2015_precinct, desc(vio_2015_precinct$count)), 10)
#    Violation Precinct   count
# 1                   0 1841922 --> No precinct named 0 is there in NYC.So considering them as blank values
# 2                  19  610323
# 3                  18  398335
# 4                  14  391344
# 5                 114  328643
# 6                   1  321147
# 7                  13  293204
# 8                 109  259570
# 9                  17  243861
# 10                 20  226259
# Violation precincts for the year 2016
vio_2016_precinct = summarize(groupBy(parking_nyc_2016, parking_nyc_2016$`Violation Precinct`), count = n(parking_nyc_2016$`Violation Precinct`))
head(arrange(vio_2016_precinct, desc(vio_2016_precinct$count)), 10)

#    Violation Precinct   count
# 1                   0 1975109 --> No precinct named 0 is there in NYC.So considering them as blank values
# 2                  19  525379
# 3                   1  309286
# 4                  14  299473
# 5                  18  286534
# 6                 114  280090
# 7                  13  277596
# 8                 109  219044
# 9                  70  185504
# 10                 17  173522
# Violation precincts for the year 2017
vio_2017_precinct = summarize(groupBy(parking_nyc_2017, parking_nyc_2017$`Violation Precinct`), count = n(parking_nyc_2017$`Violation Precinct`))
head(arrange(vio_2017_precinct, desc(vio_2017_precinct$count)), 10)

#    Violation Precinct  count
# 1                   0 925613 --> No precinct named 0 is there in NYC.So considering them as blank values
# 2                  19 274467
# 3                  14 203575
# 4                   1 174716
# 5                  18 169154
# 6                 114 147465
# 7                  13 125120
# 8                 109 104036
# 9                  17 103276
# 10                 70  96592

# 3.2. Issuing Precincts (this is the precinct that issued the ticket)

Issuer_precinct = summarize(groupBy(nyc_filtered_data, nyc_filtered_data$`Issuer Precinct`), count = n(nyc_filtered_data$`Issuer Precinct`))
head(arrange(Issuer_precinct, desc(Issuer_precinct$count)), 10)

#    Issuer Precinct   count
# 1                0 5451818 --> No precinct named 0 is there in NYC.So considering them as blank values
# 2               19 1372464
# 3               14  870724
# 4               18  831708
# 5                1  781152
# 6              114  742132
# 7               13  680403
# 8              109  589712
# 9               17  507055
# 10              20  474239

# Issuer precincts for the year 2015
Issuer_2015_precinct = summarize(groupBy(parking_nyc_2015, parking_nyc_2015$`Issuer Precinct`), count = n(parking_nyc_2015$`Issuer Precinct`))
head(arrange(Issuer_2015_precinct, desc(Issuer_2015_precinct$count)), 10)
# Issuer Precinct   count
# 1                0 2115263 --> No precinct named 0 is there in NYC.So considering them as blank values
# 2               19  593001
# 3               18  390617
# 4               14  379621
# 5              114  323190
# 6                1  313025
# 7               13  286114
# 8              109  264932
# 9               17  238142
# 10              20  223444
# Issuer precincts for the year 2016
Issuer_2016_precinct = summarize(groupBy(parking_nyc_2016, parking_nyc_2016$`Issuer Precinct`), count = n(parking_nyc_2016$`Issuer Precinct`))
head(arrange(Issuer_2016_precinct, desc(Issuer_2016_precinct$count)), 10)
# Issuer Precinct   count
# 1                0 2257892 --> No precinct named 0 is there in NYC.So considering them as blank values
# 2               19  512496
# 3                1  299375
# 4               14  290600
# 5               18  278083
# 6              114  274871
# 7               13  271796
# 8              109  219644
# 9               70  177770
# 10              20  170370
# Issuer precincts for the year 2017
Issuer_2017_precinct = summarize(groupBy(parking_nyc_2017, parking_nyc_2017$`Issuer Precinct`), count = n(parking_nyc_2017$`Issuer Precinct`))
head(arrange(Issuer_2017_precinct, desc(Issuer_2017_precinct$count)), 10)
# Issuer Precinct   count
# 1                0 1078663 --> No precinct named 0 is there in NYC.So considering them as blank values
# 2               19  266967
# 3               14  200503
# 4                1  168752
# 5               18  163008
# 6              114  144071
# 7               13  122493
# 8              109  105136
# 9               17  100653
# 10              70   92328

# from the above aggregations we can conclude that the most common violation causing Issuer
# Precincts are 19,14,18 
# the top among these is 19 precinct Manhattan 153 East 67th Street

######################################## Question No - 4 ##################################################
# 4. Find the violation code frequency across 3 precincts which have issued the most number of tickets - 
# do these precinct zones have an exceptionally high frequency of certain violation codes? 
# Are these codes common across precincts?

# From q3 we concluded that most tickets issued precincts are 19,14,18 
#so finding the violation code frequency for each of them 

# For Issuer precinct 19
Issuer_precinct_19 <- subset(nyc_filtered_data,nyc_filtered_data$`Issuer Precinct` ==19)

Iss_prec_19_vio_cd_cnt <- summarize(groupBy(Issuer_precinct_19,Issuer_precinct_19$`Violation Code`), 
                                    count=n(Issuer_precinct_19$`Violation Code`))
head(arrange(Iss_prec_19_vio_cd_cnt,desc(Iss_prec_19_vio_cd_cnt$count)))
# Violation Code  count
# 1             38 201237
# 2             37 193419
# 3             46 188145
# 4             14 153711
# 5             21 142611
# 6             16 116448

# For Issuer precinct 14
Issuer_precinct_14 <- subset(nyc_filtered_data,nyc_filtered_data$`Issuer Precinct` ==14)

Iss_prec_14_vio_cd_cnt <- summarize(groupBy(Issuer_precinct_14,Issuer_precinct_14$`Violation Code`), 
                                    count=n(Issuer_precinct_14$`Violation Code`))
head(arrange(Iss_prec_14_vio_cd_cnt,desc(Iss_prec_14_vio_cd_cnt$count)))


# Violation Code  count
# 1             14 178877
# 2             69 171058
# 3             31  98615
# 4             47  70904
# 5             42  60347
# 6             46  29377

# For Issuer precinct 18
Issuer_precinct_18 <- subset(nyc_filtered_data,nyc_filtered_data$`Issuer Precinct` ==18)

Iss_prec_18_vio_cd_cnt <- summarize(groupBy(Issuer_precinct_18,Issuer_precinct_18$`Violation Code`), 
                                    count=n(Issuer_precinct_18$`Violation Code`))
head(arrange(Iss_prec_18_vio_cd_cnt,desc(Iss_prec_18_vio_cd_cnt$count)))


# Violation Code  count
# 1             14 257748
# 2             69 115832
# 3             47  63572
# 4             31  61690
# 5             42  40750
# 6             46  38043



######################################## Question No - 5 ##################################################

#5 You d want to find out the properties of parking violations across different times of the day:
#The Violation Time field is specified in a strange format.
#Find a way to make this into a time attribute that you can use to divide into groups.
# Divide 24 hours into 6 equal discrete bins of time. The intervals you choose are at your discretion. 
# Creating a time column and appending it to the master frame
data_time_bins <- SparkR::sql("select * from (select *
                              ,case when substring (`Violation Time`,0, 2) in ('00','01','02','03')  and substring(`Violation Time` ,5,1) = 'A' then 'Midnight'
                              when substring(`Violation Time`,0,2) in ('04','05','06','07') and substring(`Violation Time` ,5,1) = 'A' then 'Early_Morning'
                              when substring(`Violation Time`,0,2) in ('08','09','10','11') and substring(`Violation Time` ,5,1) = 'A' then 'Morning'
                              when substring(`Violation Time` , 0,2) in ('12','01','02','03') and substring(`Violation Time`, 5,1) = 'P' then 'After_Noon'
                              when substring(`Violation Time`, 0,2) in ('04','05','06','07') and substring(`Violation Time`, 5,1) = 'P' then 'Evening'
                              when substring(`Violation Time`,0,2) in ('08','09','10','11') and substring(`Violation Time`, 5,1) = 'P' then 'Night'
                              else '' end as Time from nyc_data_tbl ) temp")

head(data_time_bins)

createOrReplaceTempView(data_time_bins,"tbl_time_bins")

# For each of these groups, find the 3 most commonly occurring violations
# for Early_morning 04 AM to 07 AM

vio_cd_cnt_early_morning <- SparkR::sql("select `Violation Code`,count(*) Vio_cnt
                                        from tbl_time_bins
                                        where Time = 'Early_Morning'
                                        group by `Violation Code`
                                        order by Vio_cnt desc")

head(vio_cd_cnt_early_morning,3)

# Violation Code Vio_cnt
# 1             14  357489
# 2             21  288943
# 3             40  255846

# for Morning 08 AM to 11 AM

vio_cd_cnt_morning <- SparkR::sql("select `Violation Code`,count(*) Vio_cnt
                                  from tbl_time_bins
                                  where Time = 'Morning'
                                  group by `Violation Code`
                                  order by Vio_cnt desc")

head(vio_cd_cnt_morning,3)

# Violation Code Vio_cnt
# 1             21 3035117
# 2             36 1551973
# 3             38  998822

# for After_Noon 12 PM to 03 PM

vio_cd_cnt_After_Noon <- SparkR::sql("select `Violation Code`,count(*) Vio_cnt
                                     from tbl_time_bins
                                     where Time = 'After_Noon'
                                     group by `Violation Code`
                                     order by Vio_cnt desc")

head(vio_cd_cnt_After_Noon,3) 
# Violation Code Vio_cnt
# 1             36 1334139
# 2             38 1274591
# 3             37  958015

# for Evening 04 PM to 07 PM

vio_cd_cnt_Evening <- SparkR::sql("select `Violation Code`,count(*) Vio_cnt
                                  from tbl_time_bins
                                  where Time = 'Evening'
                                  group by `Violation Code`
                                  order by Vio_cnt desc")

head(vio_cd_cnt_Evening,3) 
# Violation Code Vio_cnt
# 1             38  540791
# 2             37  400787
# 3             14  362331

# for Night 08 PM to 11 PM

vio_cd_cnt_Night <- SparkR::sql("select `Violation Code`,count(*) Vio_cnt
                                from tbl_time_bins
                                where Time = 'Night'
                                group by `Violation Code`
                                order by Vio_cnt desc")

head(vio_cd_cnt_Night,3) 

# Violation Code Vio_cnt
# 1              7  159125
# 2             38  130867
# 3             40  114952

# for Midnight 12 AM to 03 AM

vio_cd_cnt_Midnight <- SparkR::sql("select `Violation Code`,count(*) Vio_cnt
                                   from tbl_time_bins
                                   where Time = 'Midnight'
                                   group by `Violation Code`
                                   order by Vio_cnt desc")

head(vio_cd_cnt_Midnight,3) 

# Violation Code Vio_cnt
# 1             21  174056
# 2             40  103113
# 3             78   75099

# 21,38,40 are the most commonly occuring codes
# Now, try another direction. For the 3 most commonly occurring violation codes, 
# find the most common times of day (in terms of the bins from the previous part)
common_times <- SparkR::sql("select Time, count(*) violation_count 
                            from tbl_time_bins
                            where `Violation Code` in ('38', '40', '21')
                            group by Time
                            order by violation_count Desc")
head(common_times,3)

#         Time violation_count
# 1    Morning         4358538
# 2 After_Noon         1961385
# 3    Evening          659104

######################################## Question No - 6 ##################################################

# 6. Letâs try and find some seasonality in this data
#First, divide the year into some number of seasons, and find frequencies of tickets for each season.


data_6 <- SparkR::sql("select seasons,count(`summons number`) as cnt from (SELECT `summons number` 
                      ,case when substring(`Issue Date`,0,2) in ('12','01','02') then 'Winter'  
                      when substring(`Issue Date`,0,2) in ('03','04','05') then 'Spring'  
                      when substring(`Issue Date`,0,2) in ('06','07','08') then 'Summer'
                      when substring(`Issue Date`,0,2) in ('09','10','11') then 'Rainy' end as seasons
                      FROM nyc_data_tbl where `summons number` is not null) temp 
                      where `seasons` is not null group by seasons order by cnt desc" )
head(data_6,20)

# Summer: June - August
# Rainy : September - November
# Winter: December - February
# Spring: March - May

# seasons     cnt
# 1  Spring 8531399
# 2  Winter 7030570
# 3  Summer 6047215
# 4   Rainy 5803105

#Then, find the 3 most common violations for each of these season

data_6_1 <- SparkR::sql("select seasons,`Violation Code` as vio_code, count(`summons number`) as smn_cnt from 
                        (SELECT `summons number`
                        ,`Violation Code`
                        ,case when substring(`Issue Date`,0,2) in ('12','01','02') then 'Winter'  
                        when substring(`Issue Date`,0,2) in ('03','04','05') then 'Spring'  
                        when substring(`Issue Date`,0,2) in ('06','07','08') then 'Summer'
                        when substring(`Issue Date`,0,2) in ('09','10','11') then 'Rainy' end as seasons
                        FROM nyc_data_tbl where `summons number` is not null) temp 
                        where `seasons` is not null group by seasons,vio_code  order by smn_cnt desc,seasons " )

head(data_6_1,20)

# seasons vio_code smn_cnt
# 1   Spring       21 1211466
# 2   Summer       21  967365
# 3   Spring       36  945698
# 4   Winter       21  937708
# 5   Spring       38  897690
# 6    Rainy       36  894366
# 7   Winter       38  840291
# 8   Winter       36  818799
# 9    Rainy       21  752658
# 10  Spring       14  725932
# 11  Summer       38  627329
# 12   Rainy       38  587216
# 13  Winter       14  583739
# 14  Spring       37  539357
# 15  Summer       14  519864
# 16  Spring       46  480371
# 17  Spring       20  480275
# 18   Rainy       14  456967
# 19  Summer       36  452576
# 20  Winter       37  439376

######################################## Question No - 7 ##################################################

# 7. The fines collected from all the parking violation constitute a revenue source for the NYC police department. 
# Let's take an example of estimating that for the 3 most commonly occurring codes.
# Find total occurrences of the 3 most common violation codes
# Then, search the internet for NYC parking violation code fines. You will find a website (on the nyc.gov URL) that lists these fines. They're divided into two categories, one for the highest-density locations of the city, the other for the rest of the city. For simplicity, take an average of the two.
# Using this information, find the total amount collected for all of the fines. State the code which has the highest total collection.
# What can you intuitively infer from these findings?

top_violations <- summarize(groupBy(nyc_filtered_data, nyc_filtered_data$`Violation Code`), 
                            Count = n(nyc_filtered_data$`Violation Code`))
top_3_violations_count <- head(arrange(top_violations, desc(top_violations$Count)),3)
total_occurrences <- sum(top_3_violations_count$Count)
# Total Occurrence = 9933162

# Violation Code   Count
# 1             21 3869197
# 2             36 3111439
# 3             38 2952526

# As per NYC website
# Code 21 Avg. Fine = $55
# Code 36 Avg. Fine = $50
# Code 38 Avg. Fine = $50

collection <- mutate(top_3_violations_count, Fine_Amount = c(55,50,50),
                     Total_Amount_Collected = (Count * Fine_Amount))

#   Violation Code   Count Fine_Amount Total_Amount_Collected
# 1             21 3869197          55              212805835
# 2             36 3111439          50              155571950
# 3             38 2952526          50              147626300

total_collection<- sum(collection$Total_Amount_Collected)
# Total Amount Collected = 5,16,004,085

# The violation code 21 has the highest collection



