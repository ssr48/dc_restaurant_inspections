# Civic Hackathon DC
# Issue Number 17
# Author:Shashank Rai

setwd("/Users/shashankrai/GitHub/dc_restaurant_inspections")
getwd()
#Import Data

#Summary
df_sum <- read.csv("output/potential_inspection_summary_data.csv", strip.white = T, na = c("",NA,"#N/A"))

#Violations
df_viol <-read.csv("output/potential_violation_details_data.csv", strip.white = T, na = c("",NA,"#N/A"))

#GeoCoding
df_geo <- read.csv("output/inspection_geocodes.csv", strip.white = T, na = c("",NA,"#N/A"))

#dimensions
dim(df_sum)
# [1] 70157    35
dim(df_viol)
# [1] 208981      5
#What's DCMR_25_Code? New Variable!
dim(df_geo)
# [1] 68363     3

#heads
head(df_sum)
head(df_geo)
head(df_viol)

length(unique(df_sum$inspection_id))
# [1] 70156 - 1 duplicate
length(unique(df_viol$inspection_id))
# [1] 41622
length(unique(df_geo$inspection_id))
# [1] 68362

# There seems to be one repeated observation in df_sum and df_geo
n_occur <- data.frame(table(df_sum$inspection_id))

n_occur[n_occur$Freq > 1,]
#       Var1 Freq
# 3959 4000    2
# Its inspection ID 4000

# A quick look at the data tells us that these are exactly the same observations
df_sum <- unique(df_sum)
df_geo <- unique(df_geo)

# We would want to look at the duplicates in the  violations data. But that does not have the date variable
# We will merge over the violations data and try to find a unique level for analysis. 

main <- merge(df_viol, df_sum, by.x = "inspection_id", by.y = "inspection_id", all.x = T, all.y = T)
main <- merge(main, df_geo, by.x = "inspection_id", by.y = "inspection_id", all.x = T, all.y = T)
dim(main)
# [1] 237515     41

#Confirming all inspection ids are in there
length(unique(main$inspection_id))
# [1] 70156

summary(main)

length(unique(main$inspection_id, main$inspection_date))
# [1] 70187

# This is unexpected. Either 31 restaurants have been inspected twice on the same day or there are 31 obs that 
# were not recorded in summary and geo datasets

length(unique(main$inspection_id, main$inspection_date, main$inspection_time_in, fromLast = F))
# [1] 70187

# Looks like there are 31 observations that were not captured in summary and geo datasets

# For now, we'll keep these observations. 

# Inspection years go as far back as 1931 and go up until 2024. We will keep the valid entries and see if the issue persists

table(main$known_valid)
# False   True 
# 71627 165888 

main<-main[!(main$known_valid== "False"),]

dim(main)
# [1] 165888     41

length(unique(main$inspection_id))
# [1] 38860

main$feature_id <- "restaurant_inspections_overdue"

#generate binary variables for priority, priority foundation, and core violations

main$ifpriority[main$priority_violations > 0] <- 1
main$ifpriority[main$priority_violations == "NA"] <- 0
main$ifpriority[is.na(main$ifpriority)] <- 0
table(main$ifpriority)
#     0      1 
# 125987  39901

main$ifpriorityfoundation[main$priority_foundation_violations > 0] <- 1
main$ifpriorityfoundation[main$priority_foundation_violations == "NA"] <- 0
main$ifpriorityfoundation[is.na(main$ifpriorityfoundation)] <- 0
table(main$ifpriorityfoundation)
#   0      1 
# 117015  48873

main$ifcore[main$core_violations > 0] <- 1
main$ifcore[main$core_violations == "NA"] <- 0
main$ifcore[is.na(main$ifcore)] <- 0
table(main$ifcore)
#   0      1 
# 115436  50452  

# If violations are corrected on site, they do not require a follow up. But not all violations may be corrected on
# site. 
# Priority
main$priority_left <- main$priority_violations - main$priority_violations_corrected_on_site
table(main$priority_left)
#     0     1     2     3     4     5     6     7     8     9    10    11 
# 39244 15150  9290  4516  2493  1194   606   452    88   193    42    65  
table(main$ifpriority)
# 0      1 
# 125245  38301
main$ifpriority[main$priority_left == 0] <- 0
table(main$ifpriority)
#   0      1 
# 125987  39901 


main$priority_foundation_left <- main$priority_foundation_violations - main$priority_foundation_violations_corrected_on_site
table(main$priority_foundation_left)
#     0     1     2     3     4     5     6     7     8     9    10    11 
# 29496 12052  9086  7094  5304  3624  2852  1513  1044   742   266   260 
table(main$ifpriorityfoundation)
#   0      1 
# 117015  48873  
main$ifpriorityfoundation[main$priority_foundation_left == 0] <- 0
table(main$ifpriorityfoundation)
#   0      1 
# 122051  43837

main$core_left <- main$core_violations - main$core_violations_corrected_on_site
table(main$core_left)
#     0     1     2     3     4     5     6     7     8     9    10    11    12    13    14    15    16    17    18 
# 25222  9113  8272  6761  5262  3965  2931  2641  1774  1692  1296  1106   772   586   498   431   198   230    95 
# 19    20    21    23    24 
# 96   161   146    40    45 
table(main$ifcore)
# 0      1 
# 115436  50452  
main$ifcore[main$core_left == 0] <- 0
table(main$ifcore)
#     0      1 
# 117777  48111 

#Routine
main$routine[main$inspection_type == "Routine"] <- 1
main$routine[main$inspection_type == "routine"] <- 1
main$routine[main$inspection_type == "ROUTINE"] <- 1
main$routine[main$inspection_type == "App. Routine"] <- 1
main$routine[main$inspection_type == "Routine (Inaugural Inspection)"] <- 1 
main$routine[main$inspection_type == "Routine / License Renewal"] <- 1 
main$routine[main$inspection_type == "ROUTINE DONE 9/15/10 INFORMATION PROVIDED WHILE IN"] <- 1 
main$routine[main$inspection_type == "Routine/Caterer"] <- 1 
main$routine[main$inspection_type == "Routine/HACCP"] <- 1 
main$routine[main$inspection_type == "ROUTINE/HACCP"] <- 1 
main$routine[main$inspection_type == "Routine/Sweep"] <- 1 
main$routine[main$inspection_type == "ROUTINE/SWEEP"] <- 1 
main$routine[main$inspection_type == "Routune"] <- 1 
main$routine[is.na(main$routine)] <- 0
table(main$routine)
#   0      1 
# 62077 103811   

#Follow up
main$followup[main$inspection_type == "Follow-up"] <- 1
main$followup[main$inspection_type == "2nd follow up"] <- 1
main$followup[main$inspection_type == "follow bup"] <- 1
main$followup[main$inspection_type == "follow up"] <- 1
main$followup[main$inspection_type == "follow uP"] <- 1
main$followup[main$inspection_type == "Follow up"] <- 1
main$followup[main$inspection_type == "Follow Up"] <- 1
main$followup[main$inspection_type == "FOLLOW UP"] <- 1
main$followup[main$inspection_type == "follow-up"] <- 1
main$followup[main$inspection_type == "Folow-up"] <- 1
main$followup[main$inspection_type == "5-Day Extension Follow-up"] <- 1
main$followup[main$inspection_type == "Complaint (Follow-up)"] <- 1
main$followup[main$inspection_type == "Complaint / Follow-up"] <- 1
main$followup[main$inspection_type == "complaint follow up"] <- 1
main$followup[main$inspection_type == "Follow up /License Transfer"] <- 1
main$followup[main$inspection_type == "Follow up, new"] <- 1
main$followup[main$inspection_type == "Follow-up  / License Transfer"] <- 1
main$followup[main$inspection_type == "Follow-up / License Transfer"] <- 1
main$followup[main$inspection_type == "FOLLOW-UP PRE-OP"] <- 1
main$followup[main$inspection_type == "FOLLOW-UP PRE-OPERATIONAL"] <- 1
main$followup[main$inspection_type == "Follow-up Preoperational"] <- 1
main$followup[main$inspection_type == "Follow-up restoration"] <- 1
main$followup[main$inspection_type == "FOLLOWUP RENEWAL"] <- 1
main$followup[main$inspection_type == "HACCP Follow UP"] <- 1
main$followup[main$inspection_type == "HACCP FOLLOW_UP INSPECTION"] <- 1
main$followup[main$inspection_type == "HACCP Follow-up"] <- 1
main$followup[main$inspection_type == "HACCP FOLLOW-UP "] <- 1
main$followup[main$inspection_type == "HACCP FOLLOW-UP INSPECTION"] <- 1
main$followup[main$inspection_type == "License Transfer / Follow-up"] <- 1
main$followup[main$inspection_type == "Preoperational  follow up"] <- 1
main$followup[main$inspection_type == "Preoperational  follow-up"] <- 1
main$followup[main$inspection_type == "Preoperational - Follow-up"] <- 1
main$followup[main$inspection_type == "Preoperational / Follow-up"] <- 1
main$followup[main$inspection_type == "Preoperational Follow Up"] <- 1
main$followup[main$inspection_type == "preopt follow up"] <- 1
main$followup[main$inspection_type == "RCP Follow up"] <- 1
main$followup[main$inspection_type == "Restoration Follow-Up Inspecion"] <- 1 
main$followup[main$inspection_type == "restoration, follow up"] <- 1 
main$followup[main$inspection_type == "ROUTINE FOR FOLLOW-UP"] <- 1 #Routine for follow-up is treated as follow-up and not routine
main$followup[main$inspection_type == "ROUTINE FOR FOLLW UP"] <- 1 #Routine for follow-up is treated as follow-up and not routine
main$followup[is.na(main$followup)] <- 0
table(main$followup)
#     0      1 
# 147551  18337 

# Other types of inspections like complaints or sublease, we don't have data on when these violations became due. 
# We will hence not estimate the overdue nature of the same. 

#Priority Violations require another inspection in 3 days
#Priority Foundation violations require another inspection in 3 days
#Core Violations require another inspection in 14 days
# Routine inspections are based on risk category: RC 1 requires 1 inspection every year, RC 2 requires 2, and so on.

main1 <- data.frame(main$inspection_id, main$feature_id, main$inspection_date, main$inspection_time_in, main$inspection_time_out, 
          main$risk_category, main$ifpriority, main$ifpriorityfoundation, main$ifcore, main$routine, main$followup, main$latitude, main$longitude, 
          main$establishment_name, main$dcmr_25_code) #Not sure what to do with dcmr code

# Next steps
# 1. Create dates for priority, priority foundation, and core follow up
# 2. Check if on those dates the follow-up was done or not done with a binary variable 1 for if a follow-up was 
#     conducted and 0 if not. 
# 3. Repeat for routine grouping by risk categories 


class(main1$main.inspection_date)
main1$inspectiondate1 <- as.Date(main1$main.inspection_date)
class(main1$inspectiondate1)


#Calculate dates of follow up for priority violations
main1$fl_dt_if_priority <- ifelse(main1$main.ifpriority == 1, main1$inspectiondate1 + 3, "NA")
table(main1$fl_dt_if_priority)
library(zoo)
class(main1$fl_dt_if_priority)
main1$fl_dt_if_priority <- as.numeric(main1$fl_dt_if_priority)
main1$fl_dt_if_priority <- as.Date(main1$fl_dt_if_priority)
class(main1$fl_dt_if_priority)
table(main1$fl_dt_if_priority)

summary(main1$fl_dt_if_priority)
#     Min.      1st Qu.       Median         Mean      3rd Qu.         Max.         NA's 
# "2015-07-09" "2016-05-28" "2016-11-03" "2016-10-29" "2017-04-10" "2017-11-11"     "125987" 

table(main1$main.ifpriority)
# 0      1 
# 125987  39901  #Confirming that for each priority violation a follow up date has been calculated


#Calculate dates of follow up for priority foundation violations
main1$fl_dt_if_priorityfound <- ifelse(main1$main.ifpriorityfoundation == 1, main1$inspectiondate1 + 3, "NA")
table(main1$fl_dt_if_priorityfound)
class(main1$fl_dt_if_priorityfound)
main1$fl_dt_if_priorityfound <- as.numeric(main1$fl_dt_if_priorityfound)
main1$fl_dt_if_priorityfound <- as.Date(main1$fl_dt_if_priorityfound)
class(main1$fl_dt_if_priorityfound)
table(main1$fl_dt_if_priorityfound)

summary(main1$fl_dt_if_priorityfound)
#     Min.      1st Qu.       Median         Mean      3rd Qu.         Max.         NA's 
# "2015-07-09" "2016-06-16" "2016-11-20" "2016-11-11" "2017-04-16" "2017-11-11"     "122051"

table(main1$main.ifpriorityfoundation)
#   0      1 
# 122051  43837
# Confirming that for each priority foundation violation a follow up date has been calculated

#Calculate dates of follow up for core violations
main1$fl_dt_if_core <- ifelse(main1$main.ifcore == 1, main1$inspectiondate1 + 14, "NA")
table(main1$fl_dt_if_core)
class(main1$fl_dt_if_core)
main1$fl_dt_if_core <- as.numeric(main1$fl_dt_if_core)
main1$fl_dt_if_core <- as.Date(main1$fl_dt_if_core)
table(main1$fl_dt_if_core)

summary(main1$fl_dt_if_core)
# Min.      1st Qu.       Median         Mean      3rd Qu.         Max.         NA's 
# "2015-07-20" "2016-06-15" "2016-11-17" "2016-11-08" "2017-04-12" "2017-09-25"     "117473" 

table(main1$main.ifcore)
# 0      1 
# 117473  46073
# Confirming that for each core violation a follow up date has been calculated


# # Then look at Range of dates and not just +3 or +14 dates
# 
# #Compare if there was a follow-up on these dates #Problem with this code
# main1$fl_up_check <- ifelse(main1$fl_dt_if_priority == main1$inspectiondate1 & main1$main.followup==1, 1, 0)
# table(main1$fl_up_check, useNA = 'a')
# class(main1$fl_up_check) #Numeric
# main1$fl_dt_if_priority <- as.character(main1$fl_dt_if_priority)
# main1$fl_up_check[main1$fl_dt_if_priority == "NA"] <- "NA"
# table(main1$fl_up_check, useNA = 'a')
# #       0   <NA> 
# #   147717  15829 
# main1$fl_dt_if_priority <- as.Date(main1$fl_dt_if_priority)
# 
# 
# 
# main1$dups <- duplicated(main1, fromLast = FALSE)
# 
# df <- subset(main1, main1$dups != "TRUE")
# 
# library(lubridate)
# df$inspectionyear = year(df$inspectiondate1)
# 
# library(plyr)
# library(dplyr)
# 
# df$counts<-duplicated(df$main.establishment_name, fromLast = FALSE)
# df <- df[-c(16,18)]
# 
# df$counts<-duplicated(df$main.establishment_name)




#################################################################################################################

# Risk Category - Regular Checks
main1$inspectionyear <- format(main1$inspectiondate1, "%Y")
table(main1$inspectionyear)
# 2009  2010  2011  2012  2013  2014  2015  2016  2017 
#   7   5781  12533 14471 14428 26705 33002 32805 23814 
class(main1$inspectionyear) #character

#We want to take each combination of latitude and longitude and calculate number of visits in a year for each
# combination

