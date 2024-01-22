#########################################################################################################
##################### BE CHURN RATES ####################################################################
#########################################################################################################

#################################################

# Kaplan Meier Survival estimates

#################################################
# Set the path for the log file
log_file <- "error_log_16.txt"

# Open a file connection for logging
sink(log_file, append = TRUE)

# Define the directory path with backslashes
dir_path <- "S:\\Retail\\IRL\\Supply\\Marketing\\MARKETING SUPPLY\\Analytics & Products\\Analytics\\MM WIP\\BE Value Model"

# Check if the directory exists
if (dir.exists(dir_path)) {
  # Change the working directory
  setwd(dir_path)
  print("Working directory successfully changed.")
} else {
  print("Directory does not exist.")
}

# Print the current working directory
getwd()



library(DBI)
library(ROracle)
library(dplyr)
library(dbplyr)
library(survival)
library(tidyr)

drv <- dbDriver("Oracle")
host <- "Azula008"
port <- "1526"
sid <- "DMPRD1"
connect.string <- paste(
  "(DESCRIPTION=",
  "(ADDRESS=(PROTOCOL=tcp)(HOST=", host, ")(PORT=", port, "))",
  "(CONNECT_DATA=(SID=", sid, ")))", sep = "")

conn <- dbConnect(drv, username =  dbname = connect.string, prefetch = FALSE,
                  bulk_read = 1000L, stmt_cache = 0L, external_credentials = FALSE,
                  sysdba = FALSE)

dat <- dbGetQuery(conn, "select * from Fact_BE_Value_model_churn_v2")

dbDisconnect(conn)

# diff1 = c('10000098958','10000558935','0959385','10010576351','10301597312','10305691105','10013360539',
#           '10001486428','10002015690','2146057','10012206893','10302218284','10002663666','10000868987',
#           '10003871497','1144878','10010282403','10013652712','10303593680','81352133658')
# 
# diff2 = c('81028411173','81717953805','81238975301','81918526552','81785577756','81698568257','81976693863',
#           '81178282984','81258107583','81991692305','81851218888','81698924704','81194674290','81390826681',
#           '81188037260','81605780370','10025172083','10017816191','10307393771','10307393811')
# 
# 
# dat_diff1 = dat[dat$MPRN %in% diff1, ]
# dat_diff2 = dat[dat$MPRN %in% diff2, ]

dat$TPI = ifelse(dat$ACQ_CHANNEL == 'Commercial TPI', 1, 0)

table(dat$ACQ_CHANNEL, dat$ROI, useNA = 'ifany')

dat$ACQ_CHANNEL = ifelse(dat$ACQ_CHANNEL == 'Property Button', 'Unknown', dat$ACQ_CHANNEL)
dat$ACQ_CHANNEL = ifelse(dat$ACQ_CHANNEL == 'Events', 'Unknown', dat$ACQ_CHANNEL)
dat$ACQ_CHANNEL = ifelse(dat$ACQ_CHANNEL == 'Call Centre', 'Unknown', dat$ACQ_CHANNEL)
dat$ACQ_CHANNEL = ifelse(dat$ACQ_CHANNEL == 'SME Telesales', 'Anything', dat$ACQ_CHANNEL)
dat$ACQ_CHANNEL = ifelse(dat$ACQ_CHANNEL == 'Online', 'Other', dat$ACQ_CHANNEL)
dat$ACQ_CHANNEL = ifelse(dat$ACQ_CHANNEL == 'SME', 'Anything', dat$ACQ_CHANNEL)
# dat$ACQ_CHANNEL = ifelse(dat$ACQ_CHANNEL == 'Rigney', 'Unknown', dat$ACQ_CHANNEL)

dat$ACQ_CHANNEL[dat$ROI == 1] = ifelse(dat$ACQ_CHANNEL[dat$ROI == 1] == 'Door to Door', 'Commercial TPI', dat$ACQ_CHANNEL[dat$ROI == 1])
dat$ACQ_CHANNEL[dat$ROI == 1] = ifelse(dat$ACQ_CHANNEL[dat$ROI == 1] == 'DOM', 'Commercial TPI', dat$ACQ_CHANNEL[dat$ROI == 1])
dat$ACQ_CHANNEL[dat$ROI == 1] = ifelse(dat$ACQ_CHANNEL[dat$ROI == 1] == 'Anything', 'Unknown', dat$ACQ_CHANNEL[dat$ROI == 1])

dat$ACQ_CHANNEL[dat$ROI == 0] = ifelse(dat$ACQ_CHANNEL[dat$ROI == 0] == 'Anything', 'Commercial TPI', dat$ACQ_CHANNEL[dat$ROI == 0])
dat$ACQ_CHANNEL[dat$ROI == 0] = ifelse(dat$ACQ_CHANNEL[dat$ROI == 0] == 'DOM', 'Other', dat$ACQ_CHANNEL[dat$ROI == 0])
dat$ACQ_CHANNEL[dat$ROI == 0] = ifelse(dat$ACQ_CHANNEL[dat$ROI == 0] == 'Door to Door', 'Unknown', dat$ACQ_CHANNEL[dat$ROI == 0])

table(dat$DG, dat$PROFILE, useNA = 'ifany')

table(dat$PROFILE, dat$STATUS, useNA = 'ifany')

# table(dat$EXPRT, useNA = 'ifany')
#      0      1 
# 210912    169

# table(dat$REGISTER_DESC, useNA = 'ifany')

table(dat$PROMO_CODE=='TENDER', dat$STATUS, useNA = 'ifany')
#            0      1
# FALSE  24384  41633
# <NA>   55359 254368

table(dat$DG, dat$UTILITY_TYPE_CODE, useNA = 'ifany')
#           E      G
# CBC       0   6055
# DG1   21727      0
# DG2    5406      0
# DG5  181825      0
# DG6   16556      0
# T031  29757      0
# T032   3295      0
# T033   3425      0
# T034   6199      0
# T041    225      0
# T042    187      0
# T043    173      0
# <NA>  99352   1562


dat$DG = ifelse(is.na(dat$DG) & dat$UTILITY_TYPE_CODE == 'G', 'CBC', dat$DG)
#           E      G
# CBC       0   7617
# DG1   21727      0
# DG2    5406      0
# DG5  181825      0
# DG6   16556      0
# T031  29757      0
# T032   3295      0
# T033   3425      0
# T034   6199      0
# T041    225      0
# T042    187      0
# T043    173      0
# <NA>  99352      0

table(dat$CONTRACT_TYPE, is.na(dat$PROMO_CODE), useNA = 'ifany')
#                                                     FALSE   TRUE
# Commercial Pricing Contract                         43683 106985
# Introductory Period Discount                          204  11240
# Small/Medium Enterprise Price Eligibility Contract   3016  12823
# <NA>                                                19114 178679


# dat$CREDIT_WEIGHTING [dat$CREDIT_WEIGHTING == " N/A"] = '0'
# dat <- dat[dat$CREDIT_WEIGHTING != " N/A",]
dat$CREDIT_WEIGHTING = as.integer(dat$CREDIT_WEIGHTING)
dat$CW_BUCKET <- cut(dat$CREDIT_WEIGHTING, 
                     breaks = c(-Inf,0,2,4,5,9,10,29,30,99, Inf), 
                     labels = c('0', '1-2', '3-4', '5', '6-9', '10', '11-29', '30', '31-99', '100'))

table(dat$CREDIT_WEIGHTING, dat$CW_BUCKET, useNA = 'ifany')
table(dat$CW_BUCKET)
#      0    1-2    3-4      5    6-9     10  11-29     30  31-99    100 
# 351904   3045    400   1283   1807    570   4306    821   9294   2314

# cox_model <- coxph(Surv(TENURE_MTHS, STATUS) ~ ACQ_CHANNEL, data = dat)
# cox_model
# Call:
#   coxph(formula = Surv(TENURE_MTHS, STATUS) ~ ACQ_CHANNEL, data = dat)
# 
#                           coef exp(coef)  se(coef)        z        p
# ACQ_CHANNELEmployee   0.076582  1.079591  0.005424   14.119  < 2e-16
# ACQ_CHANNELHomemoves  0.246572  1.279631  0.013488   18.281  < 2e-16
# ACQ_CHANNELOther     -0.690116  0.501518  0.005699 -121.087  < 2e-16
# ACQ_CHANNELRigney    -0.032947  0.967590  0.007005   -4.703 2.56e-06
# ACQ_CHANNELUnknown   -0.085327  0.918212  0.006399  -13.333  < 2e-16
# 
# Likelihood ratio test=19697  on 5 df, p=< 2.2e-16
# n= 375744, number of events= 296001 

# cox_model <- coxph(Surv(TENURE_MTHS, STATUS) ~ REGISTER_DESC, data = dat)
# cox_model
# 
# Call:
#   coxph(formula = Surv(TENURE_MTHS, STATUS) ~ REGISTER_DESC, data = dat)
# 
#                                          coef exp(coef) se(coef)       z        p
# REGISTER_DESC24hr                    -0.73557   0.47923  0.04731 -15.547  < 2e-16
# REGISTER_DESCDay                     -0.48655   0.61474  0.04752 -10.239  < 2e-16
# REGISTER_DESCGAS_24HR                -0.49227   0.61124  0.05107  -9.638  < 2e-16
# REGISTER_DESCHeating                 -1.19679   0.30216  0.06526 -18.338  < 2e-16
# REGISTER_DESCHeating Auto            -1.94180   0.14345  0.44969  -4.318 1.57e-05
# REGISTER_DESCLow                     -1.08040   0.33946  0.06192 -17.450  < 2e-16
# REGISTER_DESCNight                   -0.47828   0.61985  0.04751 -10.066  < 2e-16
# REGISTER_DESCNormal                  -1.07807   0.34025  0.06177 -17.453  < 2e-16
# REGISTER_DESCNSH                     -0.60037   0.54861  0.04903 -12.245  < 2e-16
# REGISTER_DESCOff Peak                -0.98602   0.37306  0.09678 -10.188  < 2e-16
# REGISTER_DESCSummer Weekday Shoulder  1.14426   3.14012  0.24037   4.760 1.93e-06
# REGISTER_DESCWinter Weekday Shoulder  1.14426   3.14012  0.24037   4.760 1.93e-06
# 
# Likelihood ratio test=2577  on 12 df, p=< 2.2e-16
# n= 176198, number of events= 124748 

dat$REGISTER_DESC <- ifelse (dat$REGISTER_DESC == 'Heating Auto', 'Heating', dat$REGISTER_DESC)

# cox_model <- coxph(Surv(TENURE_MTHS, STATUS) ~ is.na(PROMO_CODE), data = dat)
# cox_model
# 
# Call:
#   coxph(formula = Surv(TENURE_MTHS, STATUS) ~ is.na(PROMO_CODE), 
#         data = dat)
# 
#                            coef exp(coef)  se(coef)      z      p
# is.na(PROMO_CODE)TRUE -0.286278  0.751054  0.005368 -53.33 <2e-16
# 
# Likelihood ratio test=2666  on 1 df, p=< 2.2e-16
# n= 375744, number of events= 296001 


dat1 = dat

##############################################

## Channel, Year of Acq & Category Cox_PH

##############################################

# group year into threes but always ensure that a new year is always grouped with the year before
if ((as.integer(format(Sys.time(), "%y"))+2000) %% 3 == 0) {dat$YR_ACQ2 = (dat$YR_ACQ+1)%/%3*3 - 1} else {dat$YR_ACQ2 = (dat$YR_ACQ)%/%3 *3}
# table(dat$YR_ACQ, dat$YR_ACQ2, useNA = 'ifany')

# make some feature adjustments to force 2015 or 2016 ROI Commercial TPI to be first in alphabetical order for cox_prop hazard calculations
# the first alphabetically is the reference and gets a cox_ph value of 1 and we want this to be a large cohort
dat$YR_ACQ2 = ifelse (dat$YR_ACQ2 == (max(dat$YR_ACQ2)-3), dat$YR_ACQ2-1000, dat$YR_ACQ2)
dat$JURISDICTION = ifelse(dat$ROI == 1, 'ROI', 'XNI')

# concatenate to form a new concatenated feature
dat$ACQ_CHAN_YR_CAT <-  paste0(dat$ACQ_CHANNEL, dat$YR_ACQ2, dat$CATGRY)

# calculate the cox prop hazard rates
cox_model <- coxph(Surv(TENURE_MTHS, STATUS) ~ ACQ_CHAN_YR_CAT, data = dat)
cox_model


summcph <- summary(cox_model)
# str(summcph)
# summcph$conf.int

# get a table of total and active customers by the feature used for cox_ph
# tab_JUR_CHAN_YR = as.data.frame(table(dat$JUR_ACQ_CHAN_YR[dat$TPI != 1]))
# tab_JUR_CHAN_YR2 = as.data.frame(table(dat$JUR_ACQ_CHAN_YR[dat$STATUS == 0 & dat$TPI != 1]))
tab_CHAN_YR_CAT = as.data.frame(table(dat$ACQ_CHAN_YR_CAT))
tab_CHAN_YR_CAT2 = as.data.frame(table(dat$ACQ_CHAN_YR_CAT[dat$STATUS == 0]))
colnames(tab_CHAN_YR_CAT2) = c('Var1', 'ADJ_FREQ')
tab_CHAN_YR_CAT = merge (tab_CHAN_YR_CAT, tab_CHAN_YR_CAT2, all.x = T)
tab_CHAN_YR_CAT$ADJ_FREQ[is.na(tab_CHAN_YR_CAT$ADJ_FREQ)] = 0
tab_CHAN_YR_CAT = tab_CHAN_YR_CAT[,-which(colnames(tab_CHAN_YR_CAT)=='Freq')]
colnames(tab_CHAN_YR_CAT) = c('ACQ_CHAN_YR_CAT', 'ADJ_FREQ')

# if confidence intervals straddle 1 then it's not statistically different from 1 so replace it with 1
coef_CHAN_YR_CAT = ifelse((summcph$conf.int[,3] < 1 & summcph$conf.int[,4] > 1), 1, summcph$conf.int[,1])

# add 1 at the start for the first reference level
coef_CHAN_YR_CAT = cbind ( 1,t(coef_CHAN_YR_CAT))
colnames(coef_CHAN_YR_CAT) = tab_CHAN_YR_CAT$ACQ_CHAN_YR_CAT

# normalise the cox_ph values for the active customer base
norm_const = sum(tab_CHAN_YR_CAT$ADJ_FREQ*coef_CHAN_YR_CAT[1,])/sum(tab_CHAN_YR_CAT$ADJ_FREQ)
coef_CHAN_YR_CAT = coef_CHAN_YR_CAT/norm_const

# create a datafram for the normalised values and merge with the raw dataframe
coef_CHAN_YR_CAT2 = cbind.data.frame('ACQ_CHAN_YR_CAT' = colnames(coef_CHAN_YR_CAT), 'COX_CHAN_YR_CAT' = as.numeric(t(coef_CHAN_YR_CAT)[,1]))
dat = merge(dat, coef_CHAN_YR_CAT2, by = 'ACQ_CHAN_YR_CAT', all.x = T, all.y = F)

# merge with the dataframe that counts active customers so that we can check to see that the 
# normalised cox_ph values have no net effect on active customer churn
tab_CHAN_YR_CAT = merge(tab_CHAN_YR_CAT, coef_CHAN_YR_CAT2, all.x = T)

# test to see if the cox_ph has no net effect on current active customers
ph_test = sum(tab_CHAN_YR_CAT$ADJ_FREQ * tab_CHAN_YR_CAT$COX_CHAN_YR_CAT)/sum(tab_CHAN_YR_CAT$ADJ_FREQ)
ph_test
# [1] 1


##############################################

## County & Industry Cox_PH

##############################################

# concatenate to form a new concatenated feature
dat$COUNTY_IND <-  paste0(dat$COUNTY, dat$INDUSTRY)

# calculate the cox prop hazard rates
# indx = dat$JURISDICTION == 'ROI'
cox_model <- coxph(Surv(TENURE_MTHS, STATUS) ~ COUNTY_IND, data = dat)
cox_model

summcph <- summary(cox_model)
# str(summcph)
# summcph$conf.int

# get a table of total and active customers by the feature used for cox_ph
tab_COUNTY_IND = as.data.frame(table(dat$COUNTY_IND))
tab_COUNTY_IND2 = as.data.frame(table(dat$COUNTY_IND[dat$STATUS == 0]))
colnames(tab_COUNTY_IND2) = c('Var1', 'ADJ_FREQ')
tab_COUNTY_IND = merge (tab_COUNTY_IND, tab_COUNTY_IND2, all.x = T)
tab_COUNTY_IND$ADJ_FREQ[is.na(tab_COUNTY_IND$ADJ_FREQ)] = 0
tab_COUNTY_IND = tab_COUNTY_IND[,-which(colnames(tab_COUNTY_IND)=='Freq')]
colnames(tab_COUNTY_IND) = c('COUNTY_IND', 'ADJ_FREQ')

# if confidence intervals straddle 1 then it's not statistically different from 1 so replace it with 1
coef_COUNTY_IND = ifelse((summcph$conf.int[,3] < 1 & summcph$conf.int[,4] > 1), 1, summcph$conf.int[,1])

# add 1 at the start for the first reference level
coef_COUNTY_IND = cbind ( 1,t(coef_COUNTY_IND))
colnames(coef_COUNTY_IND) = tab_COUNTY_IND$COUNTY_IND

# indx_NI <- names(tab_COUNTY_IND$COUNTY_IND) %in% c('ANTRIM', 'ARMAGH', 'DERRY', 'DOWN', 'FERMANAGH', 'TYRONE')
indx_NI <- grepl("ANTRIM|ARMAGH|DERRY|DOWN|FERMANAGH|TYRONE", tab_COUNTY_IND$COUNTY_IND)
indx_ROI <- !(indx_NI)

# normalise the cox_ph values for the active customer base BY JURISDICTION AS JURISDICTION IS ALREADY INCLUDED AS A CHURN CURVE FEATURE
# Changes made by Ashwath Salimath (20th September 2021) as suggested by Michael
norm_const_NI = sum(tab_COUNTY_IND$ADJ_FREQ[indx_NI]*coef_COUNTY_IND[1,indx_NI])/sum(tab_COUNTY_IND$ADJ_FREQ[indx_NI])
norm_const_ROI = sum(tab_COUNTY_IND$ADJ_FREQ[indx_ROI]*coef_COUNTY_IND[1,indx_ROI])/sum(tab_COUNTY_IND$ADJ_FREQ[indx_ROI])
coef_COUNTY_IND[indx_NI] = coef_COUNTY_IND[indx_NI]/norm_const_NI
coef_COUNTY_IND[indx_ROI] = coef_COUNTY_IND[indx_ROI]/norm_const_ROI

# norm_const = sum(tab_COUNTY_IND$ADJ_FREQ*coef_COUNTY_IND[1,])/sum(tab_COUNTY_IND$ADJ_FREQ)
# coef_COUNTY_IND = coef_COUNTY_IND/norm_const

# create a datafram for the normalised values and merge with the raw dataframe
coef_COUNTY_IND2 = cbind.data.frame('COUNTY_IND' = colnames(coef_COUNTY_IND), 'COX_COUNTY_IND' = as.numeric(t(coef_COUNTY_IND)[,1]))
dat = merge(dat, coef_COUNTY_IND2, by = 'COUNTY_IND', all.x = T, all.y = F)
dat$COX_COUNTY_IND[is.na(dat$COX_COUNTY_IND)] = 1

# merge with the dataframe that counts active customers so that we can check to see that the 
# normalised cox_ph values have no net effect on active customer churn
tab_COUNTY_IND = merge(tab_COUNTY_IND, coef_COUNTY_IND2, all.x = T)

sum(coef_COUNTY_IND[indx_NI] * tab_COUNTY_IND$ADJ_FREQ[indx_NI])/sum(tab_COUNTY_IND$ADJ_FREQ[indx_NI])
# [1] 1
sum(coef_COUNTY_IND[indx_ROI] * tab_COUNTY_IND$ADJ_FREQ[indx_ROI])/sum(tab_COUNTY_IND$ADJ_FREQ[indx_ROI])
# [1] 1

# test to see if the cox_ph has no net effect on current active customers
ph_test = sum(tab_COUNTY_IND$ADJ_FREQ * tab_COUNTY_IND$COX_COUNTY_IND)/sum(tab_COUNTY_IND$ADJ_FREQ)
ph_test 
# [1] 1

# test to see if the cox_ph has no net effect on current active customers in NI
ph_test_NI = sum(tab_COUNTY_IND$ADJ_FREQ[indx_NI] * tab_COUNTY_IND$COX_COUNTY_IND[indx_NI])/sum(tab_COUNTY_IND$ADJ_FREQ[indx_NI])
ph_test_NI
# [1] 1

# test to see if the cox_ph has no net effect on current active customers in ROI
ph_test_ROI = sum(tab_COUNTY_IND$ADJ_FREQ[indx_ROI] * tab_COUNTY_IND$COX_COUNTY_IND[indx_ROI])/sum(tab_COUNTY_IND$ADJ_FREQ[indx_ROI])
ph_test_ROI



##############################################

## Contract Type & Jurisdiction Cox_PH

##############################################

table (dat$JURISDICTION, dat$CONTRACT_TYPE, useNA = 'ifany')

# concatenate to form a new concatenated feature
dat$JUR_CON <-  paste0(dat$JURISDICTION, dat$CONTRACT_TYPE)

# calculate the cox prop hazard rates
indx = !is.na (dat$CONTRACT_TYPE)
cox_model <- coxph(Surv(TENURE_MTHS, STATUS) ~ JUR_CON, data = dat[indx,])
cox_model

summcph <- summary(cox_model)
# str(summcph)
# summcph$conf.int

# get a table of total and active customers by the feature used for cox_ph
tab_JUR_CON = as.data.frame(table(dat$JUR_CON[indx]))
tab_JUR_CON2 = as.data.frame(table(dat$JUR_CON[dat$STATUS == 0 & indx]))
colnames(tab_JUR_CON2) = c('Var1', 'ADJ_FREQ')
tab_JUR_CON = merge (tab_JUR_CON, tab_JUR_CON2, all.x = T)
tab_JUR_CON$ADJ_FREQ[is.na(tab_JUR_CON$ADJ_FREQ)] = 0
tab_JUR_CON = tab_JUR_CON[,-which(colnames(tab_JUR_CON)=='Freq')]
colnames(tab_JUR_CON) = c('JUR_CON', 'ADJ_FREQ')

# if confidence intervals straddle 1 then it's not statistically different from 1 so replace it with 1
coef_JUR_CON = ifelse((summcph$conf.int[,3] < 1 & summcph$conf.int[,4] > 1), 1, summcph$conf.int[,1])

# add 1 at the start for the first reference level
coef_JUR_CON = cbind ( 1,t(coef_JUR_CON))
colnames(coef_JUR_CON) = tab_JUR_CON$JUR_CON

# normalise the cox_ph values for the active customer base
# norm_const = sum(tab_JUR_CON$ADJ_FREQ*coef_JUR_CON[1,])/sum(tab_JUR_CON$ADJ_FREQ)
# coef_JUR_CON = coef_JUR_CON/norm_const

indx_NI <- grepl("XNI", tab_JUR_CON$JUR_CON)
indx_ROI <- !(indx_NI)

# normalise the cox_ph values for the active customer base BY JURISDICTION AS JURISDICTION IS ALREADY INCLUDED AS A CHURN CURVE FEATURE
# Changes made by Ashwath Salimath (20th September 2021) as suggested by Michael
norm_const_NI = sum(tab_JUR_CON$ADJ_FREQ[indx_NI]*coef_JUR_CON[1,indx_NI])/sum(tab_JUR_CON$ADJ_FREQ[indx_NI])
norm_const_ROI = sum(tab_JUR_CON$ADJ_FREQ[indx_ROI]*coef_JUR_CON[1,indx_ROI])/sum(tab_JUR_CON$ADJ_FREQ[indx_ROI])
coef_JUR_CON[indx_NI] = coef_JUR_CON[indx_NI]/norm_const_NI
coef_JUR_CON[indx_ROI] = coef_JUR_CON[indx_ROI]/norm_const_ROI

# create a datafram for the normalised values and merge with the raw dataframe
coef_JUR_CON2 = cbind.data.frame('JUR_CON' = colnames(coef_JUR_CON), 'COX_JUR_CON' = as.numeric(t(coef_JUR_CON)[,1]))
dat = merge(dat, coef_JUR_CON2, by = 'JUR_CON', all.x = T, all.y = F)
dat$COX_JUR_CON[is.na(dat$COX_JUR_CON)] = 1

# merge with the dataframe that counts active customers so that we can check to see that the
# normalised cox_ph values have no net effect on active customer churn
tab_JUR_CON = merge(tab_JUR_CON, coef_JUR_CON2, all.x = T)

# test to see if the cox_ph has no net effect on current active customers
ph_test = sum(tab_JUR_CON$ADJ_FREQ * tab_JUR_CON$COX_JUR_CON)/sum(tab_JUR_CON$ADJ_FREQ)
ph_test # [1] 1

# test to see if the cox_ph has no net effect on current active customers in NI
ph_test_NI = sum(tab_JUR_CON$ADJ_FREQ[indx_NI] * tab_JUR_CON$COX_JUR_CON[indx_NI])/sum(tab_JUR_CON$ADJ_FREQ[indx_NI])
ph_test_NI
# [1] 1

# test to see if the cox_ph has no net effect on current active customers in ROI
ph_test_ROI = sum(tab_JUR_CON$ADJ_FREQ[indx_ROI] * tab_JUR_CON$COX_JUR_CON[indx_ROI])/sum(tab_JUR_CON$ADJ_FREQ[indx_ROI])
ph_test_ROI



##############################################

## County & Jurisdiction Cox_PH

##############################################

# cox_model <- coxph(Surv(TENURE_MTHS, STATUS) ~ COUNTY, data = dat)
# cox_model
# 
# summcph <- summary(cox_model)
# # str(summcph)
# # summcph$conf.int
# 
# # get a table of total and active customers by the feature used for cox_ph
# tab_COUNTY = as.data.frame(table(dat$COUNTY))
# tab_COUNTY2 = as.data.frame(table(dat$COUNTY[dat$STATUS == 0]))
# colnames(tab_COUNTY2) = c('Var1', 'ADJ_FREQ')
# tab_COUNTY = merge (tab_COUNTY, tab_COUNTY2, all.x = T)
# tab_COUNTY$ADJ_FREQ[is.na(tab_COUNTY$ADJ_FREQ)] = 0
# tab_COUNTY = tab_COUNTY[,-which(colnames(tab_COUNTY)=='Freq')]
# colnames(tab_COUNTY) = c('COUNTY', 'ADJ_FREQ')
# 
# # if confidence intervals straddle 1 then it's not statistically different from 1 so replace it with 1
# coef_COUNTY = ifelse((summcph$conf.int[,3] < 1 & summcph$conf.int[,4] > 1), 1, summcph$conf.int[,1])
# 
# # add 1 at the start for the first reference level
# coef_COUNTY = cbind ( 1,t(coef_COUNTY))
# colnames(coef_COUNTY) = tab_COUNTY$COUNTY
# 
# # normalise the cox_ph values for the active customer base
# norm_const = sum(tab_COUNTY$ADJ_FREQ*coef_COUNTY[1,])/sum(tab_COUNTY$ADJ_FREQ)
# coef_COUNTY = coef_COUNTY/norm_const
# 
# # create a datafram for the normalised values and merge with the raw dataframe
# coef_COUNTY2 = cbind.data.frame('COUNTY' = colnames(coef_COUNTY), 'COX_COUNTY' = as.numeric(t(coef_COUNTY)[,1]))
# dat = merge(dat, coef_COUNTY2, by = 'COUNTY', all.x = T, all.y = F)
# dat$COX_COUNTY[is.na(dat$COX_COUNTY)] = 1
# 
# # merge with the dataframe that counts active customers so that we can check to see that the 
# # normalised cox_ph values have no net effect on active customer churn
# tab_COUNTY = merge(tab_COUNTY, coef_COUNTY2, all.x = T)
# 
# # test to see if the cox_ph has no net effect on current active customers
# ph_test = sum(tab_COUNTY$ADJ_FREQ * tab_COUNTY$COX_COUNTY)/sum(tab_COUNTY$ADJ_FREQ)
# ph_test # [1] 1



# create new features for survival plot analysis
dat$PROMO_FLAG = ifelse(is.na(dat$PROMO_CODE),0, 1)
dat$CONTRACT_FLAG = ifelse(is.na(dat$CONTRACT_END_DATE),0, 1)


###################################

# Functions to plot survival curves

####################################


dat_fun = function(dat, x, acq_limit) {
  attach(dat)
  leg = levels(factor(FEATURE))
  l = length(leg)
  surv_dat = survfit(Surv(time = DATEDIFF2, event = STATUS) ~ as.factor(FEATURE))
  plot(surv_dat , 
       col = c(1:l), 
       main = paste0("BE Survival Plot ",x, " For Acquisitions Since ",acq_limit), 
       xlab = "Time(Months)",
       xlim = c(0,36),
       ylim = c(0.037,0.963),
       ylab = "Retention",
       xaxt = "n",
       yaxt = "n")
  grid(nx=6, ny=10)
  legend("bottomleft", 
         legend = leg, 
         lty = c(rep(1,l)), 
         cex = 0.8, 
         col = c(1:l))
  axis(side = 1, at = seq(0, 36, by = 3), seq(0, 36, by = 3))
  axis(side = 2, at = seq(0, 1, by = .10), seq(0, 1, by = .10), las = 1)
  detach(dat)
}

dat_fun2 = function(dat, x, Jur, acq_limit) {
  attach(dat)
  leg = levels(factor(FEATURE))
  l = length(leg)
  surv_dat = survfit(Surv(time = DATEDIFF2, event = STATUS) ~ as.factor(FEATURE))
  plot(surv_dat , 
       col = c(1:l), 
       main = paste0("Survival Plot ",x, " For ",ifelse(Jur==1, 'ROI', 'NI')," Acquisitions Since ",acq_limit), 
       xlab = "Time(Months)",
       xlim = c(0,36),
       ylim = c(0.037,0.963),
       ylab = "Retention",
       xaxt = "n",
       yaxt = "n")
  grid(nx=6, ny=10)
  legend("bottomleft", 
         legend = leg, 
         lty = c(rep(1,l)), 
         cex = 0.8, 
         col = c(1:l))
  axis(side = 1, at = seq(0, 36, by = 3), seq(0, 36, by = 3))
  axis(side = 2, at = seq(0, 1, by = .10), seq(0, 1, by = .10), las = 1)
  detach(dat)
}

surv_fun <- function(x, acq_limit) {
  dat3 = dat[dat$YR_ACQ >= acq_limit, c(which(names(dat) == 'DATEDIFF2'),
                                        which(names(dat) == 'STATUS'),
                                        which(names(dat) == x))]
  colnames(dat3) = c(names(dat3[,c(1,2)]), 'FEATURE')
  dat_fun(dat3, x, acq_limit)
}

surv_fun2 <- function(x, Jur, acq_limit) {
  dat3 = dat[(dat$YR_ACQ >= acq_limit & dat$ROI == Jur), c(which(names(dat) == 'DATEDIFF2'),
                                                           which(names(dat) == 'STATUS'),
                                                           which(names(dat) == x))]
  colnames(dat3) = c(names(dat3[,c(1,2)]), 'FEATURE')
  dat_fun2(dat3, x, Jur, acq_limit)
}


# plot survival curves
# surv_fun allows feature and year of acquisition to be selected
# surv_fun('SME', 2010)
# surv_fun('CONTRACT_FLAG', 2010)
# surv_fun('ROI', 2010)
# surv_fun('DD_PAY', 2010)
# surv_fun('EBILL', 2010)
# surv_fun('MARKETING_OPT_IN', 2010)
# surv_fun('UTILITY_TYPE_CODE', 2010)
# surv_fun('CONTRACT_TYPE', 2010)
# surv_fun('M24HR', 2010)
# surv_fun('DF_FLAG', 2010)
# surv_fun('COUNTY', 2010)
# surv_fun('CREDIT_WEIGHTING', 2010)
# surv_fun('PROMO_FLAG', 2010)
# surv_fun('REGISTER_DESC', 2010)
# surv_fun('YR_ACQ', 2005)
# surv_fun('ACQ_CHANNEL', 2005)
#
# surv_fun2 allows feature, jurisdiction (1: ROI, 0:NI) and year of acquisition to be selected
# surv_fun2('SME', 1, 2010)
# surv_fun2('CONTRACT_FLAG', 1, 2010)
# surv_fun2('ROI',  1, 2010)
# surv_fun2('DD_PAY', 1, 2010)
# surv_fun2('EBILL', 1, 2010)
# surv_fun2('MARKETING_OPT_IN', 1, 2010)
# surv_fun2('UTILITY_TYPE_CODE', 1, 2010)
# surv_fun2('CONTRACT_TYPE', 1, 2010)
# surv_fun2('M24HR', 1, 2010)
# surv_fun2('DF_FLAG', 1, 2010)
# surv_fun2('COUNTY', 1, 2010)
# surv_fun2('CREDIT_WEIGHTING', 1, 2010)
# surv_fun2('PROMO_FLAG', 1, 2010)
# surv_fun2('REGISTER_DESC', 1, 2010)
# surv_fun2('YR_ACQ', 1, 2005)
surv_fun2('ACQ_CHANNEL', 1, 2005)
surv_fun2('ACQ_CHANNEL', 0, 2005)
#
# surv_fun2('SME', 0, 2010)
# surv_fun2('CONTRACT_FLAG', 0, 2010)
# surv_fun2('ROI', 0, 2010)
# surv_fun2('DD_PAY', 0, 2010)
# surv_fun2('EBILL', 0, 2010)
# surv_fun2('MARKETING_OPT_IN', 0, 2010)
# surv_fun2('UTILITY_TYPE_CODE', 0, 2010)
# surv_fun2('CONTRACT_TYPE', 0, 2010)
# surv_fun2('M24HR', 0, 2010)
# surv_fun2('DF_FLAG', 0, 2010)
# surv_fun2('COUNTY', 0, 2010)
# surv_fun2('CREDIT_WEIGHTING', 0, 2010)
# surv_fun2('PROMO_FLAG', 0, 2010)
# surv_fun2('REGISTER_DESC', 0, 2010)
# surv_fun2('YR_ACQ', 0, 2005)

# remove any negative DATEDIFFs
# datt = dat[-dat$DATEDIFF<=-1,]

dat$MARKETING_OPT_IN[is.na(dat$MARKETING_OPT_IN)] = 0

# convert to factors
dat$ROI = as.factor(dat$ROI)
dat$ELEC = as.factor(dat$ELEC)
# dat$CREDIT = as.factor(dat$CREDIT)
dat$EBILL = as.factor(dat$EBILL)
dat$DD_PAY = as.factor(dat$DD_PAY)
dat$CW_BUCKET = as.factor(dat$CW_BUCKET)
dat$DF_FLAG = as.factor(dat$DF_FLAG)
# dat$EXPRT = as.factor(dat$EXPRT)
dat$M24HR = as.factor(dat$M24HR)
# dat$ICS = as.factor(dat$ICS)
#dat$CREDIT_WEIGHTING = as.integer(dat$CREDIT_WEIGHTING)
dat$ACQ_CHANNEL = as.factor(dat$ACQ_CHANNEL)
dat$ACQ_YR = as.integer(floor(1E9/dat$ACQ_YR)) # this is bucketted into one of 2006, 2008, 2010, 2012 or 2014. 2009 will get value 2008.
dat$MARKETING_OPT_IN = as.factor(dat$MARKETING_OPT_IN)
# dat$CITY = as.factor(dat$CITY)
dat$PROMO_CODE = as.factor(dat$PROMO_CODE)
dat$TPI= as.factor(dat$TPI)
# dat$IN_CONTRACT = as.factor(dat$IN_CONTRACT)
dat$COUNTY = as.factor(dat$COUNTY)

# these will be used as identifiers when we save the churn data back to Oracle
dat$CUSTOMER_ID = as.integer(dat$CUSTOMER_ID)
dat$PREMISE_ID = as.integer(dat$PREMISE_ID)
dat$TENURE_MTHS = as.integer(dat$DATEDIFF)
dat$MPRN = as.character(dat$MPRN)
dat$UTILITY_TYPE_CODE = as.character(dat$UTILITY_TYPE_CODE)
dat$REGISTER_DESC = as.character(dat$REGISTER_DESC)


indx = c(which(names(dat)=='ROI'),
         which(names(dat)=='SME'),
         which(names(dat)=='TPI'),
         which(names(dat)=='DD_PAY'),
         which(names(dat)=='MARKETING_OPT_IN'),
         which(names(dat)=='EBILL'),
         which(names(dat)=='PROMO_FLAG'),
         which(names(dat)=='ELEC'))

dat$id1 <- dat[,indx] %>% unite(id, colnames(dat[,indx]), sep="")
tab_id = table(dat$id1)
tab_id

# 0000001 0000011 0000101 0000111 0001001 0001011 0001101 0001111 0010001 0010011 0010101 
#    1701      92     374      36      39       1      34       1    3230     282    1525 
# 0010111 0011001 0011011 0011101 0011111 0100001 0100011 0100101 0100111 0101001 0101011 
#     226     219      18     280      40   12106     773    2130     173     598      42 
# 0101101 0101111 0110001 0110011 0110101 0110111 0111001 0111011 0111101 0111111 1000000 
#     369      19   14338    1321    5539     788    3382     208    1666     283     185 
# 1000001 1000010 1000011 1000100 1000101 1000110 1000111 1001000 1001001 1001010 1001011 
#    8467       2     523     556    4803       6     230      12    1044       7     279 
# 1001100 1001101 1001111 1010000 1010001 1010010 1010011 1010100 1010101 1010110 1010111 
#      19     898      24     257   18654      90    2745     802   13252      42     821 
# 1011000 1011001 1011010 1011011 1011100 1011101 1011110 1011111 1100000 1100001 1100010 
#     101    5895     210    5292     332    4241      17    1097     554   40884     119 
# 1100011 1100100 1100101 1100110 1100111 1101000 1101001 1101010 1101011 1101100 1101101 
#    5506     182    6142      15     761      67    3129      26    1124      73     988 
# 1101110 1101111 1110000 1110001 1110010 1110011 1110100 1110101 1110110 1110111 1111000 
#       1     245    1208   66173     593   14631     700   19372     249    4293     319 
# 1111001 1111010 1111011 1111100 1111101 1111110 1111111 
#   14719     293   11591     327    5562     103    2196


# bit	value	desc
# 1	0	NI
# 2	0	EPRISE
# 3	0	NON-TPI
# 4	0	NON-DD
# 5	0	NON-MOI
# 6	0	NON-EBILL
# 7	0	NON-PROMO
# 8	0	GAS
# 1	1	ROI
# 2	1	SME
# 3	1	TPI
# 4	1	DD
# 5	1	MOI
# 6	1	EBILL
# 7	1	PROMO
# 8	1	ELEC

bit_code <- as.data.frame(matrix(c('NI, ', 'EPRISE, ', 'NON-TPI, ', 'NON-DD, ', 'NON-MOI, ', 'NON-EBILL, ', 'NON-PROMO, ', 'GAS',
                                   'ROI, ', 'SME, ', 'TPI, ', 'DD, ', 'MOI, ', 'EBILL, ', 'PROMO, ', 'ELEC'), nrow = 8, ncol = 2))
colnames(bit_code) = c('0', '1')

bit_code
#           0       1
# 1        NI,    ROI, 
# 2    EPRISE,    SME, 
# 3   NON-TPI,    TPI, 
# 4    NON-DD,     DD, 
# 5   NON-MOI,    MOI, 
# 6 NON-EBILL,  EBILL, 
# 7 NON-PROMO,  PROMO, 
# 8        GAS    ELEC


# convert binary string into decimal
BinToDec <- function(x) {sum(2^(which(rev(unlist(strsplit(as.character(x), "")) == 1))-1))}

# convert decimal into binary string
DecToBin <- function (x,y) {paste(rev(sapply(strsplit(paste(intToBits(x)),""),`[[`,2)[1:y]),collapse="")}

# remove the splits that have fewer than 200 customers and combine them into one group
sm_cohorts = names(tab_id [tab_id < 200])
l = length(sm_cohorts)
b = nchar(names(tab_id[1]))-1
n = 0

repeat{
  m = 0
  for (i in 1:l) {
    for (j in 0:b) {
      # this flips each bit sequentially starting at the least significant bit
      indx = DecToBin(bitwXor(BinToDec(sm_cohorts[i]),2^j+n),b+1)
      if (is.element(indx, names(tab_id))) {
        if( tab_id[which(names(tab_id) == indx)] >= 200) {
          # if the new cohort exists then rename the current group with 
          # the name of the new cohort
          dat$id1[dat$id1 == sm_cohorts[i]] <- names(tab_id[indx])
          m = m + 1}
      }
    }
  }
  
  if (m == 0) {n = n + 1}
  tab_id = table(dat$id1)
  tab_id
  sm_cohorts = names(tab_id [tab_id < 200])
  l = length(sm_cohorts)
  if(l == 0 | n > 254) {break}
}


tab_id = table(dat$id1)
tab_id

# 00000001 00000101 00010001 00010101 00100001 00100101 00110001 00110011 00110101 00110111 01000001 01000011 
#     1389      353     1903     1122      335      405     1328      324     1046      267     9098      491 
# 01000101 01001001 01001101 01010001 01010011 01010101 01010111 01011001 01011101 01100001 01100011 01100101 
#     1654      219      220     8100      482     3348      246      970      804     3070      401      879 
# 01101001 01110001 01110011 01110101 01110111 01111001 01111011 01111101 01111111 10000001 10000100 10000101 
#      623     4310     1375     2057      882     2979      217     1232      295     6168      317     2641 
# 10001101 10010001 10010011 10010100 10010101 10010111 10011001 10011011 10011101 10100001 10100011 10100100 
#      535     9374      746      300     8059      295      727      219     1849     4759      432      242 
# 10100101 10101001 10101101 10110001 10110011 10110100 10110101 10110111 10111001 10111011 10111101 10111111 
#     3613     1359      485    12816     5372      672     8005     1462     6140     3351     2977      575 
# 11000000 11000001 11000011 11000101 11000111 11001001 11001101 11010000 11010001 11010011 11010100 11010101 
#      369    33677     2644     5248      441     1384      770      656    42964     5309      449    13277 
# 11010111 11011001 11011011 11011101 11011111 11100000 11100001 11100011 11100101 11100111 11101001 11101011 
#     2633     3908     1058     2795      918      264    15320     3708     2395      516     2382      555 
# 11101101 11110000 11110001 11110010 11110011 11110100 11110101 11110111 11111000 11111001 11111011 11111100 
#      718      753    37504      426    16448      354     9482     3470      243    14097     7581      211 
# 11111101 11111111 
#     3772     1131


# these steps are slow - room to improve via RCPP!
dat$split1 = apply(dat$id1,1,BinToDec) + 1

tab1 = table(dat$split1)
tab1

#    2     6    18    22    34    38    50    52    54    56    66    68    70    74    78    82    84    86 
# 1389   353  1903  1122   335   405  1328   324  1046   267  9098   491  1654   219   220  8100   482  3348 
#   88    90    94    98   100   102   106   114   116   118   120   122   124   126   128   130   133   134 
#  246   970   804  3070   401   879   623  4310  1375  2057   882  2979   217  1232   295  6168   317  2641 
#  142   146   148   149   150   152   154   156   158   162   164   165   166   170   174   178   180   181 
#  535  9374   746   300  8059   295   727   219  1849  4759   432   242  3613  1359   485 12816  5372   672 
#  182   184   186   188   190   192   193   194   196   198   200   202   206   209   210   212   213   214 
# 8005  1462  6140  3351  2977   575   369 33677  2644  5248   441  1384   770   656 42964  5309   449 13277 
#  216   218   220   222   224   225   226   228   230   232   234   236   238   241   242   243   244   245 
# 2633  3908  1058  2795   918   264 15320  3708  2395   516  2382   555   718   753 37504   426 16448   354 
#  246   248   249   250   252   253   254   256 
# 9482  3470   243 14097  7581   211  3772  1131 


split_list1 = names(tab1)
split_list1
# [1] "2"   "6"   "18"  "22"  "34"  "38"  "50"  "52"  "54"  "56"  "66"  "68"  "70"  "74"  "78"  "82"  "84" 
# [18] "86"  "88"  "90"  "94"  "98"  "100" "102" "106" "114" "116" "118" "120" "122" "124" "126" "128" "130"
# [35] "133" "134" "142" "146" "148" "149" "150" "152" "154" "156" "158" "162" "164" "165" "166" "170" "174"
# [52] "178" "180" "181" "182" "184" "186" "188" "190" "192" "193" "194" "196" "198" "200" "202" "206" "209"
# [69] "210" "212" "213" "214" "216" "218" "220" "222" "224" "225" "226" "228" "230" "232" "234" "236" "238"
# [86] "241" "242" "243" "244" "245" "246" "248" "249" "250" "252" "253" "254" "256"


# mean(dat$COX_COUNTY) *
# mean(dat$COX_JUR_CHAN_YR) *
# mean(dat$COX_JUR_CW) *
# mean(dat$COX_JUR_REG_TYPE) *
# mean(dat$COX_JUR_DF) *
# mean(dat$COX_JUR_IND) *
# mean(dat$COX_JUR_CON)
# # [1] 1.132484
# 
# indx = dat$STATUS == 0
# mean(dat$COX_COUNTY[indx]) *
#   mean(dat$COX_JUR_CHAN_YR[indx]) *
#   mean(dat$COX_JUR_CW[indx]) *
#   mean(dat$COX_JUR_REG_TYPE[indx]) *
#   mean(dat$COX_JUR_DF[indx]) *
#   mean(dat$COX_JUR_IND[indx]) *
#   mean(dat$COX_JUR_CON[indx])
# # [1] 1


mean(dat$COX_CHAN_YR_CAT) *
  mean(dat$COX_COUNTY_IND) *
  mean(dat$COX_JUR_CON)
# [1] 1.466381

indx = dat$STATUS == 0
mean(dat$COX_CHAN_YR_CAT[indx]) *
  mean(dat$COX_COUNTY_IND[indx]) *
  mean(dat$COX_JUR_CON[indx])
# [1] 1

# create a list to hold the survival curves for the different segments
split_dat1 = list(1)
for (i in 1:length(tab1)) { # tab1 is a table of the different survival curve cohorts
  split_dat1[[i]] = survfit(Surv(TENURE_MTHS, STATUS) ~ split1, data = dat[dat$split1 == as.numeric(split_list1[i]),])
}

# create a list to hold the churn rates for the different segments
split_churn1 = list(1)
for (i in 1:length(tab1)) {
  l = length(split_dat1[[i]]$surv)
  split_dat1[[i]]$surv = c(1,split_dat1[[i]]$surv)
  split_churn1[[i]] = -diff(split_dat1[[i]]$surv, lag = 1)/split_dat1[[i]]$surv[1:l]
  # the churn rate for month x is the (retention for month x - retention for month x-1 )/ retention for month x-1
}


# plot all the survival curves for each separate cohort
par(mfrow=c(1,1))
plot(split_dat1[[1]], 
     col = c(1), 
     main = paste0('retention Curve ', DecToBin(as.integer(names(tab1[1])),8)), 
     xlab = "Time(Yrs)",
     xlim = c(0,120),
     ylab = "Retention Rate (%)", 
     xaxt = "n")
# add the other lines
for (i in 2:length(split_dat1)) {
  lines(split_dat1[[i]], col = i)
}
# re-label the axis in years rather than months
axis(side = 1, at = seq(0, 120, by = 12), (seq(1:11)-1))



# plot all the churn curves for each separate cohort
par(mfrow=c(1,1))
plot(split_churn1[[1]], 
     type = "l",
     col = c(1), 
     main = "Churn Curve", 
     xlab = "Time(Yrs)",
     xlim = c(0,120),
     ylim = c(0,0.2),
     ylab = "Churn Rate (%)", 
     xaxt = "n")
# add the other lines
for (i in 2:length(split_churn1)) {
  lines(split_churn1[[i]], col = i)
}
# re-label the axis in years rather than months
axis(side = 1, at = seq(0, 120, by = 12), (seq(1:11)-1))


# get the current active meters
dat_current = dat[dat$STATUS == 0, ]
# remove any rows where tenure is null
dat_current = dat_current[!is.na(dat_current$TENURE_MTHS),]


# get a count of the current active meters
num = dim(dat_current)[1]

# add new columns to the split dataframe to identify the index number for the survival list
# and the maximum tenure where we can read the median life directly from the survival curve
split11 = as.numeric(names(tab1))
split12 = cbind.data.frame('split1_indx' = seq(1:length(tab1)), 'split1' = split11)
num2 = dim(split12)[1]
split12$max1 = rep(0, num2)
for (i in 1:num2) {
  # split_churn [[i]] = split_churn[[i]][is.finite(1/split_churn[[i]])] # remove zeros
  split12$max1 [i] = length(split_churn1[[i]])
}

# add new columns to the current active meters to allows us access the correct survival curve
# dat_current = merge(dat_current, split12, by = 'split1')

# calculate the average monthly churn rate given the current tenure point 
# (where we can read this from the survival curve and for the last year where we can't)

# table(dat_current$max1, useNA = 'ifany')

#     37     40     50     51     53     54     55     59     69     71     72 
#     45     24    107    112     33     19     17      9     67   1237   1269 
#     73     77     79     88     99    101    104    105    106    107    115 
#    752   2617    802   1441    395   3430   9025 159653  26580  48529   1825 
#    120    122    124    126    130    131    149    152    161    162    184 
#   3031  13589   1774  17539   9330   2421   3869  33266  15860   4286   6971 
#    191    209    220 
# 111311  38699  12229

# head(dat_current,50)


l1 = dim(split12)[1]
churn_tab1 = matrix(0,nrow = l1, ncol = 8)

for (i in 1:l1) {
  l2 = split12$max1[i]
  churn_tab1[i,1] = split12$split1[i]
  churn_tab1[i,2] = round(mean(split_churn1[[i]] [1:min(6,l2)]),4)
  churn_tab1[i,3] = round(mean(split_churn1[[i]] [min(7,l2):min(11,l2)]),4)
  churn_tab1[i,4] = round(mean(split_churn1[[i]] [min(13,l2):min(18,l2)]),4)
  churn_tab1[i,5] = round(mean(split_churn1[[i]] [min(19,l2):min(23,l2)]),4)
  churn_tab1[i,6] = round(mean(split_churn1[[i]] [min(25,l2):l2]),4)   
  churn_tab1[i,7] = round(split_churn1[[i]] [min(12,l2)],4)
  churn_tab1[i,8] = round(split_churn1[[i]] [min(24,l2)],4)
}

colnames(churn_tab1) = c('split', 'ChurnY1H1', 'ChurnY1H2', 'ChurnY2H1', 'ChurnY2H2', 'ChurnY3P', 'Churn12M', 'Churn24M')

churn_tab1 = as.data.frame(churn_tab1)

dat_current$split = dat_current$split1

dat_current = merge(dat_current, churn_tab1, all = TRUE,  by = 'split')

mean(dat_current$ChurnY1H1)
# [1] 0.01091687
mean(dat_current$ChurnY1H2)
# [1] 0.01252062
mean(dat_current$Churn12M)
# [1] 0.03788095
mean(dat_current$ChurnY2H1)
# [1] 0.02658947
mean(dat_current$ChurnY2H2)
# [1] 0.01592978
mean(dat_current$Churn24M)
# [1] 0.02728756
mean(dat_current$ChurnY3P)
# [1] 0.01667725


# histogram
hist(dat_current$ChurnY1H1, breaks = 500, 
     main = 'Histogram of Future Churn Rate for Active Meters',
     xlim = c(0,0.05),
     col = 3,
     xlab = 'Monthly Churn Rate')


hist(dat_current$Churn12M, breaks = 500, 
     main = 'Histogram of Future Churn Rate for Active Meters',
     xlim = c(0,0.2),
     col = 3,
     xlab = 'Monthly Churn Rate')

hist(dat_current$Churn24M, breaks = 500, 
     main = 'Histogram of Future Churn Rate for Active Meters',
     xlim = c(0,0.1),
     col = 3,
     xlab = 'Monthly Churn Rate')



# plot all the survival curves for each separate segment

setwd("S:/Retail/IRL/Supply/Marketing/MARKETING SUPPLY/Analytics & Products/Analytics/MM WIP/BE Value Model/Retention Curves")

par(mfrow=c(1,1))
for (i in 1:length(split_dat1)) {
  cohort = DecToBin((as.integer(names(tab1[i]))-1),8)
  size = tab1[i]
  coh = unlist(strsplit(cohort, ""))
  for (a in 1:8) { if (coh[a] == '0') {val = bit_code[a,1]}
    else {val =  bit_code[a,2]}
    if (a==1) { val_tot = val}
    else {val_tot = paste0(val_tot, val)}}
  
  jpeg(paste0(val_tot,'.jpg'))
  plot(split_dat1[[i]], 
       col = 1, 
       lty = 1,  
       main = paste0("Retention Curve: ", cohort, ' cohort size: ', size), 
       xlab = "Time(Yrs)",
       xlim = c(0,120),
       ylim = c(0,1),
       ylab = "Retention Rate (%)", 
       xaxt = "n", yaxt = "n")
  # grid(nx=10, ny=10)
  
  legend("top",
         legend = val_tot,
         lty = 1,
         cex = 0.8,
         col = 1)
  axis(side = 1, at = seq(0, 120, by = 12), (seq(1:11)-1))
  axis(side = 2, at = seq(0, 1, by = .10), seq(0, 1, by = .10), las = 1)
  
  dev.off()
  #  ret_dat = cbind.data.frame('time (months)' = split_dat1[[i]]$time, 'survival' = split_dat1[[i]]$surv[1:(length(split_dat1[[i]]$surv)-1)])
  #  write.csv(ret_dat, paste0(val_tot,'.csv'), row.names = F)
}

# create a table of cohorts and counts
tab2 <- matrix(0, length(split_dat1), 2)
for (i in 1:length(split_dat1)) {
  cohort = DecToBin((as.integer(names(tab1[i]))-1),8)
  size = tab1[i]
  coh = unlist(strsplit(cohort, ""))
  for (a in 1:8) { if (coh[a] == '0') {val = bit_code[a,1]}
    else {val =  bit_code[a,2]}
    if (a==1) { val_tot = val}
    else {val_tot = paste0(val_tot, val)}}
  tab2[i,1] <- val_tot
  tab2[i,2] <- size
}

dat_fun = function(dat, x) {
  attach(dat)
  leg = levels(as.factor(FEATURE))
  l = length(leg)
  surv_dat = survfit(Surv(time = TENURE_MTHS, event = STATUS) ~ as.factor(FEATURE))
  plot(surv_dat, 
       col = c(1:l), 
       main = paste0("Survival Plot ",x), 
       xlab = "Time(Yrs)",
       xlim = c(2.69,69.31),
       ylim = c(0.037,0.963),
       ylab = "Retention",
       xaxt = "n",
       yaxt = "n")
  grid(nx=6, ny=10)
  legend("topright", 
         legend = leg, 
         lty = c(rep(1,l)), 
         cex = 0.8, 
         col = c(1:l))
  axis(side = 1, at = seq(0, 120, by = 12), (seq(1:11)-1))
  axis(side = 2, at = seq(0, 1.0, by = .10), seq(0, 1.0, by = .10), las = 1)
  detach(dat)
}

surv_fun <- function(x) {
  dat3 = dat[, c(which(names(dat) == 'TENURE_MTHS'),
                 which(names(dat) == 'STATUS'),
                 which(names(dat) == x))]
  colnames(dat3) = c(names(dat3[,c(1,2)]), 'FEATURE')
  dat_fun(dat3, x)
}

# dat5 = dat[dat$ROI == 0 & dat$ELEC == 1,] # this filter is useful for looking at export meters which are NI Elec only

#surv_dat = survfit(Surv(time = TENURE_MTHS, event = STATUS) ~ as.factor(FEATURE), data = dat3)

surv_fun('ROI')
surv_fun('EBILL')
surv_fun('DD_PAY')
surv_fun('MARKETING_OPT_IN')
surv_fun('COUNTY')
surv_fun('ACQ_CHANNEL')
surv_fun('ACQ_YR')
surv_fun('TPI')
surv_fun('YR_ACQ')
surv_fun('INDUSTRY')
surv_fun('PROMO_FLAG')
surv_fun('CONTRACT_TYPE')
surv_fun('SME')
surv_fun('ELEC')


# multiply the churn values by the normalized cox coefficients
dat_current$ChurnY1H1 = round(dat_current$ChurnY1H1 * dat_current$COX_CHAN_YR_CAT * dat_current$COX_COUNTY_IND * dat_current$COX_JUR_CON,4)
dat_current$ChurnY1H2 = round(dat_current$ChurnY1H2 * dat_current$COX_CHAN_YR_CAT * dat_current$COX_COUNTY_IND * dat_current$COX_JUR_CON,4)
dat_current$ChurnY2H1 = round(dat_current$ChurnY2H1 * dat_current$COX_CHAN_YR_CAT * dat_current$COX_COUNTY_IND * dat_current$COX_JUR_CON,4)
dat_current$ChurnY2H2 = round(dat_current$ChurnY2H2 * dat_current$COX_CHAN_YR_CAT * dat_current$COX_COUNTY_IND * dat_current$COX_JUR_CON,4)
dat_current$ChurnY3P = round(dat_current$ChurnY3P * dat_current$COX_CHAN_YR_CAT * dat_current$COX_COUNTY_IND * dat_current$COX_JUR_CON,4)
dat_current$Churn12M = round(dat_current$Churn12M * dat_current$COX_CHAN_YR_CAT * dat_current$COX_COUNTY_IND * dat_current$COX_JUR_CON,4)
dat_current$Churn24M = round(dat_current$Churn24M * dat_current$COX_CHAN_YR_CAT * dat_current$COX_COUNTY_IND * dat_current$COX_JUR_CON,4)


mean(dat_current$ChurnY1H1, na.rm = T)
# [1] 0.01458419
mean(dat_current$ChurnY1H2, na.rm = T)
# [1] 0.01826759
mean(dat_current$Churn12M, na.rm = T)
# [1] 0.05947666
mean(dat_current$ChurnY2H1, na.rm = T)
# [1] 0.04131753
mean(dat_current$ChurnY2H2, na.rm = T)
# [1] 0.02358115
mean(dat_current$Churn24M, na.rm = T)
# [1] 0.04056662
mean(dat_current$ChurnY3P, na.rm = T)
# [1] 0.02400705

dim(dat_current)

dat_current = dat_current[!is.na(dat_current$TENURE_MTHS),]
# dat_current = na.omit(dat_current)
num = dim(dat_current) [1]

# 5 year forecast from current customer's tenure
dat_current$m5y_y1h1 = rep(0,num) 
dat_current$m5y_y1h2 = rep(0,num) 
dat_current$m5y_m12 = rep(0,num) 
dat_current$m5y_y2h1 = rep(0,num) 
dat_current$m5y_y2h2 = rep(0,num) 
dat_current$m5y_m24 = rep(0,num) 
dat_current$m5y_y3p = rep(0,num) 

############  problem from here

dat_current$m5y_y1h1[dat_current$TENURE_MTHS <= 6 ] =  6 - 
  dat_current$TENURE_MTHS[dat_current$TENURE_MTHS <= 6 ] 
dat_current$m5y_y1h2 [dat_current$TENURE_MTHS <= 11] =  11 - 
  dat_current$TENURE_MTHS [dat_current$TENURE_MTHS <= 11] - 
  dat_current$m5y_y1h1 [dat_current$TENURE_MTHS <= 11]
dat_current$m5y_m12 [dat_current$TENURE_MTHS <= 12] = 1
dat_current$m5y_y2h1 [dat_current$TENURE_MTHS <= 18] = 18 - 
  dat_current$TENURE_MTHS[dat_current$TENURE_MTHS <= 18] - 
  dat_current$m5y_y1h1[dat_current$TENURE_MTHS <= 18] - 
  dat_current$m5y_y1h2[dat_current$TENURE_MTHS <= 18] - 
  dat_current$m5y_m12[dat_current$TENURE_MTHS <= 18]
dat_current$m5y_y2h2 [dat_current$TENURE_MTHS <= 23] =  23 - 
  dat_current$TENURE_MTHS [dat_current$TENURE_MTHS <= 23] - 
  dat_current$m5y_y1h1 [dat_current$TENURE_MTHS <= 23] - 
  dat_current$m5y_y1h2 [dat_current$TENURE_MTHS <= 23] - 
  dat_current$m5y_m12 [dat_current$TENURE_MTHS <= 23] - 
  dat_current$m5y_y2h1 [dat_current$TENURE_MTHS <= 23]
dat_current$m5y_m24 [dat_current$TENURE_MTHS <= 24] = 1
dat_current$m5y_y3p  = 60 - 
  dat_current$m5y_y1h1 - 
  dat_current$m5y_y1h2 - 
  dat_current$m5y_m12 - 
  dat_current$m5y_y2h1 - 
  dat_current$m5y_y2h2 - 
  dat_current$m5y_m24 

dat_current$avg_churn_5y = (dat_current$m5y_y1h1*dat_current$ChurnY1H1 +
                              dat_current$m5y_y1h2*dat_current$ChurnY1H2 +
                              dat_current$m5y_m12*dat_current$Churn12M +
                              dat_current$m5y_y2h1*dat_current$ChurnY2H1 +
                              dat_current$m5y_y2h2*dat_current$ChurnY2H2 +
                              dat_current$m5y_m24*dat_current$Churn24M +
                              dat_current$m5y_y3p*dat_current$ChurnY3P) / 60

# 3 year forecast from current customer's tenure
dat_current$m3y_y1h1 = rep(0,num) 
dat_current$m3y_y1h2 = rep(0,num) 
dat_current$m3y_m12 = rep(0,num) 
dat_current$m3y_y2h1 = rep(0,num) 
dat_current$m3y_y2h2 = rep(0,num) 
dat_current$m3y_m24 = rep(0,num) 
dat_current$m3y_y3p = rep(0,num) 

dat_current$m3y_y1h1[dat_current$TENURE_MTHS <= 6 ] =  6 - 
  dat_current$TENURE_MTHS[dat_current$TENURE_MTHS <= 6 ] 
dat_current$m3y_y1h2 [dat_current$TENURE_MTHS <= 11] =  11 - 
  dat_current$TENURE_MTHS [dat_current$TENURE_MTHS <= 11] - 
  dat_current$m3y_y1h1 [dat_current$TENURE_MTHS <= 11]
dat_current$m3y_m12 [dat_current$TENURE_MTHS <= 12] = 1
dat_current$m3y_y2h1 [dat_current$TENURE_MTHS <= 18] = 18 - 
  dat_current$TENURE_MTHS[dat_current$TENURE_MTHS <= 18] - 
  dat_current$m3y_y1h1[dat_current$TENURE_MTHS <= 18] - 
  dat_current$m3y_y1h2[dat_current$TENURE_MTHS <= 18] - 
  dat_current$m3y_m12[dat_current$TENURE_MTHS <= 18]
dat_current$m3y_y2h2 [dat_current$TENURE_MTHS <= 23] =  23 - 
  dat_current$TENURE_MTHS [dat_current$TENURE_MTHS <= 23] - 
  dat_current$m3y_y1h1 [dat_current$TENURE_MTHS <= 23] - 
  dat_current$m3y_y1h2 [dat_current$TENURE_MTHS <= 23] - 
  dat_current$m3y_m12 [dat_current$TENURE_MTHS <= 23] - 
  dat_current$m3y_y2h1 [dat_current$TENURE_MTHS <= 23]
dat_current$m3y_m24 [dat_current$TENURE_MTHS <= 24] = 1
dat_current$m3y_y3p  = 36 - 
  dat_current$m3y_y1h1 - 
  dat_current$m3y_y1h2 - 
  dat_current$m3y_m12 - 
  dat_current$m3y_y2h1 - 
  dat_current$m3y_y2h2 - 
  dat_current$m3y_m24 

dat_current$avg_churn_3y = (dat_current$m3y_y1h1*dat_current$ChurnY1H1 +
                              dat_current$m3y_y1h2*dat_current$ChurnY1H2 +
                              dat_current$m3y_m12*dat_current$Churn12M +
                              dat_current$m3y_y2h1*dat_current$ChurnY2H1 +
                              dat_current$m3y_y2h2*dat_current$ChurnY2H2 +
                              dat_current$m3y_m24*dat_current$Churn24M +
                              dat_current$m3y_y3p*dat_current$ChurnY3P) / 36


# 5 year forecast for new acquisitions at current customer mix
dat_current$avg_churn_5y0 = (6*dat_current$ChurnY1H1 +
                               5*dat_current$ChurnY1H2 +
                               1*dat_current$Churn12M +
                               6*dat_current$ChurnY2H1 +
                               5*dat_current$ChurnY2H2 +
                               1*dat_current$Churn24M +
                               36*dat_current$ChurnY3P) / 60

# 3 year forecast for new acquisitions at current customer mix
dat_current$avg_churn_3y0 = (6*dat_current$ChurnY1H1 +
                               5*dat_current$ChurnY1H2 +
                               1*dat_current$Churn12M +
                               6*dat_current$ChurnY2H1 +
                               5*dat_current$ChurnY2H2 +
                               1*dat_current$Churn24M +
                               12*dat_current$ChurnY3P) / 36

# histogram

par(mfrow = c(1,1))
hist(dat_current$avg_churn_5y, breaks = 500, 
     main = 'Histogram of 5yr Future Churn Rate for Active Meters',
     xlab = 'Avg. Monthly Churn Rate',
     col = 3,
     xlim = c(0,0.05))

mean(dat_current$avg_churn_5y[dat_current$avg_churn_5y>0])
# [1] 0.02492559

mean(dat_current$avg_churn_5y)
# [1] 0.02472647

summary(dat_current$avg_churn_5y)
#    Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
# 0.00000 0.00270 0.01589 0.02473 0.03725 0.64238


hist(dat_current$avg_churn_5y0, breaks = 500, 
     main = 'Histogram of 5yr Future Churn Rate for new acquisitions',
     xlab = 'Avg. Monthly Churn Rate',
     col = 3,
     xlim = c(0,0.05))

mean(dat_current$avg_churn_5y0[dat_current$avg_churn_5y0>0])
# [1] 0.02516295

mean(dat_current$avg_churn_5y0)
# [1] 0.02514919

summary(dat_current$avg_churn_5y0)
#     Min.  1st Qu.   Median     Mean  3rd Qu.     Max. 
# 0.000000 0.003062 0.017287 0.025149 0.038920 0.533247  

hist(dat_current$avg_churn_3y, breaks = 500, 
     main = 'Histogram of 3yr Future Churn Rate for Active Meters',
     xlab = 'Avg. Monthly Churn Rate',
     col = 3,
     xlim = c(0,0.05))

mean(dat_current$avg_churn_3y[dat_current$avg_churn_3y>0])
# [1] 0.02540906

mean(dat_current$avg_churn_3y)
# [1] 0.02520608

summary(dat_current$avg_churn_3y)
#    Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
# 0.00000 0.00270 0.01610 0.02521 0.03838 0.62234

hist(dat_current$avg_churn_3y0, breaks = 500, 
     main = 'Histogram of 3yr Future Churn Rate for new acquisitions',
     xlab = 'Avg. Monthly Churn Rate',
     col = 3,
     xlim = c(0,0.05))

mean(dat_current$avg_churn_3y0[dat_current$avg_churn_3y0>0])
# [1] 0.02592479

mean(dat_current$avg_churn_3y0)
# [1] 0.02591061

summary(dat_current$avg_churn_3y0)
#     Min.  1st Qu.   Median     Mean  3rd Qu.     Max. 
# 0.000000 0.003103 0.018142 0.025911 0.040929 0.439211

# by way of check we can get the average churn rate for the whole population
ret_new_acq = survfit(Surv(TENURE_MTHS, STATUS) ~ 1, data = dat)
ret_new_acq$surv = c(1,ret_new_acq$surv)
churn_new_acq = -diff(ret_new_acq$surv, lag = 1)/ret_new_acq$surv[1:(length(ret_new_acq$surv)-1)]
mean(churn_new_acq[1:60])/mean(dat_current$avg_churn_5y0)
# [1] 0.7308632
# this highlights an issue with the churn % allocations but we can use this 
# factor to re-adjust to get the average churn to agree. Can also do the same for
# the 3 yr projection

dat_current$multip_fact3y = 1 # mean(churn_new_acq[1:36])/mean(dat_current$avg_churn_3y0)
dat_current$multip_fact5y = 1 # mean(churn_new_acq[1:60])/mean(dat_current$avg_churn_5y0)


dat_churn = dat_current [,c(which(colnames(dat_current) == 'CUSTOMER_ID'),
                            which(colnames(dat_current) == 'PREMISE_ID'),
                            which(colnames(dat_current) == 'MPRN'),
                            which(colnames(dat_current) == 'REGISTER_DESC'),
                            which(colnames(dat_current) == 'SERVICENUM'),
                            which(colnames(dat_current) == 'REGISTERNUM'),
                            which(colnames(dat_current) == 'UTILITY_TYPE_CODE'),
                            which(colnames(dat_current) == 'DG'),
                            which(colnames(dat_current) == 'PROFILE'),
                            which(colnames(dat_current) == 'DATEDIFF'),
                            which(colnames(dat_current) == 'ChurnY1H1'),
                            which(colnames(dat_current) == 'ChurnY1H2'),
                            which(colnames(dat_current) == 'ChurnY2H1'),
                            which(colnames(dat_current) == 'ChurnY2H2'),
                            which(colnames(dat_current) == 'ChurnY3P'),
                            which(colnames(dat_current) == 'Churn12M'),
                            which(colnames(dat_current) == 'Churn24M'),
                            which(colnames(dat_current) == 'avg_churn_5y'),
                            which(colnames(dat_current) == 'avg_churn_3y'),
                            which(colnames(dat_current) == 'avg_churn_5y0'),
                            which(colnames(dat_current) == 'avg_churn_3y0'),
                            which(colnames(dat_current) == 'multip_fact3y'),
                            which(colnames(dat_current) == 'multip_fact5y'))]

mean(dat_churn$avg_churn_5y0 * dat_churn$multip_fact5y)
# [1] 0.02514919
mean(churn_new_acq[1:60])
# [1] 0.01838062
mean(dat_churn$avg_churn_3y0 * dat_churn$multip_fact3y)
# [1] 0.02591061
mean(churn_new_acq[1:36])
# [1] 0.0195839

#hist(dat_churn$avg_churn_5y0 * dat_churn$multip_fact5y, breaks = 200, 
     #main = 'Histogram of 5yr Future Churn Rate for new acquisitions',
     #xlab = 'Avg. Monthly Churn Rate',
     #col = 3,
     #xlim = c(0,0.05))

#hist(dat_churn$avg_churn_3y0 * dat_churn$multip_fact3y, breaks = 200, 
     #main = 'Histogram of 3yr Future Churn Rate for new acquisitions',
     #xlab = 'Avg. Monthly Churn Rate',
     #col = 3,
     #xlim = c(0,0.05))

#hist(dat_churn$avg_churn_5y * dat_churn$multip_fact5y, breaks = 500, 
     #main = 'Histogram of 5yr Future Churn Rate for current customers',
     #xlab = 'Avg. Monthly Churn Rate',
     #col = 3,
     #xlim = c(0,0.05))

#hist(dat_churn$avg_churn_3y * dat_churn$multip_fact3y, breaks = 500, 
     #main = 'Histogram of 3yr Future Churn Rate for current customers',
     #xlab = 'Avg. Monthly Churn Rate',
     #col = 3,
     #xlim = c(0,0.05))

mean(dat_churn$avg_churn_5y * dat_churn$multip_fact5y)
# [1] 0.02570123
setwd("S:/Retail/IRL/Supply/Marketing/MARKETING SUPPLY/Analytics & Products/Analytics/MM WIP/BE Value Model")
day_month_yr = paste0(format(Sys.time(), "%d"), format(Sys.time(), "%b"), format(Sys.time(), "%y"))
# write.csv(dat_churn, paste0("BE_CLV_ret_",day_month_yr,".csv"), row.names = F)
dat_churn_rates = dat_churn


# ###############################################################
# 
# # write churn data to a table in PRD1
# 
# ###############################################################
# 
# # fields required:-
# # Customer_ID, Premise_ID, MPRN, register_desc, Utility_Type_Code, churn_new, churn12M, churn24M, Updated
# 
# # test script
# dbGetQuery(conn, "create table Analytics.MM_tst (d date, i integer)")
# ## make sure the input data.frame has the correct types
# d <- data.frame(d = "11-MAR-04", i = as.integer(100))
# d$d <- as.character(d$d)         ## should *not* be a factor
# ## prepared statements automatically begin a new transaction
# ps <- dbPrepareStatement(conn, "insert into Analytics.MM_tst values (:1, :2)", 
#                          bind = c("character", "integer"))
# dbExecStatement(ps, d)     ## do the actual insert
# ## close the prepared statement to force a commit (otherwise you
# ## won't see the changes to the table)
# dbClearResult(ps)
# # [1] TRUE
# dbReadTable(conn, "Analytics.MM_tst")
# #         D   I
# # 11-MAR-04 100
# 
# 
# # write script
# dbGetQuery(conn, "create table Analytics.Cust_Churn_Rate (Customer_ID integer,
#            Premise_ID integer,
#            MPRN character,
#            REGISTER_DESC character,
#            Utility_type_Code Character,
#            Churn_rate_Y1H1 number,
#            Churn_rate_Y1H2 number,
#            Churn_rate_Y2H1 number,
#            Churn_rate_Y2H2 number,
#            Churn_rate_Y3P number,
#            Churn_12M number,
#            Churn_24M number,
#            Update_Date date)")
# 
# ## make sure the input data.frame has the correct types
# d <- dat_current[,c(which(colnames(dat_current) == 'CUSTOMER_ID'),
#                     which(colnames(dat_current) == 'PREMISE_ID'),
#                     which(colnames(dat_current) == 'MPRN'),
#                     which(colnames(dat_current) == 'register_desc'),
#                     which(colnames(dat_current) == 'UTILITY_TYPE_CODE'),
#                     which(colnames(dat_current) == 'ChurnY1H1'),
#                     which(colnames(dat_current) == 'ChurnY1H2'),
#                     which(colnames(dat_current) == 'ChurnY2H1'),
#                     which(colnames(dat_current) == 'ChurnY2H2'),
#                     which(colnames(dat_current) == 'ChurnY3P'),
#                     which(colnames(dat_current) == 'Churn12M'),
#                     which(colnames(dat_current) == 'Churn24M'))]
# 
# num = dim(d) [1]
# d = cbind.data.frame(d, 'Update_Date' = rep(Sys.Date(),num))
# 
# ## columns should *not* be factors
# d$Update_Date <- as.character(d$Update_Date)         
# d$MPRN <- as.character(d$MPRN)
# d$REGISTER_DESC <- as.character(d$REGISTER_DESC)
# d$UTILITY_TYPE_CODE <- as.character(d$UTILITY_TYPE_CODE)
# 
# 
# ## prepared statements automatically begin a new transaction
# ps <- dbPrepareStatement(conn, "insert into Analytics.Cust_Churn_Rate values (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13)", 
#                          bind = c("integer"
#                                   , "integer"
#                                   , "character"
#                                   , "character"
#                                   , "character"
#                                   , "number"
#                                   , "number"
#                                   , "number"
#                                   , "number"
#                                   , "number"
#                                   , "number"
#                                   , "number"
#                                   , "character"))
# dbExecStatement(ps, d)     ## do the actual insert
# ## close the prepared statement to force a commit (otherwise you won't see the changes to the table)
# dbClearResult(ps)
# # [1] TRUE
# dbReadTable(conn, "Analytics.Cust_Churn_Rate")

#########################################################################################################
##################### BE CHURN MODEL ####################################################################
#########################################################################################################

# Install specific version of recipes
#remotes::install_version("recipes", version = "0.1.13", repos = "http://cran.us.r-project.org")

# Install specific version of caret
#remotes::install_version("caret", version = "6.0-84", repos = "http://cran.us.r-project.org")


#remotes::install_version("glmnet", version = "3.0-2", repos = "http://cran.us.r-project.org")


# Load the packages
#library(recipes)
#library(caret)
#library(glmnet)
# (Add loading for other required packages)







library(DBI)
library(ROracle)
library(dplyr)
library(dbplyr)
library(survival)
library(tidyverse)
library(caret)
library(glmnet)
library(sqldf)

# load the relevent ML libraries
library(e1071)
library(rpart)
library(partykit)
library(adabag)
library(nnet)
library(randomForest)
library(kernlab)
library(fpc)
library(matrixStats)

drv <- dbDriver("Oracle")
host <- "Azula008"
port <- "1526"
sid <- "DMPRD1"
connect.string <- paste(
  "(DESCRIPTION=",
  "(ADDRESS=(PROTOCOL=tcp)(HOST=", host, ")(PORT=", port, "))",
  "(CONNECT_DATA=(SID=", sid, ")))", sep = "")

conn <- dbConnect(drv, username =  dbname = connect.string, prefetch = FALSE,
                  bulk_read = 1000L, stmt_cache = 0L, external_credentials = FALSE,
                  sysdba = FALSE)


dat <- dbGetQuery(conn, "SELECT * FROM Analytics.Fact_BE_churn_model_V2")

# dat = dat[!is.na(dat$EAC),]

dbDisconnect(conn)

# host <- "dubla055.airtricity.com"
# port <- "1526"
# sid <- "pceprdr"
# connect.string <- paste(
#   "(DESCRIPTION=",
#   "(ADDRESS=(PROTOCOL=tcp)(HOST=", host, ")(PORT=", port, "))",
#   "(CONNECT_DATA=(SID=", sid, ")))", sep = "")
# 
# conn_pce <- dbConnect(drv, username =  dbname = connect.string, prefetch = FALSE,
#                       bulk_read = 1000L, stmt_cache = 0L, external_credentials = FALSE,
#                       sysdba = FALSE)
# 
# 
# dat_classif <- dbGetQuery(conn_pce, "select A.debtornum, A.Gold_plus_class  from (
# select
# CL.debtornum
# , row_number() over (partition by CL.debtornum order by CL.DATE_EFFECTIVE desc) as row_id 
# , Case when LTRIM(RTRIM(CL.CLASSIFICATN_CODE)) in ('GOLD', 'PLATINUM') then 'Y' ELSE 'N' END AS Gold_plus_class
# from energydb.AR_CUST_CLASS_HIST CL
# inner join energydb.pm_all_consprem AB
# on CL.debtornum = AB.debtornum
# WHERE AB.status_cons = 'C'
# AND to_char(AB.changedate,'YYYY-MM-DD') < to_char(sysdate,'YYYY-MM-DD')
# ) A
# where A.row_id = 1
# order by A.debtornum")
# 
# dbDisconnect(conn_pce)

# dat[dat$MPRN == '10007840128',]

table(dat$CNT_CONTRACTS, useNA = 'ifany')



dat$CNT_CONTRACTS <- ifelse(is.na(dat$CNT_CONTRACTS),1,dat$CNT_CONTRACTS)

dat$NUM_CONTRACT_BKT <- cut(dat$CNT_CONTRACTS, breaks = c(-Inf, 1, 2, 4, 8, 16, 32, Inf), labels = c('0-1', '2', '3-4', '5-8', '9-16', '17-32', '32+'))

table(dat$CNT_CONTRACTS, dat$NUM_CONTRACT_BKT, useNA = 'ifany')

dat_1 <- dat[,c(which(colnames(dat) == 'TRI_MONTHLY_CONTRACT_END'),
                which(colnames(dat) == 'TRI_MONTHLY_CONTRACT_END_F'),
                which(colnames(dat) == 'CONTRACT_END_DATE'),
                which(colnames(dat) == 'ACQUISITION_DATE'),
                which(colnames(dat) == 'LOSS_DATE'),
                which(colnames(dat) == 'STATUS'),
                which(colnames(dat) == 'STATUS_TEST'),
                which(colnames(dat) == 'NUM_YRS_CON_ENDED'),
                which(colnames(dat) == 'NUM_YRS_AGO_LOSS'))]

# dat <- dat [, -c(which(colnames(dat) == 'ACQUISITION_DATE'),
#                  which(colnames(dat) == 'LOSS_DATE'))]

dat$HORIZON_CONTRACT_FLAG = ifelse(is.na(dat$HORIZON_CONTRACT_FLAG),0,1)

table(dat$CONTRACT_TYPE, dat$HORIZON_CONTRACT_FLAG, useNA = 'ifany')

#                                                        0     1
# Commercial Pricing Contract                        26983  1153
# Introductory Period Discount                         849     0
# Small/Medium Enterprise Price Eligibility Contract  4013     5
# <NA>                                               18739   765

table(dat$STATUS, dat$HORIZON_CONTRACT_FLAG, useNA = 'ifany')
#       0     1
# 0 26441  1134
# 1 24143   789

table(dat$STATUS_TEST, dat$HORIZON_CONTRACT_FLAG, useNA = 'ifany')
#       0     1
# 0 22720  1015
# 1 27864   908

table(dat$ACQ_SAME_CUST_BEFORE, dat$ACQ_SAME_METER_BEFORE, useNA = 'ifany')
#         0     1
# 0 133252  87124
# 1      0   3784

table(dat$CNT_SAME_CUST_METER_ACQS, dat$CNT_METER_ACQS, useNA = 'ifany')
#       1     2     3     4     5     6     7     8     9    10    11    12    13    14    15    16    17
# 1 81306 57287 34709 19954 11069  5533  3552  1651   945   368   141    88    22    42    15    12    17
# 2     0  3146  1894   974   526   236   166    76    42    22     2     8     4     0     0     4     0
# 3     0     0   156    72    51    33     9     9     3     0     0     0     0     0     0     0     0
# 4     0     0     0     8     4     0     4     0     0     0     0     0     0     0     0     0     0

# remove time from contract end date
# dat$CONTRACT_END_DATE = as.character(dat$CONTRACT_END_DATE)
dat$CONTRACT_END_DATE = gsub(x=dat$CONTRACT_END_DATE,pattern=" 00:00:00",replacement="",fixed=T)
dat$CONTRACT_END_DATE = gsub(x=dat$CONTRACT_END_DATE,pattern=" 23:00:00",replacement="",fixed=T)
dat$CONTRACT_END_DATE <- as.Date(dat$CONTRACT_END_DATE)

last_update = dat[1,which(colnames(dat) == 'LAST_UPDATE_DATE')]
last_update = as.Date(gsub(last_update, pattern = " BST", replacement = "", fixed = T))

dat = dat[,-c(which(colnames(dat) == 'LAST_UPDATE_DATE'))]

month_day = paste0(format(Sys.time(), "%b"), format(Sys.time(), "%d"))

dat$CON_END  = ifelse(!is.na(dat$TRI_MONTHLY_CONTRACT_END) & dat$TRI_MONTHLY_CONTRACT_END < 0, 'P', 'F')
dat$CON_END_F = ifelse(!is.na(dat$TRI_MONTHLY_CONTRACT_END_F) & dat$TRI_MONTHLY_CONTRACT_END_F < 0, 'P', 'F')

# write.csv(dat, paste0("BE_Churn_",month_day, "v2.csv"))
# dat7 = read.csv(paste0("BE_Churn_",month_day), header = T)
# dat7 = dat7[,-which(colnames(dat7)=='X')]

# dat8 = dat
# dat = dat7

diff = dat$TENURE_3MTHS_F - dat$TENURE_3MTHS
diff2 = dat$TRI_MONTHLY_CONTRACT_END_ABS_F - dat$TRI_MONTHLY_CONTRACT_END_ABS
table(diff[dat$STATUS_TEST == 0], useNA = 'ifany')
table(diff2[dat$STATUS_TEST == 0], useNA = 'ifany')
table(diff[dat$STATUS_TEST == 1], useNA = 'ifany')
table(diff2[dat$STATUS_TEST == 1], useNA = 'ifany')

table(diff[dat$STATUS == 0], useNA = 'ifany')
table(diff2[dat$STATUS == 0], useNA = 'ifany')
table(diff[dat$STATUS == 1], useNA = 'ifany')
table(diff2[dat$STATUS == 1], useNA = 'ifany')

table(dat$YR_ACQ,useNA = 'ifany')
table(dat$YR_ACQ_F,useNA = 'ifany')

tab1 = table(dat$LOSS_YRS_AGO, useNA = 'ifany')
sum(dat$LOSS_YRS_AGO_F <= 6.0 | is.na(dat$LOSS_YRS_AGO_F))
# [1] 62178
sum(dat$LOSS_YRS_AGO_F <= 6.0 ,na.rm=T)
# [1] 31526
sum(dat$LOSS_YRS_AGO_F <= 5.0 | is.na(dat$LOSS_YRS_AGO_F))
# [1] 61297
sum(dat$LOSS_YRS_AGO_F <= 5.0 ,na.rm = T)
# [1] 30645

# find a 'loss years ago' value such that the training set sample size of losses is
# almost the same as the training set sample size of current customers
# this currently doesn't filter any old losses out but in time it should filter out very old losses

# n = dim(tab1)
# min_diff = tab1[n] - tab1[1]
# cumm = tab1[1]
# for (i in 2:(n-1)) {
#   cumm = cumm + tab1[i]
#   if(abs(tab1[n]- cumm) < min_diff) {min_diff = abs(tab1[n] - cumm)
#   res = i}
# }
# as.numeric(names(tab1[res]))
# # [1] 13
# 
# dat_1 = dat[dat$LOSS_YRS_AGO <= as.numeric(names(tab1[res])) | is.na(dat$LOSS_YRS_AGO),]
# dat_1 = dat_1[, -c(which(names(dat_1) == 'LOSS_YRS_AGO'),
#                     which(names(dat_1) == 'LOSS_YRS_AGO_F'))]
# dat = dat_1

# dat_1 <- dat[(!is.na(dat$TRI_MONTHLY_CONTRACT_END_ABS_F) & (dat$TRI_MONTHLY_CONTRACT_END_ABS_F - dat$TRI_MONTHLY_CONTRACT_END_ABS) <= -3),]

str(dat)

month_day = paste0(format(Sys.time(), "%b"), format(Sys.time(), "%d"))
# dat = read.csv(paste0("BE_Churn_",month_day,".csv"), header = T)
# dat = dat[,-1]
table(dat$INDUSTRY ,useNA = 'ifany')
table(dat$CREDIT_WEIGHTING ,useNA = 'ifany')

# replace NA's with 'UNKN' or with median value
dat$INDUSTRY[is.na(dat$INDUSTRY)] <- 'UNKN'
dat$CREDIT_WEIGHTING = ifelse(is.na(dat$CREDIT_WEIGHTING), '0', dat$CREDIT_WEIGHTING)
dat$CREDIT_WEIGHTING = ifelse(dat$CREDIT_WEIGHTING == ' N/A', '0', dat$CREDIT_WEIGHTING)
dat$CREDIT_WEIGHTING = as.integer(dat$CREDIT_WEIGHTING)


# str(dat)
# dat$MONTH_NAME = factor(substr(dat$MONTH_NAME,1,3), levels=month.abb, ordered = T)
# print(levels(dat$MONTH_NAME))
# 
# dat$YR_ACQ_QTR = dat$YR_ACQ + (((as.integer(dat$MONTH_NAME) -1 )%/% 3)/4)

# centre the acquisition year
# dat$YR_ACQ_QTR = dat$YR_ACQ_QTR - median(dat$YR_ACQ_QTR)
# dat$YR2 = dat$YR_ACQ_QTR ^2
# dat$YR3 = dat$YR_ACQ_QTR ^3
# dat$YR4 = dat$YR_ACQ_QTR ^4
# dat$YR5 = dat$YR_ACQ_QTR ^5
# dat$YR6 = dat$YR_ACQ_QTR ^6

# cor(dat[,c(which(colnames(dat)=='YR_ACQ_CEN'),
#            which(colnames(dat)=='YR2'),
#            which(colnames(dat)=='YR3'),
#            which(colnames(dat)=='YR4'),
#            which(colnames(dat)=='YR5'),
#            which(colnames(dat)=='YR6'))])
# YR_ACQ        YR2        YR3        YR4        YR5        YR6
# YR_ACQ  1.0000000 -0.4387638  0.7596728 -0.5093120  0.5546594 -0.4655693
# YR2    -0.4387638  1.0000000 -0.8063041  0.9116730 -0.8180214  0.8066762
# YR3     0.7596728 -0.8063041  1.0000000 -0.9211088  0.9414793 -0.8857804
# YR4    -0.5093120  0.9116730 -0.9211088  1.0000000 -0.9780055  0.9719387
# YR5     0.5546594 -0.8180214  0.9414793 -0.9780055  1.0000000 -0.9892844
# YR6    -0.4655693  0.8066762 -0.8857804  0.9719387 -0.9892844  1.0000000

# remove any negative datediffs
dat = dat[-dat$TENURE_DAYS<0,]


sum(dat$MPRN == ' N/A')
# [1] 30
# remove rows where MPRN is N/A
dat = dat[-c(which(dat$MPRN == ' N/A')),]
# dat[dat$MPRN == ' N/A',]

sum(dat$AVG_YEARLY_CONSUMPTION<=100)
# [1] 1882

indx = (dat$AVG_YEARLY_CONSUMPTION <= 0 | 
          is.na(dat$AVG_YEARLY_CONSUMPTION) | 
          (!is.na(dat$MAX_TO_MEAN_READ_RATIO) & 
             !is.na(dat$AVG_YEARLY_CONSUMPTION) & 
             dat$MAX_TO_MEAN_READ_RATIO > 400 & 
             dat$AVG_YEARLY_CONSUMPTION > 10000  &
             log(dat$USAGE_VAR) < 1) |
          (!is.na(dat$MAX_TO_MEAN_READ_RATIO) & 
             !is.na(dat$AVG_YEARLY_CONSUMPTION) & 
             dat$MAX_TO_MEAN_READ_RATIO > 50 & 
             dat$AVG_YEARLY_CONSUMPTION > 1000000)  |
          (!is.na(dat$AVG_YEARLY_CONSUMPTION)  & 
             dat$AVG_YEARLY_CONSUMPTION > 5000000) )
sum(indx, na.rm = T)
# [1] 484

sort(dat$CUSTOMER_ID[indx])
# [1]  104307  104307  134472 1460520 1615373 1671608 1704590 1741415 1897858 1948716
# [11] 1980136 1984658 1985968 1987978 2041162 2140553 2155035 2165250 2174334 2191277
# [21] 2339619 2343880 2377095 2407698 2413597 2413597 2436365


# hist(log(dat$MAX_TO_MEAN_READ_RATIO), breaks = 500)
# hist(log(dat$MAX_TO_MEAN_READ_RATIO[indx]), breaks = 500)
# hist(log(dat$AVG_YEARLY_CONSUMPTION), breaks = 500)
# hist(log(dat$AVG_YEARLY_CONSUMPTION[indx]), breaks = 500)
# hist(log(dat$USAGE_VAR), breaks = 500)
# hist(log(dat$USAGE_VAR[indx]), breaks = 50)

dat$AVG_YEARLY_CONSUMPTION[indx] = NA
dat[ dat$AVG_YEARLY_CONSUMPTION == max(dat$AVG_YEARLY_CONSUMPTION, na.rm= T),]
dat[dat$MPRN == '10002597635',]

# dat$AVG_YEARLY_CONSUMPTION [ dat$AVG_YEARLY_CONSUMPTION <= 10 ] = 10

# replace blank usage with usage for that MPRN if we have it

dat4 <- na.omit(dat[,c(which(colnames(dat)=='AVG_YEARLY_CONSUMPTION'),
                       which(colnames(dat)=='ELEC'),
                       which(colnames(dat)=='MPRN'))])

sum_table = aggregate(dat4$AVG_YEARLY_CONSUMPTION, 
                      list(dat4$ELEC, 
                           dat4$MPRN), 
                      FUN = mean)

colnames(sum_table) = c(names(dat4)[2:length(names(dat4))], 'AVG_ANNUAL_USAGE')

library(sqldf)

dat <- sqldf("SELECT L.*, R.AVG_ANNUAL_USAGE
              FROM dat as L
              LEFT JOIN sum_table as R
              on L.ELEC = R.ELEC
              AND L.MPRN = R.MPRN")

# str(dat)

dat$AVG_YEARLY_CONSUMPTION = as.numeric(dat$AVG_YEARLY_CONSUMPTION)
dat$AVG_ANNUAL_USAGE = as.numeric(dat$AVG_ANNUAL_USAGE)

table(is.na(dat$AVG_YEARLY_CONSUMPTION), !is.na(dat$AVG_ANNUAL_USAGE), useNA = 'ifany')

dat$AVG_YEARLY_CONSUMPTION <- if_else(is.na(dat$AVG_YEARLY_CONSUMPTION), dat$AVG_ANNUAL_USAGE, dat$AVG_YEARLY_CONSUMPTION)

# dat[dat$MPRN == '10007840128',]

dat = dat[,-which(colnames(dat) == 'AVG_ANNUAL_USAGE')]

# dat[ dat$AVG_YEARLY_CONSUMPTION == max(dat$AVG_YEARLY_CONSUMPTION, na.rm= T),]
# dat[dat$MPRN == '81380919227',]

# replace blank usage with usage for the mediam of the cohort
indx = (dat$AVG_YEARLY_CONSUMPTION <= 0 | 
          is.na(dat$AVG_YEARLY_CONSUMPTION) | 
          (!is.na(dat$MAX_TO_MEAN_READ_RATIO) & 
             !is.na(dat$AVG_YEARLY_CONSUMPTION) & 
             dat$MAX_TO_MEAN_READ_RATIO > 400 & 
             dat$AVG_YEARLY_CONSUMPTION > 10000  &
             log(dat$USAGE_VAR) < 1) |
          (!is.na(dat$MAX_TO_MEAN_READ_RATIO) & 
             !is.na(dat$AVG_YEARLY_CONSUMPTION) & 
             dat$MAX_TO_MEAN_READ_RATIO > 50 & 
             dat$AVG_YEARLY_CONSUMPTION > 1000000)  |
          (!is.na(dat$AVG_YEARLY_CONSUMPTION)  & 
             dat$AVG_YEARLY_CONSUMPTION > 5000000) )

sum(indx, na.rm=T)

if (sum(indx, na.rm = T) > 0 | sum(is.na(dat$AVG_YEARLY_CONSUMPTION)) > 0 ) {
  
  dat4 <- na.omit(dat[,c(which(colnames(dat)=='AVG_YEARLY_CONSUMPTION'),
                         which(colnames(dat)=='ELEC'),
                         which(colnames(dat)=='EBILL'),
                         which(colnames(dat)=='DD_PAY'),
                         which(colnames(dat)=='ROI'),
                         which(colnames(dat)=='MARKETING_OPT_IN'),
                         which(colnames(dat)=='CITY'),
                         which(colnames(dat)=='BE_SEGMENT'),
                         which(colnames(dat)=='INDUSTRY'))])
  
  sum_table = aggregate(dat4$AVG_YEARLY_CONSUMPTION, 
                        list(dat4$ELEC, 
                             dat4$EBILL, 
                             dat4$DD_PAY, 
                             dat4$ROI, 
                             dat4$MARKETING_OPT_IN, 
                             dat4$CITY, 
                             dat4$BE_SEGMENT, 
                             dat4$INDUSTRY), 
                        FUN = median)
  
  colnames(sum_table) = c(names(dat4)[2:length(names(dat4))], 'AVG_ANNUAL_USAGE')
  
  dat <- sqldf("SELECT L.*, R.AVG_ANNUAL_USAGE
               FROM dat as L
               LEFT JOIN sum_table as R
               ON L.ELEC = R.ELEC
               AND L.EBILL = R.EBILL
               AND L.DD_PAY = R.DD_PAY
               AND L.ROI = R.ROI
               AND L.MARKETING_OPT_IN = R.MARKETING_OPT_IN
               AND L.CITY = R.CITY
               AND L.BE_SEGMENT = R.BE_SEGMENT
               AND L.INDUSTRY = R.INDUSTRY")
  
  # str(dat)
  
  dat$AVG_YEARLY_CONSUMPTION = as.numeric(dat$AVG_YEARLY_CONSUMPTION)
  dat$AVG_ANNUAL_USAGE = as.numeric(dat$AVG_ANNUAL_USAGE)
  
  dat$AVG_YEARLY_CONSUMPTION <- if_else(is.na(dat$AVG_YEARLY_CONSUMPTION), dat$AVG_ANNUAL_USAGE, dat$AVG_YEARLY_CONSUMPTION)
  
}

summary(dat$AVG_YEARLY_CONSUMPTION)
#    Min. 1st Qu.  Median    Mean 3rd Qu.    Max.    NA's 
#       1    4303   10432   31841   26441 4445868      11 

indx = is.na(dat$USAGE_VAR) | dat$USAGE_VAR == 0 | (!is.na(dat$USAGE_VAR) & 
                                                      !is.na(dat$MAX_TO_MEAN_READ_RATIO) & 
                                                      dat$MAX_TO_MEAN_READ_RATIO > 200 & 
                                                      !is.na(dat$AVG_YEARLY_CONSUMPTION) & 
                                                      dat$AVG_YEARLY_CONSUMPTION > 3000 & 
                                                      (dat$USAGE_VAR == 0.00 | dat$USAGE_VAR>=100) |
                                                      (!is.na(dat$USAGE_VAR) & 
                                                         !is.na(dat$MAX_TO_MEAN_READ_RATIO) & 
                                                         dat$MAX_TO_MEAN_READ_RATIO > 50 & 
                                                         !is.na(dat$AVG_YEARLY_CONSUMPTION) & 
                                                         dat$AVG_YEARLY_CONSUMPTION > 100000 & 
                                                         (dat$USAGE_VAR == 0.00 | log(dat$USAGE_VAR)>=2)) )
sum(indx)
# [1] 57450

if (sum(indx) > 0 | sum(is.na(dat$USAGE_VAR)) > 0 ) {
  
  dat$USAGE_VAR[indx] = NA
  
  dat4 <- na.omit(dat[,c(which(colnames(dat)=='USAGE_VAR'),
                         which(colnames(dat)=='ELEC'),
                         which(colnames(dat)=='MPRN'))])
  sum_table = aggregate(dat4$USAGE_VAR, 
                        list(dat4$ELEC, 
                             dat4$MPRN), 
                        FUN = mean)
  
  colnames(sum_table) = c(names(dat4)[2:length(names(dat4))], 'AVG_CONSUM_VAR')
  
  dat <- sqldf("SELECT L.*, R.AVG_CONSUM_VAR
              FROM dat as L
              LEFT JOIN sum_table as R
              on L.ELEC = R.ELEC
              AND L.MPRN = R.MPRN")
  
  dat$USAGE_VAR = as.numeric(dat$USAGE_VAR)
  dat$AVG_CONSUM_VAR = as.numeric(dat$AVG_CONSUM_VAR)
  dat$USAGE_VAR = if_else(is.na(dat$USAGE_VAR), dat$AVG_CONSUM_VAR, dat$USAGE_VAR)
  
  dat = dat[,-which(colnames(dat)== 'AVG_CONSUM_VAR')]
  
  summary(dat$USAGE_VAR)
}

indx = is.na(dat$USAGE_VAR) | dat$USAGE_VAR == 0 | (!is.na(dat$USAGE_VAR) & 
                                                      !is.na(dat$MAX_TO_MEAN_READ_RATIO) & 
                                                      dat$MAX_TO_MEAN_READ_RATIO > 200 & 
                                                      !is.na(dat$AVG_YEARLY_CONSUMPTION) & 
                                                      dat$AVG_YEARLY_CONSUMPTION > 3000 & 
                                                      (dat$USAGE_VAR == 0.00 | dat$USAGE_VAR>=100) |
                                                      (!is.na(dat$USAGE_VAR) & 
                                                         !is.na(dat$MAX_TO_MEAN_READ_RATIO) & 
                                                         dat$MAX_TO_MEAN_READ_RATIO > 50 & 
                                                         !is.na(dat$AVG_YEARLY_CONSUMPTION) & 
                                                         dat$AVG_YEARLY_CONSUMPTION > 100000 & 
                                                         (dat$USAGE_VAR == 0.00 | log(dat$USAGE_VAR)>=2)) )


if (sum(indx, na.rm = T) > 0 | sum(is.na(dat$USAGE_VAR)) > 0 ) {
  
  dat4 <- na.omit(dat[,c(which(colnames(dat)=='USAGE_VAR'),
                         which(colnames(dat)=='ELEC'),
                         which(colnames(dat)=='EBILL'),
                         which(colnames(dat)=='DD_PAY'),
                         which(colnames(dat)=='ROI'),
                         which(colnames(dat)=='MARKETING_OPT_IN'),
                         which(colnames(dat)=='CITY'),
                         which(colnames(dat)=='BE_SEGMENT'),
                         which(colnames(dat)=='INDUSTRY'))])
  
  sum_table = aggregate(dat4$USAGE_VAR, 
                        list(dat4$ELEC, 
                             dat4$EBILL, 
                             dat4$DD_PAY, 
                             dat4$ROI, 
                             dat4$MARKETING_OPT_IN, 
                             dat4$CITY, 
                             dat4$BE_SEGMENT, 
                             dat4$INDUSTRY), 
                        FUN = median)
  
  colnames(sum_table) = c(names(dat4)[2:length(names(dat4))], 'AVG_CONSUM_VAR')
  
  dat <- sqldf("SELECT L.*, R.AVG_CONSUM_VAR
               FROM dat as L
               LEFT JOIN sum_table as R
               ON L.ELEC = R.ELEC
               AND L.EBILL = R.EBILL
               AND L.DD_PAY = R.DD_PAY
               AND L.ROI = R.ROI
               AND L.MARKETING_OPT_IN = R.MARKETING_OPT_IN
               AND L.CITY = R.CITY
               AND L.BE_SEGMENT = R.BE_SEGMENT
               AND L.INDUSTRY = R.INDUSTRY")
  
  dat$USAGE_VAR = as.numeric(dat$USAGE_VAR)
  dat$AVG_CONSUM_VAR = as.numeric(dat$AVG_CONSUM_VAR)
  dat$USAGE_VAR = if_else(is.na(dat$USAGE_VAR), dat$AVG_CONSUM_VAR, dat$USAGE_VAR)
  summary(dat$USAGE_VAR)
  
}

table(is.na(dat$USAGE_VAR), !is.na(dat$AVG_CONSUM_VAR), useNA = 'ifany')
#        FALSE   TRUE
# FALSE      0 223478
# TRUE      67      0

# sort(dat$AVG_YEARLY_CONSUMPTION, decreasing = T)

# sort(dat$USAGE_VAR, decreasing = T)

summary(dat$USAGE_VAR)
#    Min. 1st Qu.  Median    Mean 3rd Qu.    Max.    NA's 
#   0.010   0.660   0.860   1.028   1.170 410.700      67 

# get the average energy consumption for the BE segment (Enterprise or SME)

dat4 <- na.omit(dat[,c(which(colnames(dat)=='AVG_YEARLY_CONSUMPTION'),
                       which(colnames(dat)=='ELEC'),
                       which(colnames(dat)=='BE_SEGMENT'))])

sum_table = aggregate(dat4$AVG_YEARLY_CONSUMPTION, 
                      list(dat4$ELEC, 
                           dat4$BE_SEGMENT), 
                      FUN = median)

colnames(sum_table) = c(names(dat4)[2:length(names(dat4))], 'AVG_CONSUM_SEG_UTIL')

dat <- sqldf("SELECT L.*, R.AVG_CONSUM_SEG_UTIL
              FROM dat as L
              LEFT JOIN sum_table as R
              on L.ELEC = R.ELEC
              AND L.BE_SEGMENT = R.BE_SEGMENT")

dat$AVG_YEARLY_CONSUMPTION = as.numeric(dat$AVG_YEARLY_CONSUMPTION)
dat$AVG_CONSUM_SEG_UTIL = as.numeric(dat$AVG_CONSUM_SEG_UTIL)

dat$AVG_YEARLY_CONSUMPTION <- if_else(is.na(dat$AVG_YEARLY_CONSUMPTION), dat$AVG_CONSUM_SEG_UTIL, dat$AVG_YEARLY_CONSUMPTION)

summary(dat$AVG_YEARLY_CONSUMPTION )

dat4 <- na.omit(dat[,c(which(colnames(dat)=='USAGE_VAR'),
                       which(colnames(dat)=='ELEC'),
                       which(colnames(dat)=='BE_SEGMENT'))])

sum_table = aggregate(dat4$USAGE_VAR, 
                      list(dat4$ELEC, 
                           dat4$BE_SEGMENT), 
                      FUN = median)

colnames(sum_table) = c(names(dat4)[2:length(names(dat4))], 'AVG_USAGE_VAR_SEG_UTIL')

dat <- sqldf("SELECT L.*, R.AVG_USAGE_VAR_SEG_UTIL
              FROM dat as L
              LEFT JOIN sum_table as R
              on L.ELEC = R.ELEC
              AND L.BE_SEGMENT = R.BE_SEGMENT")

dat$USAGE_VAR= as.numeric(dat$USAGE_VAR)
dat$AVG_USAGE_VAR_SEG_UTIL = as.numeric(dat$AVG_USAGE_VAR_SEG_UTIL)

dat$USAGE_VAR <- if_else(is.na(dat$USAGE_VAR), dat$AVG_USAGE_VAR_SEG_UTIL, dat$USAGE_VAR)

summary(dat$USAGE_VAR)

# hist(log(dat$AVG_YEARLY_CONSUMPTION) - mean(log(dat$AVG_YEARLY_CONSUMPTION),na.rm = T), breaks = 200)

dat$AVG_YEARLY_LOG_CONSUMP = log(dat$AVG_YEARLY_CONSUMPTION) - log(dat$AVG_CONSUM_SEG_UTIL)
# this normalises the usage for each segmnet SME & Enterprise

hist(dat$AVG_YEARLY_LOG_CONSUMP, breaks = 200)

# dat$AVG_YEARLY_LOG_CONSUMP = dat$AVG_YEARLY_LOG_CONSUMP - mean(dat$AVG_YEARLY_LOG_CONSUMP)
# hist(dat$AVG_YEARLY_LOG_CONSUMP, breaks = 200)

# hist(log(dat$USAGE_VAR), breaks = 200)
dat$CONSUM_LOG_VAR = log(dat$USAGE_VAR)
hist(dat$CONSUM_LOG_VAR, breaks = 200)

#dat$TENURE_3MTHS = dat$TENURE_3MTHS -median(dat$TENURE_3MTHS)
#hist(dat$TENURE_3MTHS, breaks = 200)

# hist(dat$YR_ACQ, breaks = 100)
# hist(dat$YR_ACQ_F, breaks = 100)

# dat$Ten_Yr_Acq = dat$TENURE_3MTHS * dat$YR_ACQ_QTR

dat2 = dat

str(dat2)

dat = dat[,-c(which(colnames(dat)=='SALES_AGENT_SID'),
              which(colnames(dat)=='MONTH_NAME'),
              which(colnames(dat)=='TENURE_DAYS'),
              which(colnames(dat)=='TENURE_DAYS_3M'),
              which(colnames(dat)=='AVG_YEARLY_CONS_BIN'),
              which(colnames(dat)=='AVG_YEARLY_CONSUMPTION'),
              which(colnames(dat)=='AVG_USAGE_VAR_SEG_UTIL'),
              which(colnames(dat)=='USAGE_VAR'),
              which(colnames(dat)=='AVG_ANNUAL_USAGE'),
              which(colnames(dat)=='AVG_CONSUM_SEG_UTIL'),
              which(colnames(dat)=='AVG_CONSUM_VAR'),
              which(colnames(dat)=='TRI_MONTHLY_CON_LOSS'),
              which(colnames(dat)=='TRI_MONTHLY_CON_LOSS_F'),
              which(colnames(dat)=='LOSS_YRS_AGO'),
              which(colnames(dat)=='LOSS_YRS_AGO_F'))]

# write.csv(dat, "BE_Churn_data_PRD_29Jan.csv")

# dat <- read.csv("BE_Churn_data_PRD_29Jan.csv", header = T)
# dat <- dat[,- which(colnames(dat)=='X')]

# dat = read.csv("BE_Churn_data_UAT.csv", header = T)
# dat5 = dat[,-c(seq(1,12), 15,16,seq(20,32))]
# table( dat5$CONTRACT_END_DATE[dat5$IN_CONTRACT==1])

# dat$TENURE_MTHS = dat$TENURE_DAYS *(12/365.25)
# convert to factors
dat$ROI = as.factor(dat$ROI)
dat$ELEC = as.factor(dat$ELEC)
#dat$CREDIT = as.factor(dat$CREDIT)
dat$EBILL = as.factor(dat$EBILL)
dat$DD_PAY = as.factor(dat$DD_PAY)
dat$BE_SEGMENT = as.factor(dat$BE_SEGMENT)
dat$CREDIT_WEIGHTING = as.integer(dat$CREDIT_WEIGHTING)
#dat$CREDIT_WEIGHTING = as.integer(dat$CREDIT_WEIGHTING)
# dat$SALES_AGENT_TYPE = as.factor(dat$SALES_AGENT_TYPE)
dat$MARKETING_OPT_IN = as.factor(dat$MARKETING_OPT_IN)
dat$CITY = as.factor(dat$CITY)
dat$COUNTY = as.factor(dat$COUNTY)
# dat$MONTH_NAME = as.factor(dat$MONTH_NAME)
dat$SALES_AGENT_TYPE = as.factor(dat$SALES_AGENT_TYPE)
dat$INDUSTRY = as.factor(dat$INDUSTRY)
# dat$AVG_YEARLY_CONS_BIN = as.factor(dat$AVG_YEARLY_CONS_BIN)
# dat$YR_ACQ = as.factor(dat$YR_ACQ)
dat$STATUS_TEST = as.numeric(dat$STATUS_TEST)
dat$STATUS = as.numeric(dat$STATUS)
# dat$IN_CONTRACT = as.factor(dat$IN_CONTRACT)
# dat$IN_CONTRACT_3M = as.factor(dat$IN_CONTRACT_3M)
dat$CONTRACT_TYPE = as.factor(dat$CONTRACT_TYPE)
dat$CUSTOMER_ID = as.integer(dat$CUSTOMER_ID)
dat$PREMISE_ID = as.integer(dat$PREMISE_ID)
# dat$MPRN = as.integer(dat$MPRN)
# dat$CONTRACT_END_FLAG = as.factor(dat$CONTRACT_END_FLAG)
# dat$CONTRACT_END_FLAG_F = as.factor(dat$CONTRACT_END_FLAG_F)
dat$TRI_MONTHLY_CONTRACT_END = as.integer(dat$TRI_MONTHLY_CONTRACT_END)
dat$TRI_MONTHLY_CONTRACT_END_F = as.integer(dat$TRI_MONTHLY_CONTRACT_END_F)
dat$TENURE_3MTHS = as.integer(dat$TENURE_3MTHS)
dat$TENURE_3MTHS_F = as.integer(dat$TENURE_3MTHS_F)
dat$SF_DF_FLAG = as.factor(dat$SF_DF_FLAG)
dat$CONTRACT_END_DATE = as.character(dat$CONTRACT_END_DATE)
dat$CON_END = as.factor(dat$CON_END)
dat$CON_END_F = as.factor(dat$CON_END_F)
dat$ACQ_SAME_CUST_BEFORE = as.factor(dat$ACQ_SAME_CUST_BEFORE)
dat$ACQ_SAME_METER_BEFORE = as.factor(dat$ACQ_SAME_METER_BEFORE)

# summary(dat$USAGE_VAR)
# dat$VAR_BIN <- cut(dat$USAGE_VAR, 
#                    breaks = c(0, 0.75, 1, 1.4, 2, Inf), 
#                    labels = c( '0.75', '1', '1.4', '2', '2+')) 
#
# dat$USAGE_VENTILE = (ecdf(dat$AVG_YEARLY_LOG_CONSUMP)(dat$AVG_YEARLY_LOG_CONSUMP)* 1000) %/% 50
# dat$USAGE_VENTILE [dat$USAGE_VENTILE == 20] = 19
# dat$USAGE_VENTILE = as.factor(dat$USAGE_VENTILE + 1)
# 
# dat$USAGE_VAR_VENTILE = (ecdf(dat$CONSUM_LOG_VAR)(dat$CONSUM_LOG_VAR)*1000) %/% 50
# dat$USAGE_VAR_VENTILE [dat$USAGE_VAR_VENTILE == 20] = 19
# dat$USAGE_VAR_VENTILE = as.factor(dat$USAGE_VAR_VENTILE + 1)
# 
# dat = dat[,-c(which(colnames(dat) == 'AVG_YEARLY_LOG_CONSUMP'),which(colnames(dat) == 'CONSUM_LOG_VAR'))]

dat$CONSUM_ABS = abs(dat$AVG_YEARLY_LOG_CONSUMP)
dat$CONSUM_SQR = dat$AVG_YEARLY_LOG_CONSUMP ^ 2
dat$CONSUM_CUB = abs(dat$AVG_YEARLY_LOG_CONSUMP ^ 3)

dat$TRI_MONTHLY_CONTRACT_END_ABS = abs(dat$TRI_MONTHLY_CONTRACT_END)
dat$TRI_MONTHLY_CONTRACT_END_ABS_F = abs(dat$TRI_MONTHLY_CONTRACT_END_F)

table(dat$TRI_MONTHLY_CONTRACT_END, useNA = 'ifany')
dat$TRI_MONTHLY_CONTRACT_END [ dat$TRI_MONTHLY_CONTRACT_END > 28] = 28
table(dat$TRI_MONTHLY_CONTRACT_END_F, useNA = 'ifany')
dat$TRI_MONTHLY_CONTRACT_END_F [ dat$TRI_MONTHLY_CONTRACT_END_F > 28] = 28
dat$TRI_MONTHLY_CONTRACT_END_SQR = dat$TRI_MONTHLY_CONTRACT_END ^ 2
dat$TRI_MONTHLY_CONTRACT_END_SQR_F = dat$TRI_MONTHLY_CONTRACT_END_F ^ 2

dat$TRI_MONTHLY_CONTRACT_END_CUB = abs(dat$TRI_MONTHLY_CONTRACT_END ^ 3)
dat$TRI_MONTHLY_CONTRACT_END_CUB_F = abs(dat$TRI_MONTHLY_CONTRACT_END_F ^ 3)

# dat = dat[,-c(which(colnames(dat) == 'TRI_MONTHLY_CONTRACT_END'),
#               which(colnames(dat) == 'TRI_MONTHLY_CONTRACT_END_F'))]


str(dat)

dat = dat[(is.finite(dat$AVG_YEARLY_LOG_CONSUMP) & is.finite(dat$CONSUM_LOG_VAR)),]




# find factors that have fewer than 50 distinct records
loop = 0
while(loop < 2) { 
  # go through this loop twice
  # the first loop can either remove rows where there are fewer than 100 'UNKN' values for a factor
  # or replace values for factors with 'UNKN' where there are fewer than 100 values
  # then on the second pass give an opportunity to examine again the number of 'UNKN' to see if we're above threshold
  
  
  ii = dim(dat)[2]
  indx = as.list (rep(0,ii))
  
  for (i in (1:ii)) { # for each column
    if(is.factor(dat[,i])) { # if it's a factor
      jj = dim(table(dat[,i])) # get the # of levels
      for (j in (1:jj)) { # for each level
        if (table(dat[,i]) [j] < 50) { # if the count is fewer than 100
          if (indx [[i]] [1] == 0) {indx[[i]] = j} # record the low count level indexes in a list
          else indx [[i]] = c(indx[[i]], j) }  
      } # next j
    } # close the if statement
  } # next i
  
  # indx
  dat3 = dat
  
  # replace the low frequency factors with 'UNKN' and remove the level
  for (i in (1:ii)) {
    if (indx[[i]] [1] > 0) {
      for (j in (1:length(indx[[i]]))) {
        # if only one level is lower than threshold and if it's value is 'UNKN'
        if (length(indx[[i]]) == 1 & levels(dat[,i]) [indx[[i]] [j]] == 'UNKN') {
          dat = dat[-which(dat[,i] == 'UNKN'),]
          dat[,i] <- factor(dat[,i])
        }
        # otherwise replace the value of the level with 'UNKN'
        else {
          if(loop == 0 & j == 1) {levels(dat[,i]) = c(levels(dat[,i]),'UNKN')} # add the level 'UNKN' to the factor
          dat[which((dat[,i]  == levels(dat3[,i]) [indx[[i]] [j] ])),i] = 'UNKN' # change the value to 'UNKN'
          dat[,i] <- factor(dat[,i])
        } # close the else statement
      } # next j
    }# close the if statement
  } # next i
  
  loop = loop + 1
}# next loop

# # make sure the test features are the same as the training features
# # this is important for the Tri_monthly_contract_end feature
# 
# l1 = levels(dat$TRI_MONTHLY_CONTRACT_END_F)
# l2 = levels(dat$TRI_MONTHLY_CONTRACT_END)
# l3 = setdiff(l1,l2) # those factor levels in the test feature not in the training feature
# l3
# 
# # remove unused levels
# dat$TRI_MONTHLY_CONTRACT_END_F <- factor(dat$TRI_MONTHLY_CONTRACT_END_F)

cust_indx = dat$CUSTOMER_ID
cust_prem = dat$PREMISE_ID
cust_mprn = dat$MPRN
cust_con_end = as.character(dat$CONTRACT_END_DATE)
cust_status = dat$STATUS_TEST
cust_util_type = dat$ELEC

dat = dat[,-which(colnames(dat)=='MAX_TO_MEAN_READ_RATIO')]

dat1 = dat # take a backup of dat

# remove the future contract_end 
dat = dat[,-c(which(colnames(dat)=='CUSTOMER_ID'),
              which(colnames(dat)=='PREMISE_ID'),
              which(colnames(dat)=='MPRN'),
              which(colnames(dat)=='IN_CONTRACT_3M'),
              which(colnames(dat)=='CONTRACT_END_DATE'),
              which(colnames(dat)=='CONTRACT_END_FLAG_F'),
              which(colnames(dat)=='TRI_MONTHLY_CONTRACT_END_F'),
              which(colnames(dat)=='TRI_MONTHLY_CONTRACT_END_ABS_F'),
              which(colnames(dat)=='TRI_MONTHLY_CONTRACT_END_SQR_F'),
              which(colnames(dat)=='TRI_MONTHLY_CONTRACT_END_CUB_F'),
              which(colnames(dat)=='TRI_MONTHLY_CON_LOSS_F'),
              which(colnames(dat)=='CON_END_F'),
              which(colnames(dat)=='STATUS_TEST'),
              which(colnames(dat)=='TENURE_3MTHS_F'),
              which(colnames(dat)=='YR_ACQ_F'),
              which(colnames(dat)=='ACQUISITION_DATE'),
              which(colnames(dat)== 'LOSS_DATE'),
              which(colnames(dat)== 'EAC'))] 

str(dat)

############################################################

# Machine Learning

############################################################

# convert remaining char to factor
dat = dat %>% mutate_if(is.character, as.factor)
dat$CREDIT_WEIGHTING = ifelse(is.na(dat$CREDIT_WEIGHTING), 0, dat$CREDIT_WEIGHTING)

# dat3 = dat[is.finite(dat$YR_ACQ),]
# dat = dat[,-c(which(colnames(dat)=='TENURE_MTHS'))]

N <- nrow(dat)
M <- ncol(dat)

# set the random number seed 
set.seed(1000)

iterlim <- 1



# prepare two dataframes one for those out of contract and one for those in contract
# include those on horizon contracts as the contract end date is not the true contract end date

dat4 = dat[is.na(dat$TRI_MONTHLY_CONTRACT_END_SQR) | dat$HORIZON_CONTRACT_FLAG == 1,] # no contract end date (already out of contract)

# remove contract date specific factors for those out of contract
dat4 = dat4[,-c(which(colnames(dat)=='IN_CONTRACT'),
                which(colnames(dat)=='CONTRACT_END_FLAG'),
                which(colnames(dat)=='CON_END'),
                which(colnames(dat)=='TRI_MONTHLY_CONTRACT_END'),
                which(colnames(dat)=='TRI_MONTHLY_CONTRACT_END_ABS'),
                which(colnames(dat)=='TRI_MONTHLY_CONTRACT_END_SQR'),
                which(colnames(dat)=='TRI_MONTHLY_CONTRACT_END_CUB'))]

dat44 = dat1[is.na(dat1$TRI_MONTHLY_CONTRACT_END_SQR) | dat1$HORIZON_CONTRACT_FLAG == 1,] # dataset with no contract info

dat44 = dat44[,-c(which(colnames(dat1)=='IN_CONTRACT'),
                  which(colnames(dat1)=='CONTRACT_END_FLAG'),
                  which(colnames(dat1)=='CON_END'),
                  which(colnames(dat1)=='TRI_MONTHLY_CONTRACT_END'),
                  which(colnames(dat1)=='TRI_MONTHLY_CONTRACT_END_ABS'),
                  which(colnames(dat1)=='TRI_MONTHLY_CONTRACT_END_SQR'),
                  which(colnames(dat1)=='TRI_MONTHLY_CONTRACT_END_CUB'))]

dat5 = dat[!is.na(dat$TRI_MONTHLY_CONTRACT_END_SQR) & dat$HORIZON_CONTRACT_FLAG == 0,] # with contract end date, in contract

dat55 = dat1[!is.na(dat1$TRI_MONTHLY_CONTRACT_END_SQR) & dat1$HORIZON_CONTRACT_FLAG == 0,]

# l1 = levels(dat1$TRI_MONTHLY_CONTRACT_END_SQR_F)
# l2 = levels(dat$TRI_MONTHLY_CONTRACT_END_SQR)
# l3 = setdiff(l1,l2)
# l3
# 
# iii = dim(dat1) [1]
# jjj = length(l3)
# if (jjj > 0 ) {
#   for (i in (1:iii)) {
#     for (j in (1:jjj)) {
#       if(dat1$TRI_MONTHLY_CONTRACT_END_SQR_F[i] == l3[j] & !is.na(dat1$TRI_MONTHLY_CONTRACT_END_SQR_F[i])) {
#         levels(dat1$TRI_MONTHLY_CONTRACT_END_SQR_F) <- c(levels(dat1$TRI_MONTHLY_CONTRACT_END_SQR_F),"UNKN")
#         dat1$TRI_MONTHLY_CONTRACT_END_SQR_F[i] = 'UNKN'}
#     } # next j
#   } # next i
# }
# dat1$TRI_MONTHLY_CONTRACT_END_SQR_F <- factor(dat1$TRI_MONTHLY_CONTRACT_END_SQR_F)
# 
# l4 = setdiff(l2,l1)
# l4
# 
# iii = dim(dat) [1]
# jjj = length(l4)
# if (jjj > 0 ) {
#   for (i in (1:iii)) {
#     for (j in (1:jjj)) {
#       if(dat$TRI_MONTHLY_CONTRACT_END_SQR[i] == l4[j] & !is.na(dat1$TRI_MONTHLY_CONTRACT_END_SQR_F[i])) {
#         levels(dat$TRI_MONTHLY_CONTRACT_END_SQR) <- c(levels(dat$TRI_MONTHLY_CONTRACT_END_SQR),"UNKN")
#         dat$TRI_MONTHLY_CONTRACT_END_SQR[i] = 'UNKN'}
#     } # next j
#   } # next i
# }
# dat$TRI_MONTHLY_CONTRACT_END_SQR <- factor(dat$TRI_MONTHLY_CONTRACT_END_SQR)

# str(dat4)

# make corrections to contract type, replace NA's with 'UNKN' 
levels(dat$CONTRACT_TYPE) <- c(levels(dat$CONTRACT_TYPE),"UNKN", "HORIZON")
dat$CONTRACT_TYPE[dat$HORIZON_CONTRACT_FLAG == 1] = 'HORIZON'
dat$CONTRACT_TYPE[is.na(dat$CONTRACT_TYPE)] = 'UNKN'
table(dat$CONTRACT_TYPE, useNA = 'ifany')
table(dat$CONTRACT_TYPE, dat$HORIZON_CONTRACT_FLAG, useNA = 'ifany')
dat$CONTRACT_TYPE <- factor(dat$CONTRACT_TYPE) # remove unused factors
summary(dat)

levels(dat4$CONTRACT_TYPE) <- c(levels(dat4$CONTRACT_TYPE),"UNKN", "HORIZON")
dat4$CONTRACT_TYPE[dat4$HORIZON_CONTRACT_FLAG == 1] = 'HORIZON'
dat4$CONTRACT_TYPE[is.na(dat4$CONTRACT_TYPE)] = 'UNKN'
table(dat4$CONTRACT_TYPE, useNA = 'ifany')
table(dat4$CONTRACT_TYPE, dat4$HORIZON_CONTRACT_FLAG, useNA = 'ifany')
dat4$CONTRACT_TYPE <- factor(dat4$CONTRACT_TYPE) # remove unused factors
summary(dat4)

levels(dat5$CONTRACT_TYPE) <- c(levels(dat5$CONTRACT_TYPE),"UNKN", "HORIZON")
dat5$CONTRACT_TYPE[dat5$HORIZON_CONTRACT_FLAG == 1] = 'HORIZON'
dat5$CONTRACT_TYPE[is.na(dat5$CONTRACT_TYPE)] = 'UNKN'
table(dat5$CONTRACT_TYPE, useNA = 'ifany')
table(dat5$CONTRACT_TYPE, dat5$HORIZON_CONTRACT_FLAG, useNA = 'ifany')
dat5$CONTRACT_TYPE <- factor(dat5$CONTRACT_TYPE) # remove unused factors
# dat5$TRI_MONTHLY_CONTRACT_END_SQR = as.factor(dat5$TRI_MONTHLY_CONTRACT_END_SQR)
# dat5$TRI_MONTHLY_CON_LOSS = as.factor(dat5$TRI_MONTHLY_CONTRACT_END_SQR)
summary(dat5)

# find factors that have fewer than 50 distinct records for those out of contract
loop = 0
while(loop < 2) { 
  # go through this loop twice
  # the first loop can either remove rows where there are fewer than 50 'UNKN' values for a factor
  # or replace values for factors with 'UNKN' where there are fewer than 50 values
  # then on the second pass give an opportunity to examine again the number of 'UNKN' to see if we're above threshold
  
  ii = dim(dat4)[2]
  indx = as.list (rep(0,ii))
  
  for (i in (1:ii)) { # for each column of the dataframe
    if(is.factor(dat4[,i])) {
      jj = dim(table(dat4[,i]))
      for (j in (1:jj)) { # for each level of a factor
        if (table(dat4[,i]) [j] < 50) {
          if (indx [[i]] [1] == 0) {indx[[i]] = j}
          else indx [[i]] = c(indx[[i]], j) }   
      } # next j
    } # close the if statement
  } # next i
  
  # indx is now the index of the factor levels with fewer than 50 entries
  dat3 = dat4 #make a static copy of the dataframe so that indx works even if rows or columns are deleted
  
  # replace the low frequency factor levels with 'UNKN' and remove the level
  for (i in (1:ii)) {
    if (indx[[i]] [1] > 0) {
      for (j in (1:length(indx[[i]]))) {
        # if only one level is lower than threshold and if it's value is 'UNKN'
        if (length(indx[[i]]) == 1 & levels(dat4[,i]) [indx[[i]] [j]] == 'UNKN') {
          indx2 = which(dat4[,i] == 'UNKN') # indx2 is now a row index
          dat4 = dat4[-indx2,]
          dat44 = dat44[-indx2,]
          dat4[,i] <- factor(dat4[,i])
        }
        # otherwise replace the value of the level with 'UNKN'
        else {
          if(loop == 0 & j == 1) { 
            levels(dat4[,i]) = c(levels(dat4[,i]),'UNKN') # add a level 'UNKN' to the factor in dat4
            indx3 = which(colnames(dat44) == colnames(dat4) [i]) # get the col index for dat44
            levels(dat44[,indx3]) = c(levels(dat44[,indx3]),'UNKN')} # add a level 'UNKN' to the factor in dat44
          indx2 = which ( dat4[,i]  == levels(dat3[,i] ) [ indx[[i]] [j] ] ) # get the row indexes
          dat4[indx2,i] = 'UNKN'
          indx3 = which(colnames(dat44) == colnames(dat4) [i])
          dat44[indx2,indx3] = 'UNKN'
          dat4[,i] <- factor(dat4[,i])
          dat44[,i] <- factor(dat44[,i])
        } # close the else statement
      } # next j
    }# close the if statement
  } # next i
  
  loop = loop + 1
}# next loop

# remove columns where there is not more than 1 level for a factor for those out of contract

indx = NULL
for (i in (1:(dim(dat4)[2]) )) {
  if (dim(table(dat4[,i])) < 2) {
    indx = c(indx, i) }
}

if(length(indx) > 0 ) {dat4 = dat4[,-indx] }

indx = NULL
for (i in (1:(dim(dat44)[2]) )) {
  if (dim(table(dat44[,i])) < 2) {
    indx = c(indx, i) }
}

if(length(indx) > 0 ) {dat44 = dat44[,-indx] }



# find factors that have fewer than 50 distinct records for those in contract
loop = 0
while(loop < 2) { 
  # go through this loop twice
  # the first loop can either remove rows where there are fewer than 50 'UNKN' values for a factor
  # or replace values for factors with 'UNKN' where there are fewer than 50 values
  # then on the second pass give an opportunity to examine again the number of 'UNKN' to see if 
  # we're above threshold
  
  
  ii = dim(dat5)[2]
  indx = as.list (rep(0,ii))
  
  for (i in (1:ii)) {
    if(is.factor(dat5[,i])) {
      jj = dim(table(dat5[,i]))
      for (j in (1:jj)) {
        if (table(dat5[,i]) [j] < 50) {
          if (indx [[i]] [1] == 0) {indx[[i]] = j}
          else indx [[i]] = c(indx[[i]], j) }  
      } # next j
    } # close the if statement
  } # next i
  
  # indx
  dat3 = dat5
  
  # replace the low frequency factors with 'UNKN' and remove the level
  for (i in (1:ii)) {
    if (indx[[i]] [1] > 0) {
      for (j in (1:length(indx[[i]]))) {
        # if only one level is lower than threshold and if it's value is 'UNKN'
        if (length(indx[[i]]) == 1 & levels(dat5[,i]) [indx[[i]] [j]] == 'UNKN') {
          indx2 = which(dat5[,i] == 'UNKN') # row index
          dat5 = dat5[-indx2,]
          dat55 = dat55[-indx2,]
          dat5[,i] <- factor(dat5[,i])
        }
        # otherwise replace the value of the level with 'UNKN'
        else {
          if(loop == 0 & j == 1) {
            levels(dat5[,i]) = c(levels(dat5[,i]),'UNKN')
            indx3 = which(colnames(dat55) == colnames(dat5) [i])
            levels(dat55[,indx3]) = c(levels(dat55[,indx3]),'UNKN')}
          indx2 = which ( dat5[,i]  == levels(dat3[,i] ) [ indx[[i]] [j] ] )
          dat5[indx2,i] = 'UNKN'
          indx3 = which(colnames(dat55) == colnames(dat5) [i])
          dat55[indx2,indx3] = 'UNKN'
          dat5[,i] <- factor(dat5[,i])
          dat55[,i] <- factor(dat55[,i])
        } # close the else statement
      } # next j
    }# close the if statement
  } # next i
  
  loop = loop + 1
}# next loop

# remove columns where there is not more than 1 level for a factor for those in contract

# for those in contract
indx = NULL
for (i in (1:(dim(dat5)[2]) )) {
  if (dim(table(dat5[,i])) < 2) {
    indx = c(indx, i) }
}

if(length(indx) > 0 ) {dat5 = dat5[,-indx] }

indx = NULL
for (i in (1:(dim(dat55)[2]) )) {
  if (dim(table(dat55[,i])) < 2) {
    indx = c(indx, i) }
}

if(length(indx) > 0 ) {dat55 = dat55[,-indx] }

# get dimensions of finalised dataframes
NN <- nrow(dat4); MM <- ncol(dat4)
NNN <- nrow(dat5); MMM <- ncol(dat5)





# l1 = levels(dat55$TRI_MONTHLY_CONTRACT_END_SQR_F)
# l2 = levels(dat5$TRI_MONTHLY_CONTRACT_END_SQR)
# l3 = setdiff(l1,l2)
# l3
# 
# iii = dim(dat55) [1]
# jjj = length(l3)
# if (jjj > 0 ) {
#   for (i in (1:iii)) {
#     for (j in (1:jjj)) {
#       if(dat55$TRI_MONTHLY_CONTRACT_END_SQR_F[i] == l3[j] | is.na(dat55$TRI_MONTHLY_CONTRACT_END_SQR_F[i])) {
#         levels(dat55$TRI_MONTHLY_CONTRACT_END_SQR_F) <- c(levels(dat55$TRI_MONTHLY_CONTRACT_END_SQR_F),"UNKN")
#         dat55$TRI_MONTHLY_CONTRACT_END_SQR_F[i] = 'UNKN'}
#       } # next j
#     } # next i
# }
# dat55$TRI_MONTHLY_CONTRACT_END_SQR_F <- factor(dat55$TRI_MONTHLY_CONTRACT_END_SQR_F)
# 
# l4 = setdiff(l2,l1)
# l4
# 
# iii = dim(dat5) [1]
# jjj = length(l4)
# if (jjj > 0 ) {
#   for (i in (1:iii)) {
#     for (j in (1:jjj)) {
#       if(dat5$TRI_MONTHLY_CONTRACT_END_SQR[i] == l4[j] | is.na(dat5$TRI_MONTHLY_CONTRACT_END_SQR[i])) {
#         levels(dat5$TRI_MONTHLY_CONTRACT_END_SQR) <- c(levels(dat5$TRI_MONTHLY_CONTRACT_END_SQR),"UNKN")
#         dat5$TRI_MONTHLY_CONTRACT_END_SQR[i] = 'UNKN'}
#     } # next j
#   } # next i
# }
# dat5$TRI_MONTHLY_CONTRACT_END_SQR <- factor(dat5$TRI_MONTHLY_CONTRACT_END_SQR)

# for (iter in 1:iterlim) 
# {
# bootstrap data as training data
# remaining data as test data

indtrain <- sample(floor(N*.8),replace=FALSE)
indtrainb = indtrain * (is.na(dat[indtrain,which(colnames(dat) == 'YR_ACQ')]))
indtrainb[indtrainb == 0] = NA
indtrainb = na.omit(indtrainb)
indtrain = setdiff(indtrain, indtrainb)
indtest <- sample(setdiff(1:N,indtrain))
indtrain <- sort(indtrain[is.finite(dat$YR_ACQ[indtrain])])
indtest <- sort(indtest[is.finite(dat$YR_ACQ[indtest])])

indtrain4 <- sample(floor(NN*.8),replace=FALSE)
indtest4 <- sample(setdiff(1:NN,indtrain4))
indtrain4b = indtrain4 * (is.na(dat[indtrain4,which(colnames(dat4) == 'YR_ACQ')]))
indtrain4b[indtrain4b == 0] = NA
indtrain4b = na.omit(indtrain4b)
indtrain4 = setdiff(indtrain4, indtrain4b)  
indtrain4 <- sort(indtrain4[is.finite(dat4$YR_ACQ[indtrain4])])
indtest4 <- sort(indtest4[is.finite(dat4$YR_ACQ[indtest4])])
indtrain5 <- sample(floor(NNN*.7),replace=FALSE)
indtrain5b = indtrain5 * (is.na(dat[indtrain5,which(colnames(dat5) == 'YR_ACQ')]))
indtrain5b[indtrain5b == 0] = NA
indtrain5b = na.omit(indtrain5b)
indtrain5 = setdiff(indtrain5, indtrain5b)  
indtest5 <- sample(setdiff(1:NNN,indtrain5))
indtrain5 <- sort(indtrain5[is.finite(dat5$YR_ACQ[indtrain5])])
indtest5 <- sort(indtest5[is.finite(dat5$YR_ACQ[indtest5])])

train.data  <- dat[indtrain, ]
train.data4  <- dat4[indtrain4, ]
train.data5  <- dat5[indtrain5, ]

# get the test data
dat1$TENURE_3MTHS_F = as.integer(dat1$TENURE_3MTHS_F) 
dat$TENURE_3MTHS = as.integer(dat$TENURE_3MTHS) 
# dat$TRI_MONTHLY_CONTRACT_END_SQR = as.integer(dat$TRI_MONTHLY_CONTRACT_END_SQR)
# dat1$TRI_MONTHLY_CONTRACT_END_SQR_F =as.integer(dat1$TRI_MONTHLY_CONTRACT_END_SQR_F)

# cast as numeric
dat1$YR_ACQ_F = as.numeric(dat1$YR_ACQ_F)
# cast as factors
dat$CONTRACT_END_FLAG = as.factor(dat$CONTRACT_END_FLAG )
dat1$CONTRACT_END_FLAG_F = as.factor(dat1$CONTRACT_END_FLAG_F )
dat$IN_CONTRACT = as.factor(dat$IN_CONTRACT)
dat1$IN_CONTRACT_3M= as.factor(dat1$IN_CONTRACT_3M)

dat$STATUS = dat1$STATUS_TEST
dat$IN_CONTRACT = if_else(dat$STATUS == 0, dat1$IN_CONTRACT_3M, dat$IN_CONTRACT)
dat$CONTRACT_END_FLAG = if_else(dat$STATUS == 0, dat1$CONTRACT_END_FLAG_F, dat$CONTRACT_END_FLAG)
dat$CON_END = if_else(dat$STATUS == 0, dat1$CON_END_F, dat$CON_END)
dat$TRI_MONTHLY_CONTRACT_END = if_else(dat$STATUS == 0, dat1$TRI_MONTHLY_CONTRACT_END_F, dat$TRI_MONTHLY_CONTRACT_END)
dat$TRI_MONTHLY_CONTRACT_END_ABS = if_else(dat$STATUS == 0, dat1$TRI_MONTHLY_CONTRACT_END_ABS_F, dat$TRI_MONTHLY_CONTRACT_END_ABS)
dat$TRI_MONTHLY_CONTRACT_END_SQR = if_else(dat$STATUS == 0, dat1$TRI_MONTHLY_CONTRACT_END_SQR_F, dat$TRI_MONTHLY_CONTRACT_END_SQR)
dat$TRI_MONTHLY_CONTRACT_END_CUB = if_else(dat$STATUS == 0, dat1$TRI_MONTHLY_CONTRACT_END_CUB_F, dat$TRI_MONTHLY_CONTRACT_END_CUB)

dat$TENURE_3MTHS = if_else(dat$STATUS == 0, dat1$TENURE_3MTHS_F, dat$TENURE_3MTHS)
dat$YR_ACQ = dat1$YR_ACQ_F

test.data = dat[indtest,]

# subset for those without contract end dates
dat4$TENURE_3MTHS = as.integer(dat4$TENURE_3MTHS)
dat44$TENURE_3MTHS_F = as.integer(dat44$TENURE_3MTHS_F)

dat4$STATUS = dat44$STATUS_TEST
dat4$TENURE_3MTHS = if_else(dat4$STATUS == 0, dat44$TENURE_3MTHS_F, dat4$TENURE_3MTHS)

dat44$YR_ACQ_F = as.numeric(dat44$YR_ACQ_F)
dat4$YR_ACQ = as.numeric(dat4$YR_ACQ)
dat4$YR_ACQ = dat44$YR_ACQ_F

indx = NULL
for (i in (1:(dim(dat4)[2]) )) {
  if (dim(table(dat4[,i])) < 2) {
    indx = c(indx, i) }
}

if(length(indx) > 0 ) {dat4 = dat4[,-indx] }

test.data4 <- dat4[indtest4, ]

# subset for those with contract end dates
dat5$TENURE_3MTHS = as.integer(dat5$TENURE_3MTHS)
dat55$TENURE_3MTHS_F = as.integer(dat55$TENURE_3MTHS_F)

# cast as factor
# dat5$TRI_MONTHLY_CONTRACT_END_SQR = as.factor(dat5$TRI_MONTHLY_CONTRACT_END_SQR)
# dat55$TRI_MONTHLY_CONTRACT_END_SQR_F = as.factor(dat55$TRI_MONTHLY_CONTRACT_END_SQR_F)

# cast as factor
dat5$IN_CONTRACT = as.factor(dat5$IN_CONTRACT)
dat55$IN_CONTRACT_3M = as.factor(dat55$IN_CONTRACT_3M)
dat5$CONTRACT_END_FLAG = as.factor(dat5$CONTRACT_END_FLAG)
dat55$CONTRACT_END_FLAG_F = as.factor(dat55$CONTRACT_END_FLAG_F)


# for current customers replace the current factors with their forecasted values next quarter
dat5$STATUS = dat55$STATUS_TEST 
dat5$IN_CONTRACT = if_else(dat5$STATUS == 0, dat55$IN_CONTRACT_3M, dat5$IN_CONTRACT)

# m = max(as.numeric(levels(dat55$TRI_MONTHLY_CONTRACT_END_SQR_F)),as.numeric(levels(dat5$TRI_MONTHLY_CONTRACT_END_SQR)))
# n = min(as.numeric(levels(dat55$TRI_MONTHLY_CONTRACT_END_SQR_F)),as.numeric(levels(dat5$TRI_MONTHLY_CONTRACT_END_SQR)))
# levels(dat55$TRI_MONTHLY_CONTRACT_END_SQR_F) = as.character(seq(n,m,1))
# levels(dat5$TRI_MONTHLY_CONTRACT_END_SQR) = as.character(seq(n,m,1))

dat5$CONTRACT_END_FLAG = if_else(dat5$STATUS == 0, dat55$CONTRACT_END_FLAG_F, dat5$CONTRACT_END_FLAG)
dat5$CON_END = if_else(dat5$STATUS == 0, dat55$CON_END_F, dat5$CON_END)

dat5$TRI_MONTHLY_CONTRACT_END = if_else(dat5$STATUS == 0, dat55$TRI_MONTHLY_CONTRACT_END_F, dat5$TRI_MONTHLY_CONTRACT_END)
dat5$TRI_MONTHLY_CONTRACT_END_ABS = if_else(dat5$STATUS == 0, dat55$TRI_MONTHLY_CONTRACT_END_ABS_F, dat5$TRI_MONTHLY_CONTRACT_END_ABS)
dat5$TRI_MONTHLY_CONTRACT_END_SQR = if_else(dat5$STATUS == 0, dat55$TRI_MONTHLY_CONTRACT_END_SQR_F, dat5$TRI_MONTHLY_CONTRACT_END_SQR)
dat5$TRI_MONTHLY_CONTRACT_END_CUB = if_else(dat5$STATUS == 0, dat55$TRI_MONTHLY_CONTRACT_END_CUB_F, dat5$TRI_MONTHLY_CONTRACT_END_CUB)

dat5$TENURE_3MTHS = if_else(dat5$STATUS == 0, dat55$TENURE_3MTHS_F, dat5$TENURE_3MTHS)

dat55$YR_ACQ_F = as.numeric(dat55$YR_ACQ_F)
dat5$YR_ACQ = as.numeric(dat5$YR_ACQ)
dat5$YR_ACQ = dat55$YR_ACQ_F

# dat5$Ten_Yr_Acq = dat5$TENURE_3MTHS * dat5$YR_ACQ_QTR

indx = NULL
for (i in (1:(dim(dat5)[2]) )) {
  if (dim(table(dat5[,i])) < 2) {
    indx = c(indx, i) }
}

if(length(indx) > 0 ) {dat5 = dat5[,-indx] }

test.data5 <- dat5[indtest5, ]
all.data5 <- dat5


# # Fit a classifier to the training data
# fit.r <- rpart(as.factor(STATUS)~.,
#                data=train.data,
#                subset=indtrain)
# 
# fit.r1 <- rpart(as.factor(STATUS)~.,
#                data=train.data4,
#                subset=indtrain4)
# 
# fit.r2 <- rpart(as.factor(STATUS)~.,
#                data=train.data5,
#                subset=indtrain5)
# 
# # Fit a logistic regression to the training data
# #  fit.l <- multinom(STATUS~., data=dat[,1:M],subset=indtrain, useNA="ifany", maxit = 500)
# fit.l1 <- multinom(STATUS~., 
#                    data=train.data4,
#                    useNA="ifany", 
#                    maxit = 500)
# 
# fit.l2 <- multinom(STATUS~., 
#                    data=train.data5,
#                    useNA="ifany", 
#                    maxit = 500)

# Implement the random forest algorithm
#  fit.rf <- randomForest(STATUS~., data=dat[,1:M], na.action = na.omit, subset=indtrain, mtry=2)
fit.rf1 <- randomForest(as.factor(STATUS)~., 
                        data=train.data4, 
                        na.action = na.omit, 
                        type = 'class', 
                        mtry=4)
fit.rf2 <- randomForest(as.factor(STATUS)~., 
                        data=train.data5, 
                        na.action = na.omit, 
                        type = 'class', 
                        mtry=4)

# lasso, ridge and elastic net regression



# Dummy code categorical predictor variables
x4 <- model.matrix(STATUS~., train.data4)[,-1]
# Convert the outcome (class) to a numerical variable
y4 <- train.data4$STATUS

# fit.lass1a <- glmnet(x, y, family = "binomial", alpha = 1, lambda = NULL)
# fit.lass1b <- glmnet(x, y, family = "binomial", alpha = 0, lambda = NULL)
# fit.lass1n <- glmnet(x, y, family = "binomial", alpha = .3, lambda = NULL)

# cv.lasso4 <- cv.glmnet(x4, y4, alpha = 1, family = "binomial") # very slow! 
# plot(cv.lasso4)
# cv.lasso4$lambda.min
# cv.lasso4$lambda.1se
# coef(cv.lasso4, cv.lasso4$lambda.min)
# coef(cv.lasso4, cv.lasso4$lambda.1se)
#   
# # Fit the final model on the training data
#    model4 <- glmnet(x4, y4, alpha = 1, family = "binomial",lambda = cv.lasso4$lambda.1se)
# #  model4 <- glmnet(x4, y4, alpha = 1, family = "binomial",lambda = cv.lasso4$lambda.min)
# #  model4 <- glmnet(x4, y4, alpha = 1, family = "binomial",lambda = exp(-6))
# 
# # Display regression coefficients
#  coef(model4)
# Make predictions on the test data
# x.test4 <- model.matrix(STATUS ~., test.data4)[,-1]
# probabilities4 <- model4 %>% predict(newx = x.test4)
# predicted.classes <- ifelse(probabilities4 > 0, 1, 0)
# # Model accuracy
# observed.classes <- test.data4$STATUS
# mean(predicted.classes == observed.classes)
# [1] 0.9770791, lambda = cv.lasso$lambda.min
# [1] 0.9770791, lambda = cv.lasso$lambda.1se
# [1] 0.9518781, lambda = exp(-6)

# for (i in 0:20) {
#   model4 <- glmnet(x4, y4, alpha = 1, family = "binomial",lambda = exp(-8 + (i/4)))
#   probabilities4 <- model4 %>% predict(newx = x.test4)
#   probabilities4.true <- exp(probabilities4)/(1+exp(probabilities4))
#   predicted.classes <- ifelse(probabilities4 > 0, 1, 0)
#   tab.las4 = table(observed.classes, predicted.classes)
#   mcc_las4 = ((tab.las4[2,2]*tab.las4[1,1]) - (tab.las4[1,2]* tab.las4[2,1])) / (sqrt((tab.las4[2,2] + tab.las4[1,2])) * sqrt((tab.las4[2,2] + tab.las4[2,1])) * sqrt((tab.las4[1,1] + tab.las4[1,2])) * sqrt((tab.las4[1,1] + tab.las4[2,1])))
#       # Model accuracy
#   cat('\n', 'log(lambda) = ', i/4 - 8)
#   cat('\t', 'Accuracy = ', round(mean(predicted.classes == observed.classes),4))
#   cat('\t', 'Matthews Correlation Coefficient = ', round(mcc_las4,4))
#   cat('\t', 'Count = ', sum(probabilities4.true[test.data4$STATUS == 0 & test.data4$AVG_YEARLY_LOG_CONSUMP >=0]>0.5) )
# }

# log(lambda) =  -8	      Accuracy =  0.9278	 Matthews Correlation Coefficient =  0.8634	 Count =  2
# log(lambda) =  -7.75	  Accuracy =  0.9266	 Matthews Correlation Coefficient =  0.8616	 Count =  1
# log(lambda) =  -7.5	    Accuracy =  0.9247	 Matthews Correlation Coefficient =  0.8584	 Count =  1
# log(lambda) =  -7.25	  Accuracy =  0.9247	 Matthews Correlation Coefficient =  0.8584	 Count =  1
# log(lambda) =  -7	      Accuracy =  0.9229	 Matthews Correlation Coefficient =  0.8555	 Count =  1
# log(lambda) =  -6.75	  Accuracy =  0.9223	 Matthews Correlation Coefficient =  0.8545	 Count =  1
# log(lambda) =  -6.5	    Accuracy =  0.9223	 Matthews Correlation Coefficient =  0.8551	 Count =  0
# log(lambda) =  -6.25	  Accuracy =  0.9205	 Matthews Correlation Coefficient =  0.852	 Count =  0
# log(lambda) =  -6	      Accuracy =  0.9157	 Matthews Correlation Coefficient =  0.8436	 Count =  0
# log(lambda) =  -5.75	  Accuracy =  0.9115	 Matthews Correlation Coefficient =  0.8363	 Count =  0
# log(lambda) =  -5.5	    Accuracy =  0.9067	 Matthews Correlation Coefficient =  0.8281	 Count =  0
# log(lambda) =  -5.25	  Accuracy =  0.9025	 Matthews Correlation Coefficient =  0.8209	 Count =  0
# log(lambda) =  -5	      Accuracy =  0.9007	 Matthews Correlation Coefficient =  0.8178	 Count =  0
# log(lambda) =  -4.75	  Accuracy =  0.8995	 Matthews Correlation Coefficient =  0.8154	 Count =  0
# log(lambda) =  -4.5	    Accuracy =  0.8964	 Matthews Correlation Coefficient =  0.8099	 Count =  0
# log(lambda) =  -4.25	  Accuracy =  0.8922	 Matthews Correlation Coefficient =  0.8024	 Count =  0
# log(lambda) =  -4	      Accuracy =  0.8874	 Matthews Correlation Coefficient =  0.793	 Count =  0
# log(lambda) =  -3.75	  Accuracy =  0.891	   Matthews Correlation Coefficient =  0.7983	 Count =  1
# log(lambda) =  -3.5	    Accuracy =  0.8868	 Matthews Correlation Coefficient =  0.7858	 Count =  5
# log(lambda) =  -3.25	  Accuracy =  0.8868	 Matthews Correlation Coefficient =  0.7815	 Count =  9
# log(lambda) =  -3	      Accuracy =  0.8772	 Matthews Correlation Coefficient =  0.7557	 Count =  30

# Final model with lambda = exp(-4)

#model4 <- glmnet(x4, y4, alpha = 1, family = "binomial",lambda = exp(-5))
model4 <- glmnet(x4, y4, alpha = 1, family = "binomial", lambda = exp(-5), maxit = 100000)

x.test4 <- model.matrix(STATUS ~., test.data4)[,-1]
probabilities4 <- model4 %>% predict(newx = x.test4)
probabilities4.true <- exp(probabilities4)/(1+exp(probabilities4))
predicted.classes <- ifelse(probabilities4 > 0, 1, 0)

x.test4.all <- model.matrix(STATUS ~., dat4)[,-1]
probabilities4.all <- model4 %>% predict(newx = x.test4.all)
probabilities4.all.true <-exp(probabilities4.all)/(1 + exp(probabilities4.all) ) # this converts from log-odds to true probabilities


###############################################

# lasso for customers with contract end dates

###############################################


# Dummy code categorical predictor variables
x5 <- model.matrix(STATUS~., train.data5)[,-1]
# Convert the outcome (class) to a numerical variable
y5 <- train.data5$STATUS

# fit.lass1a <- glmnet(x, y, family = "binomial", alpha = 1, lambda = NULL)
# fit.lass1b <- glmnet(x, y, family = "binomial", alpha = 0, lambda = NULL)
# fit.lass1n <- glmnet(x, y, family = "binomial", alpha = .3, lambda = NULL)

# cv.lasso5 <- cv.glmnet(x5, y5, alpha = 1, family = "binomial")
# plot(cv.lasso5)
# cv.lasso5$lambda.min
# cv.lasso5$lambda.1se
# coef(cv.lasso5, cv.lasso5$lambda.min)
# coef(cv.lasso5, cv.lasso5$lambda.1se)

# Fit the final model on the training data
#  model5 <- glmnet(x5, y5, alpha = 1, family = "binomial",lambda = cv.lasso5$lambda.min)
#  model5 <- glmnet(x5, y5, alpha = 1, family = "binomial",lambda = cv.lasso5$lambda.1se)

x.test5 <- model.matrix(STATUS ~., test.data5)[,-1]
observed.classes <- test.data5$STATUS

# for (i in 0:7) {
#   model5 <- glmnet(x5, y5, alpha = 1, family = "binomial",lambda = exp(-5.5 + (i/4)))
#   probabilities5 <- model5 %>% predict(newx = x.test5)
#   probabilities5.true <- exp(probabilities5)/(1+exp(probabilities5))
#   predicted.classes <- ifelse(probabilities5 > 0, 1, 0)
#   tab.las5 = table(observed.classes, predicted.classes)
#   mcc_las5 = ((tab.las5[2,2]*tab.las5[1,1]) - (tab.las5[1,2]* tab.las5[2,1])) / (sqrt((tab.las5[2,2] + tab.las5[1,2])) * sqrt((tab.las5[2,2] + tab.las5[2,1])) * sqrt((tab.las5[1,1] + tab.las5[1,2])) * sqrt((tab.las5[1,1] + tab.las5[2,1])))
#   # Model accuracy
#   cat('\n', 'log(lambda) = ', i/4 - 5.5)
#   cat('\t', 'Accuracy = ', round(mean(predicted.classes == observed.classes),4))
#   cat('\t', 'Matthews Correlation Coefficient = ', round(mcc_las5,4))
#   cat('\t', 'Count = ', sum(probabilities5.true[test.data5$STATUS == 0 & test.data5$AVG_YEARLY_LOG_CONSUMP >=0]>0.5)  )
#   
#   x.test5.all <- model.matrix(STATUS ~., all.data5)[,-1]
#   probabilities.all <- model5 %>% predict(newx = x.test5.all)
#   
#   plot(as.Date(dat55$CONTRACT_END_DATE[dat5$STATUS == 0 & dat5$AVG_YEARLY_LOG_CONSUMP < 0]),
#        exp(probabilities.all[dat5$STATUS == 0 & dat5$AVG_YEARLY_LOG_CONSUMP < 0])/(1+exp(probabilities.all[dat5$STATUS == 0 & dat5$AVG_YEARLY_LOG_CONSUMP < 0])),
#        ylab = 'Churn Propensity',
#        xlim = c(as.Date('2017-01-01'), as.Date('2021-01-01')),
#        # ylim = c(0,0.6),
#        pch = 16,
#        col = 1,
#        xlab = 'Contract End Date',
#        main = paste0('Lasso Logistic Regression, Log(Lambda) = ', i/4 - 5.5)  )
#   points(x = as.Date(dat55$CONTRACT_END_DATE[dat5$STATUS == 0 & dat5$AVG_YEARLY_LOG_CONSUMP >= 0]),
#          y = exp(probabilities.all[dat5$STATUS == 0 & dat5$AVG_YEARLY_LOG_CONSUMP >=0])/(1+exp(probabilities.all[dat5$STATUS == 0 & dat5$AVG_YEARLY_LOG_CONSUMP >= 0])),
#          col = 2,
#          pch = 16)
#   abline(h= 0.5, col=2)
#   legend('topleft', legend = (c('usage below median', 'usage above median')),  pch =c(16, 16) , col = c(1,2))
# }


# log(lambda) =  -5.5	    Accuracy =  0.9431	 Matthews Correlation Coefficient =  0.8728	 Count =  5
# log(lambda) =  -5.25	  Accuracy =  0.9433	 Matthews Correlation Coefficient =  0.8731	 Count =  7
# log(lambda) =  -5	      Accuracy =  0.9434	 Matthews Correlation Coefficient =  0.8731	 Count =  14
# log(lambda) =  -4.75	  Accuracy =  0.9431	 Matthews Correlation Coefficient =  0.8717	 Count =  27
# log(lambda) =  -4.5	    Accuracy =  0.942	   Matthews Correlation Coefficient =  0.8688	 Count =  43
# log(lambda) =  -4.25	  Accuracy =  0.9381	 Matthews Correlation Coefficient =  0.8588	 Count =  92
# log(lambda) =  -4	      Accuracy =  0.9298	 Matthews Correlation Coefficient =  0.836	 Count =  239
# log(lambda) =  -3.75	  Accuracy =  0.9037	 Matthews Correlation Coefficient =  0.763	 Count =  687

# Final model with lambda = exp(-3.75)

model5 <- glmnet(x5, y5, alpha = 1, family = "binomial",lambda = exp(-5))
probabilities5 <- model5 %>% predict(newx = x.test5)
probabilities5.true <- exp(probabilities5)/(1+exp(probabilities5))
predicted.classes <- ifelse(probabilities5 > 0, 1, 0)

x.test5.all <- model.matrix(STATUS ~., all.data5)[,-1]
probabilities5.all <- model5 %>% predict(newx = x.test5.all)
probabilities5.all.true <-exp(probabilities5.all)/(1 + exp(probabilities5.all) ) # this converts from log-odds to true probabilities

plot(as.Date(dat55$CONTRACT_END_DATE[dat5$STATUS == 0 & dat5$AVG_YEARLY_LOG_CONSUMP < 0]), 
     probabilities5.all.true[dat5$STATUS == 0 & dat5$AVG_YEARLY_LOG_CONSUMP < 0],
     ylab = 'Churn Propensity',
     xlim = c(as.Date('2017-01-01'), as.Date('2021-01-01')),
     ylim = c(0,0.8),
     pch = 16,
     col = 1,
     xlab = 'Contract End Date',
     main = 'Lasso Logistic Regression, log(Lambda) = -5')

points(x = as.Date(dat55$CONTRACT_END_DATE[dat5$STATUS == 0 & dat5$AVG_YEARLY_LOG_CONSUMP >= 0]),
       y = exp(probabilities5.all[dat5$STATUS == 0 & dat5$AVG_YEARLY_LOG_CONSUMP >=0])/(1+exp(probabilities5.all[dat5$STATUS == 0 & dat5$AVG_YEARLY_LOG_CONSUMP >= 0])),
       col = 2,
       pch = 16)

abline(h=0.5, col=2)

legend('topleft', legend = (c('usage below median', 'usage above median')),  pch =c(16, 16) , col = c(1,2))



sum(probabilities5.all.true[dat5$STATUS == 0 & dat5$AVG_YEARLY_LOG_CONSUMP >=0]>0.5)
# 56

tab.las5 = table(observed.classes, predicted.classes)
mcc_las5 = ((tab.las5[2,2]*tab.las5[1,1]) - (tab.las5[1,2]* tab.las5[2,1])) / (sqrt((tab.las5[2,2] + tab.las5[1,2])) * sqrt((tab.las5[2,2] + tab.las5[2,1])) * sqrt((tab.las5[1,1] + tab.las5[1,2])) * sqrt((tab.las5[1,1] + tab.las5[2,1])))
# Model accuracy
cat('\n', 'log(lambda) = ', -5)
cat('\t', 'Accuracy = ', round(mean(predicted.classes == observed.classes),4))
cat('\t', 'Matthews Correlation Coefficient = ', round(mcc_las5,4))
cat('\t', 'Count = ', sum(probabilities5.true[test.data5$STATUS == 0 & test.data5$AVG_YEARLY_LOG_CONSUMP >=0]>0.5)  )

coef(model5) 
# 86 x 1 sparse Matrix of class "dgCMatrix"
# s0
# (Intercept)                                                      3.187882e+03
# ELEC1                                                            .           
# EBILL1                                                           .           
# DD_PAY1                                                          .           
# ROI1                                                             6.645894e-02
# MARKETING_OPT_IN1                                                2.349715e-01
# CITY1                                                            .           
# IN_CONTRACTY                                                     .           
# YR_ACQ                                                          -1.579744e+00
# COUNTYARMAGH                                                     .           
# COUNTYCARLOW                                                     .           
# COUNTYCAVAN                                                      .           
# COUNTYCLARE                                                      .           
# COUNTYCORK                                                       .           
# COUNTYDERRY                                                      .           
# COUNTYDONEGAL                                                    .           
# COUNTYDOWN                                                       .           
# COUNTYDUBLIN                                                     .           
# COUNTYFERMANAGH                                                  .           
# COUNTYGALWAY                                                     .           
# COUNTYKERRY                                                      .           
# COUNTYKILDARE                                                    .           
# COUNTYKILKENNY                                                   .           
# COUNTYLAOIS                                                      .           
# COUNTYLEITRIM                                                    .           
# COUNTYLIMERICK                                                   .           
# COUNTYLONGFORD                                                   .           
# COUNTYLOUTH                                                      .           
# COUNTYMAYO                                                       .           
# COUNTYMEATH                                                      .           
# COUNTYMONAGHAN                                                   .           
# COUNTYOFFALY                                                     .           
# COUNTYROSCOMMON                                                  .           
# COUNTYSLIGO                                                      .           
# COUNTYTIPPERARY                                                  .           
# COUNTYTYRONE                                                     .           
# COUNTYWATERFORD                                                  .           
# COUNTYWESTMEATH                                                  .           
# COUNTYWEXFORD                                                    .           
# COUNTYWICKLOW                                                    .           
# SF_DF_FLAGSF                                                     .           
# SALES_AGENT_TYPEDoor to Door                                     .           
# SALES_AGENT_TYPEHomemoves                                        .           
# SALES_AGENT_TYPEOnline                                           .           
# SALES_AGENT_TYPEProperty Button                                  .           
# SALES_AGENT_TYPEUNKN                                             .           
# BE_SEGMENTSME                                                    .           
# INDUSTRYEducation/Community                                      .           
# INDUSTRYEnvironmental                                            .           
# INDUSTRYFinancial Services                                       .           
# INDUSTRYGovernment                                               .           
# INDUSTRYHealth/Beauty                                            .           
# INDUSTRYHospitality                                              .           
# INDUSTRYMedical                                                  .           
# INDUSTRYProfessional Bodies                                      .           
# INDUSTRYRetail Food                                              .           
# INDUSTRYRetail Non Food                                          .           
# INDUSTRYSports & Leisure                                         .           
# INDUSTRYTrade & Transport                                        .           
# INDUSTRYUNKN                                                     .           
# CREDIT_WEIGHTING                                                 3.881144e-03
# CONTRACT_END_FLAGY                                               2.072347e+00
# TRI_MONTHLY_CONTRACT_END                                        -6.990757e-02
# CONTRACT_TYPEIntroductory Period Discount                        .           
# CONTRACT_TYPESmall/Medium Enterprise Price Eligibility Contract -2.134712e-01
# TENURE_3MTHS                                                    -3.973320e-01
# CNT_SAME_CUST_METER_ACQS                                         .           
# CNT_METER_ACQS                                                   1.841437e-01
# ACQ_SAME_CUST_BEFORE1                                            .           
# ACQ_SAME_METER_BEFORE1                                           .           
# CNT_CONTRACTS                                                   -2.732622e-03
# NUM_CONTRACT_BKT2                                                1.253862e-02
# NUM_CONTRACT_BKT3-4                                              .           
# NUM_CONTRACT_BKT5-8                                              .           
# NUM_CONTRACT_BKT9-16                                            -1.198225e-02
# NUM_CONTRACT_BKT17-32                                            .           
# NUM_CONTRACT_BKT32+                                              .           
# CON_ENDP                                                         8.323370e-01
# AVG_YEARLY_LOG_CONSUMP                                           .           
# CONSUM_LOG_VAR                                                   5.049443e-02
# CONSUM_ABS                                                       .           
# CONSUM_SQR                                                       .           
# CONSUM_CUB                                                       .           
# TRI_MONTHLY_CONTRACT_END_ABS                                     1.440708e-04
# TRI_MONTHLY_CONTRACT_END_SQR                                    -6.658542e-03
# TRI_MONTHLY_CONTRACT_END_CUB                                     .           


# use boosting
#  train.data$STATUS = as.factor(train.data$STATUS)
train.data4$STATUS = as.factor(train.data4$STATUS)
train.data5$STATUS = as.factor(train.data5$STATUS)
# fit.b <- boosting(STATUS~.,
#                   data=train.data,
#                   boos=FALSE,
#                   coeflearn="Freund")
# 
fit.b1 <- boosting(STATUS~.,
                   data=train.data4,
                   boos=FALSE,
                   coeflearn="Freund")

fit.b2 <- boosting(STATUS~.,
                   data=train.data5,
                   boos=FALSE,
                   coeflearn="Freund")

# Implement Bootstrap Aggregating
# fit.bg <- bagging(STATUS~.,
#                   data=train.data,
#                   useNA="no")

# fit.bg1 <- bagging(STATUS~.,
#                   data=train.data4,
#                   useNA="no")
# 
# fit.bg2 <- bagging(STATUS~.,
#                   data=train.data5,
#                   useNA="no")

# Implement Support Vector Machine
#  fit.svm <- ksvm(STATUS~.,data=dat[indtrain,1:M], kernel="rbfdot")  

#length(train.data4)
fit.svm1 <- ksvm(as.factor(STATUS)~.,
                 data=train.data4,
                 type = 'C-svc',
                 kernel="rbfdot",
                 prob.model = T)
fit.svm2 <- ksvm(as.factor(STATUS)~.,
                 data=train.data5,
                 type = 'C-svc',
                 kernel="rbfdot",
                 prob.model = T)

test.data5$STATUS <- as.factor(test.data5$STATUS)


# Classify for the test data observations
# pred.r <- ((predict(fit.r,type="prob",newdata=test.data)[,2] * 100) %/% 5 ) / 20
# pred.r1 <- ((predict(fit.r1,type="prob",newdata=test.data4)[,2] * 100) %/% 5 ) / 20
# pred.r2 <- ((predict(fit.r2,type="prob",newdata=test.data5)[,2] * 100) %/% 5 ) / 20
# pred.l1 <- ((predict(fit.l1,type="prob",newdata=test.data4)* 100) %/% 5 ) / 20
# pred.l2 <- ((predict(fit.l2,type="prob",newdata=test.data5)* 100) %/% 5 ) / 20
pred.rf1 <- ((predict(fit.rf1,type="prob",newdata=test.data4) [,2]* 100) %/% 5 ) / 20
pred.rf2 <- ((predict(fit.rf2,type="prob",newdata=test.data5) [,2]* 100) %/% 5 ) / 20
#  pred.b <- ((predict(fit.b,newdata=test.data,type="prob")$prob[,2]* 100) %/% 5 ) / 20
pred.b1 <- ((predict(fit.b1,newdata=test.data4,type="prob")$prob[,2]* 100) %/% 5 ) / 20
pred.b2 <- ((predict(fit.b2,newdata=test.data5,type="prob")$prob[,2]* 100) %/% 5 ) / 20
#  pred.bg <- ((predict(fit.bg,newdata=test.data,type="prob")$prob[,2]* 100) %/% 5 ) / 20
# pred.bg1 <- ((predict(fit.bg1,newdata=test.data4,type="prob")$prob[,2]* 100) %/% 5 ) / 20
# pred.bg2 <- ((predict(fit.bg2,newdata=test.data5,type="prob")$prob[,2]* 100) %/% 5 ) / 20
pred.svm1 <- ((predict(fit.svm1, test.data4, type="probabilities") [,2]* 100) %/% 5 ) / 20
pred.svm2 <- ((predict(fit.svm2, test.data5, type="probabilities") [,2]* 100) %/% 5 ) / 20


# Classify for the test data observations (not binned)
# preda.r <- predict(fit.r,type="prob",newdata=test.data)[,2] 
# preda.r1 <- predict(fit.r1,type="prob",newdata=test.data4)[,2] 
# preda.r2 <- predict(fit.r2,type="prob",newdata=test.data5)[,2] 
# preda.l1 <- predict(fit.l1,type="prob",newdata=test.data4)
# preda.l2 <- predict(fit.l2,type="prob",newdata=test.data5)
preda.rf1 <- predict(fit.rf1,type="prob",newdata=test.data4) [,2]
preda.rf2 <- predict(fit.rf2,type="prob",newdata=test.data5) [,2]
#  preda.b <- predict(fit.b,newdata=test.data,type="prob")$prob[,2]
preda.b1 <- predict(fit.b1,newdata=test.data4,type="prob")$prob[,2]
preda.b2 <- predict(fit.b2,newdata=test.data5,type="prob")$prob[,2]
#  preda.bg <- predict(fit.bg,newdata=test.data,type="prob")$prob[,2]
# preda.bg1 <- predict(fit.bg1,newdata=test.data4,type="prob")$prob[,2]
# preda.bg2 <- predict(fit.bg2,newdata=test.data5,type="prob")$prob[,2]
preda.svm1 <- predict(fit.svm1, test.data4, type="probabilities") [,2]
preda.svm2 <- predict(fit.svm2, test.data5, type="probabilities") [,2]


# Look at table for the test data only (rows=truth, cols=prediction)
# tab.r <- table(test.data$STATUS,pred.r)
# tab.r        
#   0.15  0.2 0.25  0.3 0.35  0.7  0.8 0.85  0.9 0.95
# 0 1340  394  334 1407  334  270  330   75    0    0
# 1  773  104   89  418  228  231  875  414  725 1197

# tab.r <- table(test.data$STATUS,as.integer(preda.r>0.5))
# tab.r
# #      0    1
# # 0 3809  675
# # 1 1612 3442
# 
# mean(test.data$STATUS==as.integer(preda.r>0.5))
# # [1] 0.8132353
# 
# plot(as.Date(dat1$CONTRACT_END_DATE[indtest]), 
#      pred.r,
#      ylab = 'Churn Propensity',
#      xlab = 'Contract End Date',
#      xlim = c(as.Date('2017-01-01'), as.Date('2021-01-01')),
#      xaxt = 'n',
#      main = 'Decision Tree')
# 
# axis(1, at = c(as.Date('2017-01-01'),
#                as.Date('2018-01-01'),
#                as.Date('2019-01-01'),
#                as.Date('2020-01-01'),
#                as.Date('2021-01-01')),
#      labels= seq(2017, 2021, 1), lty = 1, col=0, las=0)
# 
# tab.r1 <- table(test.data4$STATUS,pred.r1)
# tab.r1
# #     0 0.05 0.15 0.25 0.3 0.65 0.75 0.8 0.9   1
# # 0  70  306   33  221 121   17    6   0  11   0
# # 1   5   51   19  110  68   24   31  52  52 497
# 
# tab.r1 <- table(test.data4$STATUS,as.integer(preda.r1>0.5))
# tab.r1
# #     0   1
# # 0 751  34
# # 1 253 656
# 
# mean(test.data4$STATUS==as.integer(preda.r1>0.5))
# # [1] 0.6720269
# 
# tab.r2 <- table(test.data5$STATUS,pred.r2)
# tab.r2 
# #    0.1  0.2 0.25  0.3 0.65 0.75  0.9 0.95    1
# # 0 2227   54  581 2517  281  456   69    0    0
# # 1 1043   87  228  813  324 1809 2667  103 1363
# 
# tab.r2 <- table(test.data5$STATUS,as.integer(preda.r2>0.5))
# tab.r2
# #      0    1
# # 0 5379  806
# # 1 2171 6266
# 
# mean(test.data5$STATUS==as.integer(preda.r2>0.5))
# # [1] 0.8643275
# 
# indtest5b = indtest5 * (dat55[indtest5,which(colnames(dat55) == 'STATUS_TEST')] == 0)
# indtest5b[indtest5b == 0] = NA
# indtest5b = na.omit(indtest5b)
# 
# plot(as.Date(dat55$CONTRACT_END_DATE[indtest5b]), 
#      preda.r2 [test.data5$STATUS == 0],
#      ylab = 'Churn Propensity',
#      xlab = 'Contract End Date',
#      xlim = c(as.Date('2017-01-01'), as.Date('2021-01-01')),
#      xaxt = 'n',
#      main = 'Decision Tree')
# 
# axis(1, at = c(as.Date('2017-01-01'),
#                as.Date('2018-01-01'),
#                as.Date('2019-01-01'),
#                as.Date('2020-01-01'),
#                as.Date('2021-01-01')),
#      labels= seq(2017, 2021, 1), lty = 1, col=0, las=0)
# 
# tab.l1 <- table(test.data4$STATUS,pred.l1, useNA="ifany")
# tab.l1
# #     0 0.05 0.1 0.15 0.2 0.25 0.4 0.45 0.6 0.65 0.7 0.9 0.95   1
# # 0 785    0   0    0   0    0   0    0   0    0   0   0    0   0
# # 1  70   12  10    5   4    1   2    1   2    1   4   1   45 751
# 
# tab.l1 <- table(test.data4$STATUS,as.integer(preda.l1>0.5), useNA="ifany")
# tab.l1
# #     0   1
# # 0 785   0
# # 1 105 804
# 
# mean(test.data4$STATUS==as.integer(preda.l1>0.5))
# # [1] 0.5985839
# 
# tab.l2 <- table(test.data5$STATUS,pred.l2, useNA="ifany")
# tab.l2
# #      0 0.05  0.1 0.15  0.2 0.25  0.3 0.35  0.4 0.45  0.5 0.55  0.6 0.65  0.7 0.75  0.8 0.85  0.9 0.95    1
# # 0 6185    0    0    0    0    0    0    0    0    0    0    0    0    0    0    0    0    0    0    0    0
# # 1  815  206  143  110   81   52   64   33   21   24   14   10   10    6   13    6    8   13   26 1291 5491
# 
# tab.l2 <- table(test.data5$STATUS,as.integer(preda.l2>0.5), useNA="ifany")
# tab.l2
# #      0    1
# # 0 6185    0
# # 1 1549 6888
# 
# mean(test.data5$STATUS==as.integer(preda.l2>0.5))
# # [1] 0.8479532
# 
# plot(as.Date(dat55$CONTRACT_END_DATE[indtest5b]), 
#      preda.l2 [test.data5$STATUS == 0],
#      ylab = 'Churn Propensity',
#      xlab = 'Contract End Date',
#      xlim = c(as.Date('2017-01-01'), as.Date('2021-01-01')),
#      xaxt = 'n',
#      main = 'Logistic Regression')
# 
# axis(1, at = c(as.Date('2017-01-01'),
#                as.Date('2018-01-01'),
#                as.Date('2019-01-01'),
#                as.Date('2020-01-01'),
#                as.Date('2021-01-01')),
#      labels= seq(2017, 2021, 1), lty = 1, col=0, las=0)

tab.rf1 <- table(test.data4$STATUS,pred.rf1, useNA="ifany")
tab.rf1
#   0.05 0.1 0.15 0.2 0.25 0.3 0.35 0.4 0.45 0.5 0.55 0.6 0.65 0.7 0.75 0.8 0.85 0.9 0.95
# 0   24  68   83 101  120 114   77  67   50  30   19  13    9   7    1   1    1   0    0
# 1    1   7    8  13   13  25   33  30   34  49   44  41   44  67   55 102   97 151   95

tab.rf1 <- table(test.data4$STATUS,as.integer(preda.rf1>0.5), useNA="ifany")
tab.rf1
#     0   1
# 0 705  80
# 1 166 743

mean(test.data4$STATUS==as.integer(preda.rf1>0.5))
# [1] 0.6349454

tab.rf2 <- table(test.data5$STATUS,pred.rf2, useNA="ifany")
tab.rf2
#      0 0.05  0.1 0.15  0.2 0.25  0.3 0.35  0.4 0.45  0.5 0.55  0.6 0.65  0.7 0.75  0.8 0.85  0.9 0.95    1
# 0  339  494  643  588  696  740  631  497  385  313  281  192  123   99   66   49   24   15   10    0    0
# 1  105  105  127  149  149  138  174  182  179  163  182  188  241  266  314  428  592  822 1082 2676  175

tab.rf2 <- table(test.data5$STATUS,as.integer(preda.rf2>0.5), useNA="ifany")
tab.rf2
#      0    1
# 0 5339  846
# 1 1480 6957

mean(test.data5$STATUS==as.integer(preda.rf2>0.5))
# [1] 0.8666667

plot(as.Date(dat55$CONTRACT_END_DATE[indtest5b]), 
     preda.rf2 [test.data5$STATUS == 0],
     ylab = 'Churn Propensity',
     xlab = 'Contract End Date',
     xlim = c(as.Date('2017-01-01'), as.Date('2021-01-01')),
     xaxt = 'n',
     main = 'Random Forest')

axis(1, at = c(as.Date('2017-01-01'),
               as.Date('2018-01-01'),
               as.Date('2019-01-01'),
               as.Date('2020-01-01'),
               as.Date('2021-01-01')),
     labels= seq(2017, 2021, 1), lty = 1, col=0, las=0)

# tab.b <- table(test.data$STATUS,pred.b)
# tab.b
#   0.05 0.1 0.15 0.2 0.25 0.3 0.35 0.4 0.45 0.5 0.55 0.6 0.65 0.7 0.75 0.8 0.85 0.9 0.95
# 0    4  65  457 925  970 816  618 454  141  31    2   1    0   0    0   0    0   0    0
# 1    0   0    1  18   56 133  201 297  294 228  263 319  444 598  662 779  519 223   19

# tab.b <- table(test.data$STATUS,as.integer(preda.b>0.5))
# tab.b
#      0    1
# 0 4450   34
# 1 1000 4054

# mean(test.data$STATUS==as.integer(preda.b>0.5))
# [1] 0.8883403

tab.b1 <- table(test.data4$STATUS,pred.b1)
tab.b1
#   0.2 0.25 0.3 0.35 0.4 0.45 0.5 0.55 0.6 0.65 0.7 0.75 0.8 0.85 0.9 0.95
# 0  40  223 258  185  58   19   1    1   0    0   0    0   0    0   0    0
# 1   0    4  10   37  45   44  43   60  88  137 121  108  96   92  23    1
# 
tab.b1 <- table(test.data4$STATUS,as.integer(preda.b1>0.5))
tab.b1
#     0   1
# 0 783   2
# 1 140 769

mean(test.data4$STATUS==as.integer(preda.b1>0.5))
# [1] 0.6048242

tab.b2 <- table(test.data5$STATUS,pred.b2)
tab.b2
#    0.1 0.15  0.2 0.25  0.3 0.35  0.4 0.45  0.5 0.55  0.6 0.65  0.7 0.75  0.8 0.85  0.9 0.95
# 0   30  276  973 1734 1407  855  572  262   72    4    0    0    0    0    0    0    0    0
# 1    0    3   16   57  168  387  497  418  359  358  574  679 1029 1277 1290  941  358   26

tab.b2 <- table(test.data5$STATUS,as.integer(preda.b2>0.5))
tab.b2
#      0    1
# 0 6109   76
# 1 1546 6891

mean(test.data5$STATUS==as.integer(preda.b2>0.5))
# [1] 0.8666667

plot(as.Date(dat55$CONTRACT_END_DATE[indtest5b]), 
     preda.b2 [test.data5$STATUS == 0],
     ylab = 'Churn Propensity',
     xlab = 'Contract End Date',
     xlim = c(as.Date('2017-01-01'), as.Date('2021-01-01')),
     xaxt = 'n',
     main = 'Gradient Boosting')

axis(1, at = c(as.Date('2017-01-01'),
               as.Date('2018-01-01'),
               as.Date('2019-01-01'),
               as.Date('2020-01-01'),
               as.Date('2021-01-01')),
     labels= seq(2017, 2021, 1), lty = 1, col=0, las=0)

# tab.bg <- table(test.data$STATUS,pred.bg)
# tab.bg
# #      0 0.05  0.1 0.15  0.2 0.25  0.3 0.45  0.7 0.75  0.8  0.9 0.95    1
# # 0 3171    7    0  107  383    0   35  106    8   30    0   20  617    0
# # 1 1130   31  128    4  240    3   18   58   19  190    2    7  405 2819
# 
# tab.bg <- table(test.data$STATUS,as.integer(preda.bg>0.5))
# tab.bg
# #      0    1
# # 0 3809  675
# # 1 1612 3442  
# 
# mean(test.data$STATUS==as.integer(preda.bg>0.5))
# # [1] 0.8164916

# tab.bg1 <- table(test.data4$STATUS,pred.bg1)
# tab.bg1
# #     0 0.05 0.1 0.15 0.2 0.25 0.3 0.35 0.4 0.45 0.5 0.55 0.6 0.65 0.7 0.75 0.8 0.85 0.9 0.95   1
# # 0 552  111  11   15   1    9  11   10   8    8   7   17   2    3   1    4  12    1   2    0   0
# # 1  90   34  30   22  21   10  15   16   5   10  15   18  12    4   4   12  17   13  22   95 444
# 
# tab.bg1 <- table(test.data4$STATUS,as.integer(preda.bg1>0.5))
# tab.bg1
# #     0   1
# # 0 737  48
# # 1 258 651
# 
# mean(test.data4$STATUS==as.integer(preda.bg1>0.5))
# # [1] 0.6704668
# 
# tab.bg2 <- table(test.data5$STATUS,pred.bg2)
# tab.bg2  
# #      0 0.05  0.1 0.15  0.2  0.3 0.35  0.4 0.45  0.5 0.55  0.6 0.65  0.7 0.75  0.8 0.85  0.9 0.95    1
# # 0 5104  205   70   69    0    0   18   31    0   14   49  322   10    0    0    0    9    3  136  145
# # 1 1473  249   52  233    5   84  100   33   18   55  136  171  125    6   32  265   88  227  321 4764
# 
# tab.bg2 <- table(test.data5$STATUS,as.integer(preda.bg2>0.5))
# tab.bg2
# #      0    1
# # 0 5497  688
# # 1 2249 6188
# 
# mean(test.data5$STATUS==as.integer(preda.bg2>0.5))
# # [1] 0.8608187
# 
# plot(as.Date(dat55$CONTRACT_END_DATE[indtest5b]), 
#      preda.bg2 [test.data5$STATUS == 0],
#      ylab = 'Churn Propensity',
#      xlab = 'Contract End Date',
#      xlim = c(as.Date('2017-01-01'), as.Date('2021-01-01')),
#      xaxt = 'n',
#      main = 'Bagging')
# 
# axis(1, at = c(as.Date('2017-01-01'),
#                as.Date('2018-01-01'),
#                as.Date('2019-01-01'),
#                as.Date('2020-01-01'),
#                as.Date('2021-01-01')),
#      labels= seq(2017, 2021, 1), lty = 1, col=0, las=0)

tab.svm1 <- table(test.data4$STATUS,pred.svm1)
tab.svm1
#     0 0.05 0.1 0.15 0.2 0.25 0.3 0.35 0.4 0.45 0.5 0.55 0.6 0.65 0.7 0.75 0.8 0.85 0.9 0.95   1
# 0 732   25   6    1   6    1   2    1   0    0   0    1   0    1   1    0   2    2   2    2   0
# 1  34   29  18   18  13    8   3   10  17   18   7   14  11    7  12   16  22   28  45  557  22

tab.svm1 <- table(test.data4$STATUS,as.integer(pred.svm1>0.5))
tab.svm1
#     0   1
# 0 774  11
# 1 175 734

mean(test.data4$STATUS==as.integer(preda.svm1>0.5))
# [1] 0.5985839

tab.svm2 <- table(test.data5$STATUS,pred.svm2)
tab.svm2
#      0 0.05  0.1 0.15  0.2 0.25  0.3 0.35  0.4 0.45  0.5 0.55  0.6 0.65  0.7 0.75  0.8 0.85  0.9 0.95    1
# 0 5979   68   33   22   15   12   10    8    7    4    5    6    1    1    6    2    1    1    4    0    0
# 1  620  291  146   94   75   74   55   51   69   51   74   67   59   88   90  115  141  220  338 5676   43
#
tab.svm2 <- table(test.data5$STATUS,as.integer(preda.svm2>0.5))
tab.svm2
#      0    1
# 0 6158   27
# 1 1526 6911

mean(test.data5$STATUS==as.integer(preda.svm2>0.5))
# [1] 0.8666667

# plot(as.Date(dat55$CONTRACT_END_DATE[indtest5b]), 
#      preda.svm2 [test.data5$STATUS == 0],
#      ylab = 'Churn Propensity',
#      xlab = 'Contract End Date',
#      xlim = c(as.Date('2017-01-01'), as.Date('2021-01-01')),
#      xaxt = 'n',
#      main = 'Support Vector Machine') 

axis(1, at = c(as.Date('2017-01-01'),
               as.Date('2018-01-01'),
               as.Date('2019-01-01'),
               as.Date('2020-01-01'),
               as.Date('2021-01-01')),
     labels= seq(2017, 2021, 1), lty = 1, col=0, las=0)

# matrices to hold the accuracy, sensitivity, specificity, 
# false discover rate and diagnostic odds ratio measures
accres<-matrix(NA,iterlim,6)
senres<-matrix(NA,iterlim,6)
speres<-matrix(NA,iterlim,6)
fdrres<-matrix(NA,iterlim,6)
dorres<-matrix(NA,iterlim,6)
mccres<-matrix(NA,iterlim,6) 

# # Work out the accuracy
# acc.r <- sum(diag(tab.r))/sum(tab.r);acc.r # 0.7469058
# acc.r1 <- sum(diag(tab.r1))/sum(tab.r1);acc.r1 # 0.7177542
# acc.r2 <- sum(diag(tab.r2))/sum(tab.r2);acc.r2 # 0.8437867
# acc.l1 <- sum(diag(tab.l1))/sum(tab.l1);acc.l1 # 0.3869499
# acc.l2 <- sum(diag(tab.l2))/sum(tab.l2);acc.l2 # 0.8974443
acc.rf1 <- sum(diag(tab.rf1))/sum(tab.rf1);acc.rf1 # 0.7033384
acc.rf2 <- sum(diag(tab.rf2))/sum(tab.rf2);acc.rf2 # 0.8865563
#  acc.b <- sum(diag(tab.b))/sum(tab.b); acc.b # 0.9005664
acc.b1 <- sum(diag(tab.b1))/sum(tab.b1); acc.b1 # 0.6213961
acc.b2 <- sum(diag(tab.b2))/sum(tab.b2); acc.b2 # 0.899335
#  acc.bg <- sum(diag(tab.bg))/sum(tab.bg);acc.bg # 0.8270401
# acc.bg1 <- sum(diag(tab.bg1))/sum(tab.bg1);acc.bg1 # 0.6623672
# acc.bg2 <- sum(diag(tab.bg2))/sum(tab.bg2);acc.bg2 # 0.850176
acc.svm1 <- sum(diag(tab.svm1))/sum(tab.svm1);acc.svm1 # 0.3869499
acc.svm2 <- sum(diag(tab.svm2))/sum(tab.svm2);acc.svm2 # 0.905268
# 
# # Work out the sensitivity
# sen.r <- tab.r[2,2]/sum(tab.r[2,]); sen.r # 0.9606151
# sen.r1 <- tab.r1[2,2]/sum(tab.r1[2,]); sen.r1 # 0.9606151
# sen.r2 <- tab.r2[2,2]/sum(tab.r2[2,]); sen.r2 # 0.9606151
# sen.l1 <- tab.l1[2,2]/sum(tab.l1[2,]); sen.l1 # 0.5988039
# sen.l2 <- tab.l2[2,2]/sum(tab.l2[2,]); sen.l2 # 0.5988039
sen.rf1 <- tab.rf1[2,2]/sum(tab.rf1[2,]); sen.rf1# 0.5919692
sen.rf2 <- tab.rf2[2,2]/sum(tab.rf2[2,]); sen.rf2 # 0.5919692
#  sen.b <- tab.b[2,2]/sum(tab.b[2,]); sen.b
sen.b1 <- tab.b1[2,2]/sum(tab.b1[2,]); sen.b1
sen.b2 <- tab.b2[2,2]/sum(tab.b2[2,]); sen.b2
#  sen.bg <- tab.bg[2,2]/sum(tab.bg[2,]); sen.bg # 0.9608714
# sen.bg1 <- tab.bg1[2,2]/sum(tab.bg1[2,]); sen.bg # 0.9608714
# sen.bg2 <- tab.bg2[2,2]/sum(tab.bg2[2,]); sen.bg # 0.9608714
sen.svm1 <- tab.svm1[2,2]/sum(tab.svm1[2,]); sen.svm1 # NaN
sen.svm2 <- tab.svm2[2,2]/sum(tab.svm2[2,]); sen.svm2 # NaN
#  
# # Work out the specificity
# spe.r <- tab.r[1,1]/sum(tab.r[1,]); spe.r # 0.8717302
# spe.r1 <- tab.r1[1,1]/sum(tab.r1[1,]); spe.r1 # 0.8717302
# spe.r2 <- tab.r2[1,1]/sum(tab.r2[1,]); spe.r2 # 0.8717302
# spe.l1 <- tab.l1[1,1]/sum(tab.l1[1,]); spe.l1 # 0.7639458
# spe.l2 <- tab.l2[1,1]/sum(tab.l2[1,]); spe.l2 # 0.7639458
spe.rf1 <- tab.rf1[1,1]/sum(tab.rf1[1,]); spe.rf1 # 0.7606366
spe.rf2 <- tab.rf2[1,1]/sum(tab.rf2[1,]); spe.rf2 # 0.7606366
#  spe.b <- tab.b[1,1]/sum(tab.b[1,]); spe.b
spe.b1 <- tab.b1[1,1]/sum(tab.b1[1,]); spe.b1
spe.b2 <- tab.b2[1,1]/sum(tab.b2[1,]); spe.b2
#  spe.bg <- tab.bg[1,1]/sum(tab.bg[1,]);spe.bg # 0.8728333
# spe.bg1 <- tab.bg1[1,1]/sum(tab.bg1[1,]);spe.bg1 # 0.8728333
# spe.bg2 <- tab.bg2[1,1]/sum(tab.bg2[1,]);spe.bg2 # 0.8728333
spe.svm1 <- tab.svm1[1,1]/sum(tab.svm1[1,]);spe.svm1 # NaN
spe.svm2 <- tab.svm2[1,1]/sum(tab.svm2[1,]);spe.svm2 # NaN
# 
# # Work out the false dicovery rate
# fdr.r <- tab.r[1,2]/sum(tab.r[,2]); fdr.r # 0.06750705
# fdr.r1 <- tab.r1[1,2]/sum(tab.r1[,2]); fdr.r1 # 0.06750705
# fdr.r2 <- tab.r2[1,2]/sum(tab.r2[,2]); fdr.r2 # 0.06750705
# fdr.l1 <- tab.l1[1,2]/sum(tab.l1[,2]); fdr.l1 # 0.0001426534
# fdr.l2 <- tab.l2[1,2]/sum(tab.l2[,2]); fdr.l2 # 0.0001426534
fdr.rf1 <- tab.rf1[1,2]/sum(tab.rf1[,2]); fdr.rf1 # 0.003165012
fdr.rf2 <- tab.rf2[1,2]/sum(tab.rf2[,2]); fdr.rf2 # 0.003165012
#  fdr.b <- tab.b[1,2]/sum(tab.b[,2]); fdr.b
fdr.b1 <- tab.b1[1,2]/sum(tab.b1[,2]); fdr.b1
fdr.b2 <- tab.b2[1,2]/sum(tab.b2[,2]); fdr.b2
#  fdr.bg <- tab.bg[1,2]/sum(tab.bg[,2]); fdr.bg # 0.06694873
# fdr.bg1 <- tab.bg1[1,2]/sum(tab.bg1[,2]); fdr.bg # 0.06694873
# fdr.bg2 <- tab.bg2[1,2]/sum(tab.bg2[,2]); fdr.bg # 0.06694873
fdr.svm1 <- tab.svm1[1,2]/sum(tab.svm1[,2]); fdr.svm1 # NaN
fdr.svm2 <- tab.svm2[1,2]/sum(tab.svm2[,2]); fdr.svm2 # NaN
# 
# # Work out the diagnostic odds ratio 
# dor.r <- (tab.r[2,2]*tab.r[1,1])/(tab.r[1,2]*tab.r[2,1]); dor.r # 165.7592
# dor.r1 <- (tab.r1[2,2]*tab.r1[1,1])/(tab.r1[1,2]*tab.r1[2,1]); dor.r1 # 165.7592
# dor.r2 <- (tab.r2[2,2]*tab.r2[1,1])/(tab.r2[1,2]*tab.r2[2,1]); dor.r2 # 165.7592
# dor.l1 <- (tab.l1[2,2]*tab.l1[1,1])/(tab.l1[1,2]*tab.l1[2,1]); dor.l1 # 2265309
# dor.l2 <- (tab.l2[2,2]*tab.l2[1,1])/(tab.l2[1,2]*tab.l2[2,1]); dor.l2 # 2265309
dor.rf1 <- (tab.rf1[2,2]*tab.rf1[1,1])/(tab.rf1[1,2]*tab.rf1[2,1]); dor.rf1 # 16003.01
dor.rf2 <- (tab.rf2[2,2]*tab.rf2[1,1])/(tab.rf2[1,2]*tab.rf2[2,1]); dor.rf2 # 16003.01
#  dor.b <- (tab.b[2,2]*tab.b[1,1])/(tab.b[1,2]*tab.b[2,1]); dor.b
dor.b1 <- (tab.b1[2,2]*tab.b1[1,1])/(tab.b1[1,2]*tab.b1[2,1]); dor.b1
dor.b2 <- (tab.b2[2,2]*tab.b2[1,1])/(tab.b2[1,2]*tab.b2[2,1]); dor.b2
#  dor.bg <- (tab.bg[2,2]*tab.bg[1,1])/(tab.bg[1,2]*tab.bg[2,1]); dor.bg #168.5501
# dor.bg1 <- (tab.bg1[2,2]*tab.bg1[1,1])/(tab.bg1[1,2]*tab.bg1[2,1]); dor.bg1 #168.5501
# dor.bg2 <- (tab.bg2[2,2]*tab.bg2[1,1])/(tab.bg2[1,2]*tab.bg2[2,1]); dor.bg2 #168.5501
dor.svm1 <- (tab.svm1[2,2]*tab.svm1[1,1])/(tab.svm1[1,2]*tab.svm1[2,1]); dor.svm1 # NaN
dor.svm2 <- (tab.svm2[2,2]*tab.svm2[1,1])/(tab.svm2[1,2]*tab.svm2[2,1]); dor.svm2 # NaN
#
# MCC = TP * TN - FP * FN / sqrt( (TP +FP) * (TP + FN) * (TN + FP) * (TN + FN) )
# mcc.r <- ((tab.r[2,2]*tab.r[1,1]) - (tab.r[1,2]* tab.r[2,1])) / ( sqrt((tab.r[2,2] + tab.r[1,2])) * sqrt((tab.r[2,2] + tab.r[2,1])) * sqrt((tab.r[1,1] + tab.r[1,2])) * sqrt((tab.r[1,1] + tab.r[2,1])) )
# mcc.r1 <- ((tab.r1[2,2]*tab.r1[1,1]) - (tab.r1[1,2]* tab.r1[2,1])) / ( sqrt((tab.r1[2,2] + tab.r1[1,2])) * sqrt((tab.r1[2,2] + tab.r1[2,1])) * sqrt((tab.r1[1,1] + tab.r1[1,2])) * sqrt((tab.r1[1,1] + tab.r1[2,1])) )
# mcc.r2 <- ((tab.r2[2,2]*tab.r2[1,1]) - (tab.r2[1,2]* tab.r2[2,1])) / ( sqrt((tab.r2[2,2] + tab.r2[1,2])) * sqrt((tab.r2[2,2] + tab.r2[2,1])) * sqrt((tab.r2[1,1] + tab.r2[1,2])) * sqrt((tab.r2[1,1] + tab.r2[2,1])) )
# mcc.l1 <- ((tab.l1[2,2]*tab.l1[1,1]) - (tab.l1[1,2]* tab.l1[2,1])) / ( sqrt((tab.l1[2,2] + tab.l1[1,2])) * sqrt((tab.l1[2,2] + tab.l1[2,1])) * sqrt((tab.l1[1,1] + tab.l1[1,2])) * sqrt((tab.l1[1,1] + tab.l1[2,1])) )
# mcc.l2 <- ((tab.l2[2,2]*tab.l2[1,1]) - (tab.l2[1,2]* tab.l2[2,1])) / ( sqrt((tab.l2[2,2] + tab.l2[1,2])) * sqrt((tab.l2[2,2] + tab.l2[2,1])) * sqrt((tab.l2[1,1] + tab.l2[1,2])) * sqrt((tab.l2[1,1] + tab.l2[2,1])) )
mcc.rf1 <- ((tab.rf1[2,2]*tab.rf1[1,1]) - (tab.rf1[1,2]* tab.rf1[2,1])) / ( sqrt((tab.rf1[2,2] + tab.rf1[1,2])) * sqrt((tab.rf1[2,2] + tab.rf1[2,1])) * sqrt((tab.rf1[1,1] + tab.rf1[1,2])) * sqrt((tab.rf1[1,1] + tab.rf1[2,1])) )
mcc.rf2 <- ((tab.rf2[2,2]*tab.rf2[1,1]) - (tab.rf2[1,2]* tab.rf2[2,1])) / ( sqrt((tab.rf2[2,2] + tab.rf2[1,2])) * sqrt((tab.rf2[2,2] + tab.rf2[2,1])) * sqrt((tab.rf2[1,1] + tab.rf2[1,2])) * sqrt((tab.rf2[1,1] + tab.rf2[2,1])) )
#  mcc.b <- ((tab.b[2,2]*tab.b[1,1]) - (tab.b[1,2]* tab.b[2,1])) / ( sqrt((tab.b[2,2] + tab.b[1,2])) * sqrt((tab.b[2,2] + tab.b[2,1])) * sqrt((tab.b[1,1] + tab.b[1,2])) * sqrt((tab.b[1,1] + tab.b[2,1])) )
mcc.b1 <- ((tab.b1[2,2]*tab.b1[1,1]) - (tab.b1[1,2]* tab.b1[2,1])) / ( sqrt((tab.b1[2,2] + tab.b1[1,2])) * sqrt((tab.b1[2,2] + tab.b1[2,1])) * sqrt((tab.b1[1,1] + tab.b1[1,2])) * sqrt((tab.b1[1,1] + tab.b1[2,1])) )
mcc.b2 <- ((tab.b2[2,2]*tab.b2[1,1]) - (tab.b2[1,2]* tab.b2[2,1])) / ( sqrt((tab.b2[2,2] + tab.b2[1,2])) * sqrt((tab.b2[2,2] + tab.b2[2,1])) * sqrt((tab.b2[1,1] + tab.b2[1,2])) * sqrt((tab.b2[1,1] + tab.b2[2,1])) )
#  mcc.bg <- ((tab.bg[2,2]*tab.bg[1,1]) - (tab.bg[1,2]* tab.bg[2,1])) / ( sqrt((tab.bg[2,2] + tab.bg[1,2])) * sqrt((tab.bg[2,2] + tab.bg[2,1])) * sqrt((tab.bg[1,1] + tab.bg[1,2])) * sqrt((tab.bg[1,1] + tab.bg[2,1])) )
# mcc.bg1 <- ((tab.bg1[2,2]*tab.bg1[1,1]) - (tab.bg1[1,2]* tab.bg1[2,1])) / ( sqrt((tab.bg1[2,2] + tab.bg1[1,2])) * sqrt((tab.bg1[2,2] + tab.bg1[2,1])) * sqrt((tab.bg1[1,1] + tab.bg1[1,2])) * sqrt((tab.bg1[1,1] + tab.bg1[2,1])) )
# mcc.bg2 <- ((tab.bg2[2,2]*tab.bg2[1,1]) - (tab.bg2[1,2]* tab.bg2[2,1])) / ( sqrt((tab.bg2[2,2] + tab.bg2[1,2])) * sqrt((tab.bg2[2,2] + tab.bg2[2,1])) * sqrt((tab.bg2[1,1] + tab.bg2[1,2])) * sqrt((tab.bg2[1,1] + tab.bg2[2,1])) )
mcc.svm1 <- ((tab.svm1[2,2]*tab.svm1[1,1]) - (tab.svm1[1,2]* tab.svm1[2,1])) / ( sqrt((tab.svm1[2,2] + tab.svm1[1,2])) * sqrt((tab.svm1[2,2] + tab.svm1[2,1])) * sqrt((tab.svm1[1,1] + tab.svm1[1,2])) * sqrt((tab.svm1[1,1] + tab.svm1[2,1])) )
mcc.svm2 <- ((tab.svm2[2,2]*tab.svm2[1,1]) - (tab.svm2[1,2]* tab.svm2[2,1])) / ( sqrt((tab.svm2[2,2] + tab.svm2[1,2])) * sqrt((tab.svm2[2,2] + tab.svm2[2,1])) * sqrt((tab.svm2[1,1] + tab.svm2[1,2])) * sqrt((tab.svm2[1,1] + tab.svm2[2,1])) )

# Store the results
iter = 1
# accres[iter,1] <- acc.r
# accres[iter,2] <- acc.r1
# accres[iter,3] <- acc.r2
# accres[iter,4] <- acc.l1
# accres[iter,5] <- acc.l2
accres[iter,1] <- acc.rf1
accres[iter,2] <- acc.rf2
# accres[iter,8] <- acc.b
accres[iter,3] <- acc.b1
accres[iter,4] <- acc.b2
# accres[iter,11] <- acc.bg
# accres[iter,10] <- acc.bg1
# accres[iter,11] <- acc.bg2
accres[iter,5] <- acc.svm1
accres[iter,6] <- acc.svm2

# senres[iter,1] <- sen.r
# senres[iter,2] <- sen.r1
# senres[iter,3] <- sen.r2
# senres[iter,4] <- sen.l1
# senres[iter,5] <- sen.l2
senres[iter,1] <- sen.rf1
senres[iter,2] <- sen.rf2
#  senres[iter,8] <- sen.b
senres[iter,3] <- sen.b1
senres[iter,4] <- sen.b2
#  senres[iter,11] <- sen.bg
# senres[iter,10] <- sen.bg1
# senres[iter,11] <- sen.bg2
senres[iter,5] <- sen.svm1
senres[iter,6] <- sen.svm2

# speres[iter,1] <- spe.r
# speres[iter,2] <- spe.r1
# speres[iter,3] <- spe.r2
# speres[iter,4] <- spe.l1
# speres[iter,5] <- spe.l2
speres[iter,1] <- spe.rf1
speres[iter,2] <- spe.rf2
#  speres[iter,8] <- spe.b
speres[iter,3] <- spe.b1
speres[iter,4] <- spe.b2
#  speres[iter,11] <- spe.bg
# speres[iter,10] <- spe.bg1
# speres[iter,11] <- spe.bg2
speres[iter,5] <- spe.svm1
speres[iter,6] <- spe.svm2

# fdrres[iter,1] <- fdr.r
# fdrres[iter,2] <- fdr.r1
# fdrres[iter,3] <- fdr.r2
# fdrres[iter,4] <- fdr.l1
# fdrres[iter,5] <- fdr.l2
fdrres[iter,1] <- fdr.rf1
fdrres[iter,2] <- fdr.rf2
#  fdrres[iter,8] <- fdr.b
fdrres[iter,3] <- fdr.b1
fdrres[iter,4] <- fdr.b2
#  fdrres[iter,11] <- fdr.bg
# fdrres[iter,10] <- fdr.bg1
# fdrres[iter,11] <- fdr.bg2
fdrres[iter,5] <- fdr.svm1
fdrres[iter,6] <- fdr.svm2

# dorres[iter,1] <- dor.r
# dorres[iter,2] <- dor.r1
# dorres[iter,3] <- dor.r2
# dorres[iter,4] <- dor.l1
# dorres[iter,5] <- dor.l2
dorres[iter,1]<- dor.rf1
dorres[iter,2] <- dor.rf2
#  dorres[iter,8] <- dor.b
dorres[iter,3] <- dor.b1
dorres[iter,4] <- dor.b2
#  dorres[iter,11] <- dor.bg
# dorres[iter,10] <- dor.bg1
# dorres[iter,11] <- dor.bg2
dorres[iter,5] <- dor.svm1
dorres[iter,5] <- dor.svm2

# mccres[iter,1] <- mcc.r
# mccres[iter,2] <- mcc.r1
# mccres[iter,3] <- mcc.r2
# mccres[iter,4] <- mcc.l1
# mccres[iter,5] <- mcc.l2
mccres[iter,1] <- mcc.rf1
mccres[iter,2] <- mcc.rf2
#  mccres[iter,8] <- mcc.b
mccres[iter,3] <- mcc.b1
mccres[iter,4]<- mcc.b2
#  mccres[iter,11] <- mcc.bg
# mccres[iter,10] <- mcc.bg1
# mccres[iter,11] <- mcc.bg2
mccres[iter,5] <- mcc.svm1
mccres[iter,6] <- mcc.svm2

# } # iter

round(accres,3); round(senres,3); round(speres,3); round(fdrres,3); round(dorres,3)


# label the columns
colnames(accres)<-c("test.rf1","test.rf2", "test.b1", "test.b2", "test.svm1",  "test.svm2")
colnames(senres)<-c("test.rf1","test.rf2", "test.b1", "test.b2", "test.svm1",  "test.svm2")
colnames(speres)<-c("test.rf1","test.rf2", "test.b1", "test.b2", "test.svm1",  "test.svm2")
colnames(fdrres)<-c("test.rf1","test.rf2", "test.b1", "test.b2", "test.svm1",  "test.svm2")
colnames(dorres)<-c("test.rf1","test.rf2", "test.b1", "test.b2", "test.svm1",  "test.svm2")
colnames(mccres)<-c("test.rf1","test.rf2", "test.b1", "test.b2", "test.svm1",  "test.svm2")

# append the column number for the maximum, break ties randomly
accres<-cbind(accres, 'chosen' = colnames(accres) [apply(accres,1,which.max)])
senres<-cbind(senres, 'chosen' = colnames(senres) [apply(senres,1,which.max)])
speres<-cbind(speres, 'chosen' = colnames(speres) [apply(speres,1,which.max)])
fdrres<-cbind(fdrres, 'chosen' = colnames(fdrres) [apply(fdrres,1,which.min)])
dorres<-cbind(dorres, 'chosen' = colnames(dorres) [apply(dorres,1,which.max)])
mccres<-cbind(mccres, 'chosen' = colnames(mccres) [apply(mccres,1,which.max)])

# Check out the error rate summary statistics.
# apply(accres,2,summary)
# apply(senres,2,summary)
# apply(speres,2,summary)
# apply(fdrres,2,summary)
# apply(dorres,2,summary)
# 
t(accres)
# [,1]
# test.r    "0.760222268819459"
# test.r1   "0.830578512396694"
# test.r2   "0.796402680891807"
# test.l1   "0.93801652892562" 
# test.l2   "0.89406373957051" 
# test.rf1  "0.854781582054309"
# test.rf2  "0.84092463411298" 
# test.b    "0.891591528622353"
# test.b1   "0.916174734356553"
# test.b2   "0.889071262481193"
# test.bg   "0.760222268819459"
# test.bg1  "0.819362455726092"
# test.bg2  "0.799138284776364"
# test.svm1 "0.890200708382527"
# test.svm2 "0.893790179182054"
# chosen    "test.l1"

t(senres)
# [,1]
# test.r    "0.681044717055797"
# test.r1   "0.721672167216722"
# test.r2   "0.742681047765794"
# test.l1   "0.884488448844885"
# test.l2   "0.816403935048003"
# test.rf1  "0.817381738173817"
# test.rf2  "0.824582197463553"
# test.b    "0.802136921250495"
# test.b1   "0.845984598459846"
# test.b2   "0.816759511674766"
# test.bg   "0.681044717055797"
# test.bg1  "0.716171617161716"
# test.bg2  "0.733436055469954"
# test.svm1 "0.807480748074808"
# test.svm2 "0.819130022519853"
# chosen    "test.l1"

t(speres)
# [,1]
# test.r    "0.849464763603925"
# test.r1   "0.956687898089172"
# test.r2   "0.869684721099434"
# test.l1   "1"                
# test.l2   "1"                
# test.rf1  "0.898089171974522"
# test.rf2  "0.863217461600647"
# test.b    "0.992417484388938"
# test.b1   "0.997452229299363"
# test.b2   "0.987712206952304"
# test.bg   "0.849464763603925"
# test.bg1  "0.938853503184713"
# test.bg2  "0.888763136620857"
# test.svm1 "0.985987261146497"
# test.svm2 "0.995634599838318"
# chosen    "test.l1"

t(fdrres)
# [,1]
# test.r    "0.163954335681321"  
# test.r1   "0.0492753623188406" 
# test.r2   "0.113970588235294"  
# test.l1   "0"                  
# test.l2   "0"                  
# test.rf1  "0.0972053462940462" 
# test.rf2  "0.108419838523645"  
# test.b    "0.00831702544031311"
# test.b1   "0.00259403372243839"
# test.b2   "0.010908568967992"  
# test.bg   "0.163954335681321"  
# test.bg1  "0.0686695278969957" 
# test.bg2  "0.100058173356603"  
# test.svm1 "0.0147651006711409" 
# test.svm2 "0.00389161141539349"
# chosen    "test.l1"

t(dorres)
# test.r    "12.0490561529271"
# test.r1   "57.2722622645896"
# test.r2   "19.2618088884266"
# test.l1   "Inf"             
# test.l2   "Inf"             
# test.rf1  "39.4439006024096"
# test.rf2  "29.6653752156412"
# test.b    "530.597058823529"
# test.b1   "2150.45357142857"
# test.b2   "358.285550146388"
# test.bg   "12.0490561529271"
# test.bg1  "38.7424903100775"
# test.bg2  "21.9835663395618"
# test.svm1 "295.125194805195"
# test.svm2 "1032.90951895539"
# chosen    "test.l1"

t(mccres)
# test.r    "0.404244995385489"
# test.r1   "0.634681627860791"
# test.r2   "0.475420070893323"
# test.l1   "0.869851048924077"
# test.l2   "0.843073608440367"
# test.rf1  "0.689947873610857"
# test.rf2  "0.656039171917944"
# test.b    "0.810590757086767"
# test.b1   "0.838748467802535"
# test.b2   "0.813691029977472"
# test.bg   "0.334879477060015"
# test.bg1  "0.701540782577737"
# test.bg2  "0.479656821794244"
# test.svm1 "0.786576224814936"
# test.svm2 "0.827685190534666"
# chosen    "test.l1"

###############################################

# predictions for all data

###############################################

# preda.r <- predict(fit.r,type="prob",newdata=dat) [,2]
# tab.r <- table(dat$STATUS,((preda.r* 100) %/% 5 ) / 20)
# tab.r
# #   0.15  0.2 0.25  0.3 0.35  0.7  0.8 0.85  0.9 0.95
# # 0 9138 1671 2426 5709 1829 1056  995  910    0    1
# # 1 4094  702  430 2235 1238 1065 4302 3321 3940 7411
# 
# preda.r1 <- predict(fit.r1,type="prob",newdata=dat4) [,2]
# tab.r1 <- table(dat4$STATUS,((preda.r1* 100) %/% 5 ) / 20)
# tab.r1
# 
# #      0 0.05 0.15 0.25  0.3 0.65 0.75  0.8  0.9    1
# # 0  275 1175  116  900  864  365   32    0   27    0
# # 1   31  192   55  482  468  340  234  191  234 3240
# 
# preda.r2 <- predict(fit.r2,type="prob",newdata=dat5) [,2]
# tab.r2 <- table(dat5$STATUS,((preda.r2* 100) %/% 5 ) / 20)
# tab.r2
# 
# #    0.1  0.2 0.25  0.3 0.65 0.75  0.9 0.95    1
# # 0 8660  232 1416 6408  760 1182 1322    1    0
# # 1 2595  359  607 2551  955 5033 7308  263 3600
# 
# preda.l1 <- predict(fit.l1,type="prob",newdata=dat4)
# tab.l1 <- table(dat4$STATUS,((preda.l1* 100) %/% 5 ) / 20)
# tab.l1
# 
# preda.l2 <- predict(fit.l2,type="prob",newdata=dat5)
# tab.l2 <- table(dat5$STATUS,((preda.l2* 100) %/% 5 ) / 20)
# tab.l2

preda.rf1 <- predict(fit.rf1,type="prob",newdata=dat4) [,2]
tab.rf1 <- table(dat4$STATUS,((preda.rf1* 100) %/% 5 ) / 20)
tab.rf1

preda.rf2 <- predict(fit.rf2,type="prob",newdata=dat5) [,2]
tab.rf2 <- table(dat5$STATUS,((preda.rf2* 100) %/% 5 ) / 20)
tab.rf2

# preda.bg <- predict(fit.bg,newdata=dat,type="prob")$prob [,2]
# tab.bg <- table(dat$STATUS,((preda.bg* 100) %/% 5 ) / 20)
# tab.bg

# preda.bg1 <- predict(fit.bg1,newdata=dat4,type="prob")$prob [,2]
# tab.bg1 <- table(dat4$STATUS,((preda.bg1* 100) %/% 5 ) / 20)
# tab.bg1
# 
# preda.bg2 <- predict(fit.bg2,newdata=dat5,type="prob")$prob [,2]
# tab.bg2 <- table(dat5$STATUS,((preda.bg2* 100) %/% 5 ) / 20)
# tab.bg2

# preda.b <- predict(fit.b,newdata=dat,type="prob")$prob [,2]
# tab.b <- table(dat$STATUS,((preda.b* 100) %/% 5 ) / 20)
# tab.b

preda.b1 <- predict(fit.b1,newdata=dat4,type="prob")$prob [,2]
tab.b1 <- table(dat4$STATUS,((preda.b1* 100) %/% 5 ) / 20)
tab.b1

preda.b2 <- predict(fit.b2,newdata=dat5,type="prob")$prob [,2]
tab.b2 <- table(dat5$STATUS,((preda.b2* 100) %/% 5 ) / 20)
tab.b2

# dim(dat5)
# length(preda.svm2)
# dat4[rowSums(is.na(dat4)) > 0, ]  

preda.svm1 <- predict(fit.svm1, dat4, type="probabilities") [,2]
tab.svm1 <- table(dat4$STATUS,((preda.svm1* 100) %/% 5 ) / 20)
tab.svm1

preda.svm2 <- predict(fit.svm2, dat5, type="probabilities") [,2]
tab.svm2 <- table(dat5$STATUS,((preda.svm2* 100) %/% 5 ) / 20)
tab.svm2


####################################################

# Create a table of predictions by customer, premise, MPRN etc

####################################################

cust_indx = dat1$CUSTOMER_ID
cust_prem = dat1$PREMISE_ID
cust_mprn = dat1$MPRN
cust_con_end = as.character( dat1$CONTRACT_END_DATE)
cust_status = dat1$STATUS_TEST
levels(dat1$ELEC) = c('G', 'E')
levels(dat44$ELEC) = c('G', 'E')
levels(dat55$ELEC) = c('G', 'E')
dat55$ELEC[dat55$ELEC == 1] = 'E'
dat55$ELEC[dat55$ELEC == 0] = 'G'
cust_util_type = dat1$ELEC
levels(dat1$BE_SEGMENT) = c('ENTERPRISE', 'SME')
levels(dat44$BE_SEGMENT) = c('ENTERPRISE', 'SME')
levels(dat55$BE_SEGMENT) = c('ENTERPRISE', 'SME')
cust_seg = dat1$BE_SEGMENT

cust_pred = cbind.data.frame ('Customer_ID' = cust_indx, 
                              'Premise_ID' = cust_prem,
                              'MPRN' = cust_mprn,
                              'Contract_end_date' = cust_con_end,
                              'Util_Elec' = cust_util_type, 
                              'Segment' = cust_seg,
                              'Status' = cust_status,
                              'Consumption' = dat1$AVG_YEARLY_LOG_CONSUMP)
cust_pred$MPRN = as.character(cust_pred$MPRN)


# filter for current customers
cust_pred_curr = cust_pred[cust_pred$Status == 0,]
summary(cust_pred_curr); str(cust_pred_curr)
# Customer_ID      Premise_ID             MPRN       Util_Elec Segment   Status                Consumption    Tree      Bagging  
# 24572  :  142   66106  :   10   10002490247:   10   0: 1738   0: 9074   0:43011   -0.904316522001752:  227   0:22599   0:22434  
# 69772  :   98   933144 :   10   10004730081:   10   1:41273   1:33937   1:    0   -1.07572250551149 :  173   1:20412   1:20577  
# 81983  :   94   39612  :    9   10005366162:    9                                 -1.05525120238982 :  164                      
# 72034  :   80   114568 :    8   10000010596:    8                                 -0.856127379587365:  160                      
# 29601  :   74   35494  :    8   10009899125:    8                                 -0.80497246675616 :  148                      
# 81077  :   64   39015  :    8   10011230165:    8                                 -1.12268977493073 :  141                      
# (Other):42459   (Other):42958   (Other)    :42958                                 (Other)           :41998                      # cust_pred_curr[!is.finite(cust_pred_curr$Logistic),]$Logistic = 0

# cust_pred_curr[!is.finite(cust_pred_curr$'Random Forest'),]$'Random Forest' = 0
# cust_pred_curr$Tree = as.integer(cust_pred_curr$Tree) -1
# cust_pred_curr$Bagging = as.integer(cust_pred_curr$Bagging) -1
# cust_pred_curr$vote = apply(cust_pred_curr[,-seq(1,8)],1,sum)
summary(cust_pred_curr)

# write.csv(cust_pred_curr, paste0('Churn Predition_BE_', month_day, '.csv'))

cust_indx4 = dat44$CUSTOMER_ID
cust_prem4 = dat44$PREMISE_ID
cust_mprn4 = dat44$MPRN
cust_con_end4 = as.character(dat44$CONTRACT_END_DATE)
cust_status4 = dat44$STATUS
cust_util_type4 = dat44$ELEC
colnames(probabilities4.all.true) = 'lasso'

# create a prediction dataframe for customers where we do not have contract info
cust_pred4 = cbind.data.frame ('Customer_ID' = cust_indx4, 
                               'Premise_ID' = cust_prem4,
                               'MPRN' = cust_mprn4,
                               'Contract_end_date' = cust_con_end4,
                               'Util_Elec' = dat44$ELEC, 
                               'Segment' = dat44$BE_SEGMENT,
                               'Status' = dat44$STATUS_TEST,
                               'Consumption' = dat44$AVG_YEARLY_LOG_CONSUMP,
                               'lasso' = probabilities4.all.true,
                               'random_forest' = preda.rf1,
                               'boosting' = preda.b1,
                               # 'bagging' = preda.bg1,
                               'svm' = preda.svm1)

cust_pred4$MPRN = as.character(cust_pred4$MPRN)

# filter for current customers
cust_pred_curr4 = cust_pred4[cust_pred4$Status == 0,]
# summary(cust_pred_curr4)
# cust_pred_curr4$vote = apply(cust_pred_curr4[,-seq(1,8)],1,sum)
# table(cust_pred_curr4$vote)
#     0     1     2     3 
# 23302  8439    27     1

cor(cust_pred_curr4[,-seq(1,8)])

#               decision tree logistic     lasso random_forest  boosting   bagging       svm
# decision tree     1.0000000       NA 0.4623633     0.5681475 0.5589909 0.7965987 0.3549557
# logistic                 NA        1        NA            NA        NA        NA        NA
# lasso             0.4623633       NA 1.0000000     0.6769952 0.5391000 0.6445813 0.4964028
# random_forest     0.5681475       NA 0.6769952     1.0000000 0.6918531 0.6736338 0.4494534
# boosting          0.5589909       NA 0.5391000     0.6918531 1.0000000 0.6415703 0.3378689
# bagging           0.7965987       NA 0.6445813     0.6736338 0.6415703 1.0000000 0.4524400
# svm               0.3549557       NA 0.4964028     0.4494534 0.3378689 0.4524400 1.0000000

# write.csv(cust_pred_curr4, paste0('Churn_Predition_BE_',month_day, '_No_Con.csv'))

# create a prediction dataframe for customers where we have contract info
cust_indx5 = dat55$CUSTOMER_ID
cust_prem5 = dat55$PREMISE_ID
cust_mprn5 = dat55$MPRN
cust_con_end5 = dat55$CONTRACT_END_DATE
cust_status5 = dat55$STATUS
cust_util_type5 = dat55$ELEC
cust_seg5 = dat55$BE_SEGMENT
colnames(probabilities5.all.true) = 'lasso'

cust_pred5 = cbind.data.frame ('Customer_ID' = cust_indx5, 
                               'Premise_ID' = cust_prem5,
                               'MPRN' = as.character(cust_mprn5),
                               'Contract_end_date' = cust_con_end5,
                               'Util_Elec' = cust_util_type5, 
                               'Segment' = cust_seg5,
                               'Status' = cust_status5,
                               'Consumption' = dat55$AVG_YEARLY_LOG_CONSUMP, 
                               'lasso' = probabilities5.all.true,
                               'random_forest' = preda.rf2,
                               'boosting' = preda.b2,
                               # 'bagging' = preda.bg2,
                               'svm' = rep(0, length(cust_indx5))
                               #'svm' = preda.svm2
                               )
cust_pred5$MPRN = as.character(cust_pred5$MPRN)

# filter for active customers
cust_pred_curr5 = cust_pred5[cust_pred5$Status == 0,]

cust_pred_curr5$Contract_end_date = gsub(x=cust_pred_curr5$Contract_end_date,pattern=" 00:00:00",replacement="",fixed=T)
cust_pred_curr5$Contract_end_date = gsub(x=cust_pred_curr5$Contract_end_date,pattern=" 23:00:00",replacement="",fixed=T)

cust_pred_curr5$Contract_end_date <- as.Date(cust_pred_curr5$Contract_end_date)

par(mfrow = c(1,1))
plot(x = cust_pred_curr5$Contract_end_date[!is.na(cust_pred_curr5$Contract_end_date)], 
     y = cust_pred_curr5$logistic[!is.na(cust_pred_curr5$Contract_end_date)],
     main = "Logistic Regression", 
     xlab = 'Contract End Date',
     ylab = 'Churn Propensity',
     #ylim = c(0,1),
     col = 1)

# summary(cust_pred_curr5)
# cust_pred_curr5$vote = apply(cust_pred_curr5[,-seq(1,8)],1,sum)
# table(cust_pred_curr5$vote)
#    0    1 
# 7273 1397
cor(cust_pred_curr5[,-seq(1,8)])
#               decision tree     logistic     lasso random_forest  boosting   bagging       svm
# decision tree     1.0000000  0.156392946 0.6473799   0.477318981 0.4258307 0.6692615 0.2071967
# logistic          0.1563929  1.000000000 0.4267970  -0.000918942 0.3361171 0.1775741 0.5083094
# lasso             0.6473799  0.426797020 1.0000000   0.471320423 0.2782676 0.3753855 0.3779475
# random_forest     0.4773190 -0.000918942 0.4713204   1.000000000 0.3641680 0.4502333 0.1710180
# boosting          0.4258307  0.336117066 0.2782676   0.364167962 1.0000000 0.5061030 0.2727746
# bagging           0.6692615  0.177574078 0.3753855   0.450233268 0.5061030 1.0000000 0.1028394
# svm               0.2071967  0.508309409 0.3779475   0.171017960 0.2727746 0.1028394 1.0000000

# write.csv(cust_pred_curr5, paste0('Churn_Predition_BE_', month_day, '_with_Con.csv'))

# merge the three proediction dataframes together
cust_pred_curr6 = merge(cust_pred_curr, 
                        cust_pred_curr5, 
                        by.x = c(1,2,3,6), 
                        by.y = c(1,2,3,6),
                        all.x = T, all.y = F)

cust_pred_curr7 = merge(cust_pred_curr6, 
                        cust_pred_curr4, 
                        by.x = c(1,2,3,4), 
                        by.y = c(1,2,3,6),
                        all.x = T)

# create survivor features
cust_pred_curr7$Status = cust_pred_curr7$Status.x
cust_pred_curr7$Contract_end_date = cust_pred_curr7$Contract_end_date.x
cust_pred_curr7$Consumption = cust_pred_curr7$Consumption.x
cust_pred_curr7$Util_Elec = cust_pred_curr7$Util_Elec.x

# replace NA's with zeros (as 2 of the three dataframes are mutually exclusive)
# cust_pred_curr7$vote.y[is.na(cust_pred_curr7$vote.y)] = 0
# cust_pred_curr7$vote.x[is.na(cust_pred_curr7$vote.x)] = 0
# cust_pred_curr7$vote[is.na(cust_pred_curr7$vote)] = 0
# cust_pred_curr7$decision_tree.x[is.na(cust_pred_curr7$decision_tree.x)] = 0
# cust_pred_curr7$decision_tree.y[is.na(cust_pred_curr7$decision_tree.y)] = 0
# cust_pred_curr7$logistic.x[is.na(cust_pred_curr7$logistic.x)] = 0
# cust_pred_curr7$logistic.y[is.na(cust_pred_curr7$logistic.y)] = 0
cust_pred_curr7$lasso.x[is.na(cust_pred_curr7$lasso.x)] = 0
cust_pred_curr7$lasso.y[is.na(cust_pred_curr7$lasso.y)] = 0
cust_pred_curr7$random_forest.x[is.na(cust_pred_curr7$random_forest.x)] = 0
cust_pred_curr7$random_forest.y[is.na(cust_pred_curr7$random_forest.y)] = 0
cust_pred_curr7$boosting.x[is.na(cust_pred_curr7$boosting.x)] = 0
cust_pred_curr7$boosting.y[is.na(cust_pred_curr7$boosting.y)] = 0
# cust_pred_curr7$bagging.x[is.na(cust_pred_curr7$bagging.x)] = 0
# cust_pred_curr7$bagging.y[is.na(cust_pred_curr7$bagging.y)] = 0
cust_pred_curr7$svm.x[is.na(cust_pred_curr7$svm.x)] = 0
cust_pred_curr7$svm.y[is.na(cust_pred_curr7$svm.y)] = 0

# create features that combine across the mutually exclusive predictors
# cust_pred_curr7$decision_tree = cust_pred_curr7$decision_tree.x + cust_pred_curr7$decision_tree.y
# cust_pred_curr7$logistic = cust_pred_curr7$logistic.x + cust_pred_curr7$logistic.y
cust_pred_curr7$lasso = cust_pred_curr7$lasso.x + cust_pred_curr7$lasso.y
cust_pred_curr7$random_forest = cust_pred_curr7$random_forest.x + cust_pred_curr7$random_forest.y
cust_pred_curr7$boosting = cust_pred_curr7$boosting.x + cust_pred_curr7$boosting.y
# cust_pred_curr7$bagging = cust_pred_curr7$bagging.x + cust_pred_curr7$bagging.y
cust_pred_curr7$svm = cust_pred_curr7$svm.x + cust_pred_curr7$svm.y

# sum the votes across the three dataframes
# cust_pred_curr7$vote = cust_pred_curr7$vote + cust_pred_curr7$vote.x + cust_pred_curr7$vote.y 

# drop the columns that are not required
cust_pred_curr7 = cust_pred_curr7[,-c(which(colnames(cust_pred_curr7) == 'Status.x'),
                                      which(colnames(cust_pred_curr7) == 'Status.y'),
                                      which(colnames(cust_pred_curr7) == 'Contract_end_date.x'),
                                      which(colnames(cust_pred_curr7) == 'Contract_end_date.y'),
                                      which(colnames(cust_pred_curr7) == 'Consumption.x'),
                                      which(colnames(cust_pred_curr7) == 'Consumption.y'),
                                      which(colnames(cust_pred_curr7) == 'Util_Elec.x'),
                                      which(colnames(cust_pred_curr7) == 'Util_Elec.y'),
                                      # which(colnames(cust_pred_curr7) == 'decision_tree.x'),
                                      # which(colnames(cust_pred_curr7) == 'decision_tree.y'),
                                      # which(colnames(cust_pred_curr7) == 'logistic.x'),
                                      # which(colnames(cust_pred_curr7) == 'logistic.y'),
                                      which(colnames(cust_pred_curr7) == 'lasso.x'),
                                      which(colnames(cust_pred_curr7) == 'lasso.y'),
                                      which(colnames(cust_pred_curr7) == 'random_forest.x'),
                                      which(colnames(cust_pred_curr7) == 'random_forest.y'),
                                      which(colnames(cust_pred_curr7) == 'boosting.x'),
                                      which(colnames(cust_pred_curr7) == 'boosting.y'),
                                      # which(colnames(cust_pred_curr7) == 'bagging.x'),
                                      # which(colnames(cust_pred_curr7) == 'bagging.y'),
                                      which(colnames(cust_pred_curr7) == 'svm.x'),
                                      which(colnames(cust_pred_curr7) == 'svm.y'))]

indx = c(# which(colnames(cust_pred_curr7) =="Tree"),
  # which(colnames(cust_pred_curr7) =="Boosting"),
  # which(colnames(cust_pred_curr7) =="Bagging"),
  # which(colnames(cust_pred_curr7) =="decision_tree"),
  # which(colnames(cust_pred_curr7) =="logistic"),
  which(colnames(cust_pred_curr7) =="lasso"),
  which(colnames(cust_pred_curr7) =="random_forest"),
  which(colnames(cust_pred_curr7) =="boosting"),
  #1 which(colnames(cust_pred_curr7) =="bagging"),
  which(colnames(cust_pred_curr7) =="svm"))

indx2 = c(which(colnames(cust_pred_curr7) =="lasso"))

cust_pred_curr7$vote[!is.na(cust_pred_curr7$Contract_end_date)] = cust_pred_curr7[!is.na(cust_pred_curr7$Contract_end_date),indx2]

cust_pred_curr7$vote[is.na(cust_pred_curr7$Contract_end_date)] = apply(cust_pred_curr7[is.na(cust_pred_curr7$Contract_end_date),indx], 1,mean)

cust_pred_curr7$Last_Updated_Date = rep(last_update, dim(cust_pred_curr7)[1])

# table(cust_pred_curr7$vote_bucket)
# 0.05  0.1 0.15  0.2 0.25  0.3 0.35  0.4 0.45  0.5 0.55  0.6 0.65 
# 1391 8644 6224 2365 1003 1511  331  489  731  706  317   27    1

cor(cust_pred_curr7[,c(indx,which(colnames(cust_pred_curr7) == 'vote'))])
# Tree   Boosting      Bagging decision_tree     logistic        lasso random_forest
# Tree           1.000000000 0.42684017  0.911206998   0.468917122 -0.001921924  0.320054340   0.414751802
# Boosting       0.426840169 1.00000000  0.375471889   0.385869299  0.008543170  0.176825288   0.315421146
# Bagging        0.911206998 0.37547189  1.000000000   0.457924651 -0.002239669  0.306330513   0.448880286
# decision_tree  0.468917122 0.38586930  0.457924651   1.000000000 -0.004856937  0.649160264   0.530483255
# logistic      -0.001921924 0.00854317 -0.002239669  -0.004856937  1.000000000 -0.001795885  -0.008828989
# lasso          0.320054340 0.17682529  0.306330513   0.649160264 -0.001795885  1.000000000   0.597169525
# random_forest  0.414751802 0.31542115  0.448880286   0.530483255 -0.008828989  0.597169525   1.000000000
# boosting       0.465093143 0.56567813  0.428727647   0.428084685 -0.001379870  0.181795015   0.469124785
# bagging        0.690176826 0.39728286  0.766143985   0.671913408 -0.002657162  0.383950258   0.514410096
# svm            0.079499632 0.17400282  0.051683300   0.155876248 -0.001073041  0.203744552   0.225359437
# vote           0.824221662 0.53468600  0.840494163   0.782674700 -0.003651433  0.614287271   0.714973011
# boosting      bagging          svm         vote
# Tree           0.46509314  0.690176826  0.079499632  0.824221662
# Boosting       0.56567813  0.397282859  0.174002816  0.534686004
# Bagging        0.42872765  0.766143985  0.051683300  0.840494163
# decision_tree  0.42808468  0.671913408  0.155876248  0.782674700
# logistic      -0.00137987 -0.002657162 -0.001073041 -0.003651433
# lasso          0.18179502  0.383950258  0.203744552  0.614287271
# random_forest  0.46912478  0.514410096  0.225359437  0.714973011
# boosting       1.00000000  0.528546663  0.166120606  0.610018737
# bagging        0.52854666  1.000000000  0.115046006  0.868073890
# svm            0.16612061  0.115046006  1.000000000  0.276186552
# vote           0.61001874  0.868073890  0.276186552  1.000000000
# cust_pred_curr7$vote_percentile = round(ecdf(cust_pred_curr7$vote)(cust_pred_curr7$vote)*100,1)

cust_pred_curr7$Contract_end_date = gsub(x=cust_pred_curr7$Contract_end_date,pattern=" 00:00:00",replacement="",fixed=T)
cust_pred_curr7$Contract_end_date = gsub(x=cust_pred_curr7$Contract_end_date,pattern=" 23:00:00",replacement="",fixed=T)

cust_pred_curr7$Contract_end_date <- as.Date(cust_pred_curr7$Contract_end_date)

setwd("S:/Retail/IRL/Supply/Marketing/MARKETING SUPPLY/Analytics & Products/Analytics/MM WIP/BE Churn")

# 
# 
# cust_pred_curr7 <- sqldf("SELECT *
#              FROM cust_pred_curr7 as l
#              LEFT JOIN dat_classif as r
#              on l.CUSTOMER_ID = r.DEBTORNUM")
# 
# cust_pred_curr7 = cust_pred_curr7[,-which(colnames(cust_pred_curr7)=='DEBTORNUM')]

# required output file column order
# Customer_ID	
# Premise_ID	
# Util_Elec	
# MPRN	
# Segment	
# Contract_end_date	
# contract end date exists	
# contract end month & Year	
# Consumption	
# boosting	
# lasso	
# random_forest	
# svm	
# vote	
# vote * consumption	
# vote and consumption above threshold	
# Last_Updated_Date

cust_pred_curr7$Contract_end_date_exists <- ifelse(is.na(cust_pred_curr7$Contract_end_date), 'N', 'Y')
cust_pred_curr7$vote_by_consumption <- ifelse(cust_pred_curr7$vote >= 0.5 & cust_pred_curr7$Consumption>0, cust_pred_curr7$vote*cust_pred_curr7$Consumption, NA)
cust_pred_curr7$vote_consupmtion_above_threshold <- ifelse(is.na(cust_pred_curr7$vote_by_consumption) , 'N', 'Y')
cust_pred_curr7$Contract_end_Month_Year = paste0(format(cust_pred_curr7$Contract_end_date, "%B"), '_', format(cust_pred_curr7$Contract_end_date, "20%y"))
cust_pred_curr7$Utility_Type = cust_pred_curr7$Util_Elec

out_indx = c(which(colnames(cust_pred_curr7) == 'Customer_ID'), 
             which(colnames(cust_pred_curr7) == 'Premise_ID'),
             which(colnames(cust_pred_curr7) == 'Utility_Type'),
             which(colnames(cust_pred_curr7) == 'MPRN'),
             which(colnames(cust_pred_curr7) == 'Segment'),
             which(colnames(cust_pred_curr7) == 'Contract_end_date'),
             which(colnames(cust_pred_curr7) == 'Contract_end_date_exists'),
             which(colnames(cust_pred_curr7) == 'Contract_end_Month_Year'),
             which(colnames(cust_pred_curr7) == 'Consumption'),
             which(colnames(cust_pred_curr7) == 'boosting'),
             which(colnames(cust_pred_curr7) == 'lasso'),
             which(colnames(cust_pred_curr7) == 'random_forest'),
             which(colnames(cust_pred_curr7) == 'svm'),
             which(colnames(cust_pred_curr7) == 'vote'),
             which(colnames(cust_pred_curr7) == 'vote_by_consumption'),
             which(colnames(cust_pred_curr7) == 'vote_consupmtion_above_threshold'),
             which(colnames(cust_pred_curr7) == 'Last_Updated_Date'))

cust_pred_curr8 <- cust_pred_curr7[,out_indx]

cust_pred_curr9 <- sqldf("SELECT *
             ,dense_rank() over (partition by AA.Customer_ID order by AA.MPRN desc) + dense_rank() over (partition by AA.Customer_ID order by AA.MPRN asc) - 1 as num_all_mprns
             FROM cust_pred_curr8 AA
             ")

# write.csv(cust_pred_curr9, paste0('Churn_Predition_BE_', month_day, '_v2_all.csv'), row.names = F)
dat_churn_model = cust_pred_curr9

# mprn_list = c('XYZ1','XYZ2')
# 
# deep_dive <- dat [which(cust_mprn %in% mprn_list),
#      c(which(colnames(dat) == 'ROI'),
#        which(colnames(dat) == 'MARKETING_OPT_IN'),
#        which(colnames(dat) == 'IN_CONTRACT'),
#        which(colnames(dat) == 'YR_ACQ'),
#        which(colnames(dat) == 'CREDIT_WEIGHTING'),
#        which(colnames(dat) == 'CONTRACT_END_FLAG'),
#        which(colnames(dat) == 'TRI_MONTHLY_CONTRACT_END'),
#        which(colnames(dat) == 'CONTRACT_TYPE'),
#        which(colnames(dat) == 'TENURE_3MTHS'),
#        which(colnames(dat) == 'CNT_METER_ACQS'),
#        which(colnames(dat) == 'CON_END'),
#        which(colnames(dat) == 'TRI_MONTHLY_CONTRACT_END_ABS'),
#        which(colnames(dat) == 'STATUS'))]
# 
# 
# out = cbind.data.frame(cust_mprn[which(cust_mprn %in% mprn_list)], deep_dive)
# write.csv(out, paste0('BE_churn_deep_dive', month_day, '.csv'), row.names = F)
# 
# # churn propensity by churn propensity variability for in contract
# summary(cust_pred_curr7$vote[!is.na(cust_pred_curr7$Contract_end_date)])
# plot(x = cust_pred_curr7$vote[!is.na(cust_pred_curr7$Contract_end_date)], 
#      y = cust_pred_curr7$vote_var[!is.na(cust_pred_curr7$Contract_end_date)],
#      main = "BE Customers With Contract End Date", 
#      xlab = 'Churn Propensity',
#      ylab = 'Churn Propensity Variability')
# 
# # churn by date for in contract
# plot(x = cust_pred_curr7$Contract_end_date[!is.na(cust_pred_curr7$Contract_end_date)], 
#      y = cust_pred_curr7$vote_var[!is.na(cust_pred_curr7$Contract_end_date)],
#      main = "BE Customers With Contract End Date (churn score variability)", 
#      xlab = 'Date',
#      ylab = 'Churn Propensity')
# 
# # churn by date for in contract for each algorithm
# plot(x = cust_pred_curr7$Contract_end_date[!is.na(cust_pred_curr7$Contract_end_date)], 
#      y = cust_pred_curr7$Tree[!is.na(cust_pred_curr7$Contract_end_date)],
#      main = "BE Customers With Contract End Date", 
#      xlab = 'Date',
#      ylab = 'Churn Propensity',
#      ylim = c(0,1),
#      col = 1)
# points (cust_pred_curr7$Boosting[!is.na(cust_pred_curr7$Contract_end_date)], col = 2)
# points (cust_pred_curr7$Bagging[!is.na(cust_pred_curr7$Contract_end_date)], col = 3)
# points (cust_pred_curr7$logistic[!is.na(cust_pred_curr7$Contract_end_date)], col = 4)
# points (cust_pred_curr7$random_forest[!is.na(cust_pred_curr7$Contract_end_date)], col = 5)
# points (cust_pred_curr7$svm[!is.na(cust_pred_curr7$Contract_end_date)], col = 6)
# 
# par(mfrow = c(1,1))
# plot(x = cust_pred_curr7$Contract_end_date[!is.na(cust_pred_curr7$Contract_end_date) & cust_pred_curr7$Consumption < 0], 
#      y = cust_pred_curr7$logistic[!is.na(cust_pred_curr7$Contract_end_date) & cust_pred_curr7$Consumption < 0],
#      main = "Logistic Regression (use usage as a proxy for value)",
#      xlab = 'Contract End Date',
#      ylab = 'Churn Propensity',
#      #ylim = c(0,1),
#      col = 1, 
#      pch = 16)
# 
# points(x = cust_pred_curr7$Contract_end_date[!is.na(cust_pred_curr7$Contract_end_date) & cust_pred_curr7$Consumption >= 0], 
#        y = cust_pred_curr7$logistic[!is.na(cust_pred_curr7$Contract_end_date) & cust_pred_curr7$Consumption >= 0],
#        col = 2, pch = 16)
# 
# legend('topright', legend = (c('usage below median', 'usage above median')),  pch =c(16, 16) , col = c(1,2))
# 
# plot(x = cust_pred_curr7$Contract_end_date[!is.na(cust_pred_curr7$Contract_end_date)], 
#      y = cust_pred_curr7$vote_percentile[!is.na(cust_pred_curr7$Contract_end_date)],
#      main = "Overall churn percentile", 
#      xlab = 'Contract End Date',
#      ylab = 'Churn Percentile',
#      #ylim = c(0,1),
#      col = 1)
# 
# plot(x = cust_pred_curr7$Contract_end_date[!is.na(cust_pred_curr7$Contract_end_date)], 
#      y = cust_pred_curr7$random_forest[!is.na(cust_pred_curr7$Contract_end_date)],
#      main = "Random Forest", 
#      xlab = 'Date',
#      ylab = 'Churn Propensity',
#      #ylim = c(0,1),
#      col = 1)
# 
# plot(x = cust_pred_curr7$Contract_end_date[!is.na(cust_pred_curr7$Contract_end_date)], 
#      y = cust_pred_curr7$svm[!is.na(cust_pred_curr7$Contract_end_date)],
#      main = "SVM", 
#      xlab = 'Date',
#      ylab = 'Churn Propensity',
#      #ylim = c(0,1),
#      col = 1)
# 
# plot(x = cust_pred_curr7$Contract_end_date[!is.na(cust_pred_curr7$Contract_end_date)], 
#      y = cust_pred_curr7$Boosting[!is.na(cust_pred_curr7$Contract_end_date)],
#      main = "Boosting", 
#      xlab = 'Date',
#      ylab = 'Churn Propensity',
#      #ylim = c(0,1),
#      col = 1)
# 
# par(mfrow = c(1,2))
# 
# plot(x = cust_pred_curr7$Contract_end_date[!is.na(cust_pred_curr7$Contract_end_date)], 
#      y = cust_pred_curr7$Tree[!is.na(cust_pred_curr7$Contract_end_date)],
#      main = "Decision Tree", 
#      xlab = 'Date',
#      ylab = 'Churn Propensity',
#      #ylim = c(0,1),
#      col = 1)
# 
# plot(x = cust_pred_curr7$Contract_end_date[!is.na(cust_pred_curr7$Contract_end_date)], 
#      y = cust_pred_curr7$Bagging[!is.na(cust_pred_curr7$Contract_end_date)],
#      main = "Bagging", 
#      xlab = 'Date',
#      ylab = 'Churn Propensity',
#      #ylim = c(0,1),
#      col = 1)
# 
# par(mfrow = c(1,1))
# 
# 
# # churn propensity by variability for out of contract
# plot(x = cust_pred_curr7$vote[is.na(cust_pred_curr7$Contract_end_date)], 
#      y = cust_pred_curr7$vote_var[is.na(cust_pred_curr7$Contract_end_date)],
#      main = "BE Customers Without Contract End Date", 
#      xlab = 'Churn Propensity',
#      ylab = 'Churn Propensity Variability')
# 
# summary(fit.l)
# # Call:
# #   multinom(formula = STATUS ~ ., data = dat[, 1:M], subset = indtrain, 
# #            useNA = "ifany")
# # 
# # Coefficients:
# #   Values  Std. Err.
# # (Intercept)                                                      11.2225006   4.745469
# # ELEC1                                                            11.2225006   4.745469
# # BE_SEGMENTSME                                                    12.2911708   4.745469
# # EBILL1                                                          -24.5096154        NaN
# # DD_PAY1                                                          13.7237277   4.745469
# # ROI1                                                             26.9789828   4.745469
# # MARKETING_OPT_IN1                                                -4.9982714   0.000000
# # CITY1                                                            13.8124503 319.092286
# # INDUSTRYEducation/Community                                       1.6632628   0.000000
# # INDUSTRYEnvironmental                                            -7.7229774   0.000000
# # INDUSTRYFinancial Services                                       -8.3897021   0.000000
# # INDUSTRYGovernment                                                0.0000000   0.000000
# # INDUSTRYHealth/Beauty                                            17.4977041   0.000000
# # INDUSTRYHospitality                                              -0.6152881  55.962643
# # INDUSTRYMedical                                                  -8.9244052   0.000000
# # INDUSTRYProfessional Bodies                                      27.1988647 466.068762
# # INDUSTRYRetail Food                                               5.1930620   0.000000
# # INDUSTRYRetail Non Food                                          10.3110829 364.869965
# # INDUSTRYSports & Leisure                                         -7.8105267   0.000000
# # INDUSTRYTrade & Transport                                       -11.9042645   0.000000
# # INDUSTRYUNKN                                                     -5.2743118  67.959768
# # IN_CONTRACT1                                                      7.3571877   0.000000
# # YR_ACQ                                                          -77.7556255   8.281420
# # CREDIT_WEIGHTING                                                  0.5744466  70.624221
# # CONTRACT_END_FLAG1                                               -7.5675676 104.890983
# # TRI_MONTHLY_CONTRACT_END                                         -5.6365487 192.855956
# # CONTRACT_TYPEIntroductory Period Discount                         0.0000000   0.000000
# # CONTRACT_TYPESmall/Medium Enterprise Price Eligibility Contract  27.2292007 135.842625
# # TENURE_3MTHS                                                    -16.7449246  85.477048
# # SALES_AGENT_TYPEDoor to Door                                      0.0000000   0.000000
# # SALES_AGENT_TYPEEvents                                            0.0000000   0.000000
# # SALES_AGENT_TYPEHomemoves                                         0.0000000   0.000000
# # SALES_AGENT_TYPEOnline                                            0.0000000   0.000000
# # SALES_AGENT_TYPEOnline - ICS                                      0.0000000   0.000000
# # SALES_AGENT_TYPEOther                                            11.2225006   4.745469
# # SALES_AGENT_TYPEProperty Button                                   0.0000000   0.000000
# # YR2                                                              31.5728137  13.203029
# # YR3                                                             -22.0673396  24.151923
# # YR4                                                               1.8839917  61.649902
# # YR5                                                               1.1868900 125.954123
# # YR6                                                              -0.1654939  39.093122
# # AVG_YEARLY_LOG_CONSUMP                                           -5.2166597 352.532199
# # CONSUM_LOG_VAR                                                   11.6678966 164.268017
# # Ten_Yr_Acq                                                        6.2276003  61.576953
# # 
# # Residual Deviance: 0.0001090594 
# # AIC: 66.00011 
# 
# summary(fit.l1)
# # Call:
# #   multinom(formula = STATUS ~ ., data = dat4[, 1:MM], subset = indtrain4, 
# #            useNA = "ifany")
# # 
# # Coefficients:
# #   Values    Std. Err.
# # (Intercept)                                                      1.217355e+04 0.0008137185
# # ELEC1                                                           -3.338660e+03 0.0008137185
# # BE_SEGMENTSME                                                    2.537020e+02 0.0008137185
# # EBILL1                                                           1.542080e+02 0.0000000000
# # DD_PAY1                                                          3.482297e+02 0.0008137185
# # ROI1                                                             4.200417e+02 0.0008137185
# # MARKETING_OPT_IN1                                               -5.658620e+02 0.0000000000
# # CITY1                                                           -4.165188e+01 0.0000000000
# # INDUSTRYEducation/Community                                      4.231164e+02 0.0000000000
# # INDUSTRYEnvironmental                                           -2.534129e+03 0.0000000000
# # INDUSTRYFinancial Services                                      -1.243917e+04 0.0000000000
# # INDUSTRYGovernment                                               0.000000e+00 0.0000000000
# # INDUSTRYHealth/Beauty                                           -2.290321e+03 0.0000000000
# # INDUSTRYHospitality                                             -8.778139e+02 0.0000000000
# # INDUSTRYMedical                                                  2.467272e+02 0.0000000000
# # INDUSTRYProfessional Bodies                                      9.489679e+02 0.0000000000
# # INDUSTRYRetail Food                                             -2.124465e+03 0.0000000000
# # INDUSTRYRetail Non Food                                          6.493761e+02 0.0000000000
# # INDUSTRYSports & Leisure                                         2.247938e+01 0.0000000000
# # INDUSTRYTrade & Transport                                       -8.740089e+02 0.0000000000
# # INDUSTRYUNKN                                                     1.132833e+03 0.0008137185
# # YR_ACQ                                                          -1.885843e+03 0.0024441809
# # CREDIT_WEIGHTING                                                -2.497212e+00 0.0000000000
# # CONTRACT_TYPEIntroductory Period Discount                        1.111275e+03 0.0000000000
# # CONTRACT_TYPESmall/Medium Enterprise Price Eligibility Contract -2.844076e+02 0.0008137185
# # TENURE_3MTHS                                                    -1.433278e+03 0.0227841186
# # SALES_AGENT_TYPEDoor to Door                                     0.000000e+00 0.0000000000
# # SALES_AGENT_TYPEEvents                                           0.000000e+00 0.0000000000
# # SALES_AGENT_TYPEHomemoves                                        0.000000e+00 0.0000000000
# # SALES_AGENT_TYPEOnline                                           0.000000e+00 0.0000000000
# # SALES_AGENT_TYPEOnline - ICS                                     0.000000e+00 0.0000000000
# # SALES_AGENT_TYPEOther                                            1.217355e+04 0.0008137185
# # SALES_AGENT_TYPEProperty Button                                  0.000000e+00 0.0000000000
# # YR2                                                             -9.613895e+02 0.0073416299
# # YR3                                                             -1.320955e+01 0.0220521853
# # YR4                                                             -1.676839e+01 0.0662385440
# # YR5                                                             -4.941957e-01 0.1989619008
# # YR6                                                              2.705315e-01 0.5976254245
# # AVG_YEARLY_LOG_CONSUMP                                          -9.489441e+01 0.0001731766
# # CONSUM_LOG_VAR                                                  -2.984485e+01 0.0001918116
# # Ten_Yr_Acq                                                      -2.486142e+02 0.0684370651
# # 
# # Residual Deviance: 7.999058e-06 
# # AIC: 66.00001 
# 
# 
# summary(fit.l2)
# # Call:
# #   multinom(formula = STATUS ~ ., data = dat5[, 1:M], subset = indtrain5, 
# #            useNA = "ifany")
# # 
# # Coefficients:
# #   Values  Std. Err.
# # (Intercept)                                                       16.12263243 0.06392338
# # ELEC1                                                             16.12263243 0.06392338
# # BE_SEGMENTSME                                                     16.12263243 0.06392338
# # EBILL1                                                           -10.42603253 0.06899861
# # DD_PAY1                                                          -20.94281014 0.06899861
# # ROI1                                                               5.50148682 0.06392338
# # MARKETING_OPT_IN1                                                  0.80413078 0.00000000
# # CITY1                                                              5.46522320 0.00000000
# # INDUSTRYEducation/Community                                      -11.06072151 0.00000000
# # INDUSTRYEnvironmental                                              0.00000000 0.00000000
# # INDUSTRYFinancial Services                                         5.85403489 0.00000000
# # INDUSTRYGovernment                                                 0.00000000 0.00000000
# # INDUSTRYHealth/Beauty                                              3.42849181 0.00000000
# # INDUSTRYHospitality                                                2.51768188 0.00000000
# # INDUSTRYMedical                                                   -7.68399292 0.00000000
# # INDUSTRYProfessional Bodies                                        6.19360475 0.13291808
# # INDUSTRYRetail Food                                                0.00000000 0.00000000
# # INDUSTRYRetail Non Food                                            9.23590567 0.00000000
# # INDUSTRYSports & Leisure                                          -1.06196181 0.06899861
# # INDUSTRYTrade & Transport                                          1.50538358 0.00000000
# # INDUSTRYUNKN                                                       4.06836594 0.00000000
# # IN_CONTRACT1                                                     -29.11158292 0.00000000
# # YR_ACQ                                                          -112.72337148 0.14770360
# # CREDIT_WEIGHTING                                                  -0.06746012 7.57633035
# # CONTRACT_END_FLAG1                                               -22.79126659 0.00000000
# # TRI_MONTHLY_CONTRACT_END                                           3.38537598 0.05886582
# # CONTRACT_TYPEIntroductory Period Discount                          0.00000000 0.00000000
# # CONTRACT_TYPESmall/Medium Enterprise Price Eligibility Contract   26.27764540 0.06392338
# # TENURE_3MTHS                                                     -10.89975815 1.52402837
# # SALES_AGENT_TYPEDoor to Door                                       0.00000000 0.00000000
# # SALES_AGENT_TYPEEvents                                             0.00000000 0.00000000
# # SALES_AGENT_TYPEHomemoves                                          0.00000000 0.00000000
# # SALES_AGENT_TYPEOnline                                             0.00000000 0.00000000
# # SALES_AGENT_TYPEOnline - ICS                                       0.00000000 0.00000000
# # SALES_AGENT_TYPEOther                                             16.12263243 0.06392338
# # SALES_AGENT_TYPEProperty Button                                    0.00000000 0.00000000
# # YR2                                                               33.52229628 0.32535449
# # YR3                                                              -14.54984706 0.66137552
# # YR4                                                                1.68017151 1.14321265
# # YR5                                                                0.72698596 1.18521152
# # YR6                                                               -0.11661072 2.50415013
# # AVG_YEARLY_LOG_CONSUMP                                            -5.13158461 0.10182294
# # CONSUM_LOG_VAR                                                    15.66098941 0.07044657
# # Ten_Yr_Acq                                                         8.03132845 3.42586212
# # 
# # Residual Deviance: 7.514942e-05 
# # AIC: 62.00008 
# 
# 
# summary(fit.bg)
# # Length   Class   Mode     
# # formula           3 formula call     
# # trees           100 -none-  list     
# # votes        220684 -none-  numeric  
# # prob         220684 -none-  numeric  
# # class        110342 -none-  character
# # samples    11034200 -none-  numeric  
# # importance       23 -none-  numeric  
# # terms             3 terms   call     
# # call              4 -none-  call 
# 
# summary(fit.b)
# # Length Class   Mode     
# # formula         3 formula call     
# # trees         100 -none-  list     
# # weights       100 -none-  numeric  
# # votes      220684 -none-  numeric  
# # prob       220684 -none-  numeric  
# # class      110342 -none-  character
# # importance     23 -none-  numeric  
# # terms           3 terms   call     
# # call            5 -none-  call 
# 
# ##########################################
# 
# # Variable Importance for boosting
# 
# ##########################################
# iter<-1
# 
# #prednoiseall <- matrix(NA,36,iter)
# boostimp <- matrix(NA,M-1,iter)
# 
# for (a in 1:iter) {
#   indtrain<-sort(sample(1:N,N*0.75))
#   indtest<-setdiff(1:N,indtrain)
#   Ntest<-length(indtest)
#   #  dattrain<-dat[indtrain,]
#   #  dattest<-dat[indtest,]
#   fit<-boosting(STATUS~.,data=dat[indtrain,])
#   #  pred<-predict(fit,newdata=dattest)
#   
#   # Shuffle column j of the test data for all variables X1 to X36
#   # for (j in 1:36) {
#   #  dattestnoise<-dattest
#   #    dattestnoise[,j]<-dattestnoise[sample(1:Ntest),j]
#   #    prednoise<-predict(fit,newdata=dattestnoise)
#   #    prednoiseall[j,a]<-prednoise$error
#   #    } # next j
#   boostimp[,a]<-fit$importance
# } # next iter
# 
# #prednoiseall
# boostimp
# 
# rownames(boostimp) = colnames(dat[,-18])
# 
# 
# boxplot(t(boostimp), ylim = c(0,max(boostimp)), xlab = "Feature #", ylab = "Relative Importance", main = "Variables Relative Importance",col = 1)
# 
# legend("topleft", legend = "Relative Importance", pch = -1, lty = 1, col=1)
# 
# ##########################################
# 
# # Variable Importance for random forest
# 
# ##########################################
# iterlim =3
# gini<-matrix(NA, M-1, iterlim)
# for (iter in 1:iterlim) 
# {
#   indtrain <- sort(sample(1:N,N*0.75))
#   indtest <- sample(setdiff(1:N,indtrain))
#   
#   # Implement the random forest algorithm
#   fit.rf <- randomForest(STATUS~., data=dat[,1:M], na.action = na.omit, subset=indtrain)
#   gini[,iter]<- fit.rf$importance
# }
# rmeans.rf<- rowMeans(matrix(as.numeric(unlist(gini)),nrow=nrow(gini)))
# rmeans.rf
# 
# rownames(gini) = colnames(dat[,-15])
# 
# par(mfrow=c(1,1))
# 
# rotate_x <- function(data, column_to_plot, labels_vec, rot_angle, type) {
#   plt <- barplot(data[[column_to_plot]], col='steelblue', xaxt="n", main = paste0("Variable Importance (",type," Algorithm)"))
#   text(plt, par("usr")[3], labels = labels_vec, srt = rot_angle, adj = c(1.1,1.1), xpd = TRUE, cex=0.5) 
# }
# 
# rotate_x(as.data.frame(sort(fit.bg$importance, decreasing = T)), 1, names(sort(fit.bg$importance, decreasing = T)), 30, "Bagging")
# 
# rotate_x(as.data.frame(sort(fit.rf$importance, decreasing = T)), 1, 
#          row.names(sort(cbind(fit.rf$importance, "factors" = as.data.frame(rownames(fit.rf$importance))), 1, decreasing = T)), 30, "Random Forest")
# 
# rotate_x(as.data.frame(fit.rf$importance, decreasing = T), 1, 
#          row.names(cbind(fit.rf$importance, "factors" = as.data.frame(rownames(fit.rf$importance)), 1, decreasing = T)), 30, "Random Forest")
# 
# 
# rotate_x(as.data.frame(fit.r$variable.importance), 1, names(fit.r$variable.importance), 30, "Tree Classifier")
# 
# rotate_x(as.data.frame(sort(fit.b$importance, decreasing = T)), 1, names(sort(fit.b$importance, decreasing = T)), 30, "Gradient Boosting")
# 
# 
# barplot(t(gini), 
#         xlab = "", 
#         ylab = "Decrease in Gini Index", 
#         main = "Random Forest variable importance boxplot of decrease in Gini index by Feature#, 30 iterations",
#         las = 2)
# 
# legend("topleft", legend = "Mean", pch = 15, col=3)
#

#########################################################################################################
##################### BE VALUE MODEL ####################################################################
#########################################################################################################


setwd("S:/Retail/IRL/Supply/Marketing/MARKETING SUPPLY/Analytics & Products/Analytics/MM WIP/BE Churn")


library(DBI)
library(ROracle)
library(dplyr)
library(dbplyr)
library(survival)
library(ggplot2)
library(DescTools)
library(forcats)
library(sqldf)

drv <- dbDriver("Oracle")
host <- "Azula008"
port <- "1526"
sid <- "DMPRD1"
connect.string <- paste(
  "(DESCRIPTION=",
  "(ADDRESS=(PROTOCOL=tcp)(HOST=", host, ")(PORT=", port, "))",
  "(CONNECT_DATA=(SID=", sid, ")))", sep = "")

conn <- dbConnect(drv, username =  dbname = connect.string, prefetch = FALSE,
                  bulk_read = 1000L, stmt_cache = 0L, external_credentials = FALSE,
                  sysdba = FALSE)

# Objective: Get a monthly bill for each meter based on 0% discount 
# (i.e. average monthly/Annual KWh consumption and unit standard 
# electricity rates based on the suitable register - 24hr, day/night). 
# For customers with <1 years consumption data this should be estimated 
# by the EUF and then by the median for their cohort (use the same cohorts as 
# we use in the churn file)

dat2 <- dbGetQuery(conn, "SELECT * FROM Analytics.Fact_BE_Value_Model_v3
                   order by customer_id, premise_id, Utility_type_code, MPRN, servicenum, registernum")


# DISC <- dbGetQuery(conn, "SELECT * FROM Analytics.Fact_BE_Value_Tariffs
#                    order by customer_id, premise_id, Utility_type_code, MPRN, servicenum, register_desc")
# 
# 
# DISC = DISC[, -c(which(colnames(DISC) == 'LAST_UPDATE_DATE'))]
# 
# dat_promo <- dbGetQuery(conn, "SELECT * FROM Analytics.Fact_BE_Promo_Tariffs
#                    order by customer_id, premise_id, Utility_type_code, MPRN, servicenum, registernum")
# 
# dat_promo = dat_promo[, -c(which(colnames(dat_promo) == 'LAST_UPDATE_DATE'))]

dbDisconnect(conn)

dat = dat2

dat2$CON_END_MTHS[is.na(dat2$CON_END_MTHS)] = 0

# dat2 = dat

table(dat2$RATE_OFF_CON[!is.na(dat2$PROMO_CODE)], useNA = 'ifany')
#    0 
# 6301

table(dat2$RATE_OFF_CON, is.na(dat2$PROMO_CODE), useNA = 'ifany')

table(dat2$EXPRT, useNA = 'ifany')

#     1 
# 48340 
# customers with export meters not being labelled correctly, one to fix!

# dat2 = dat

# dat_promo$MPRN[nchar(dat_promo$MPRN) < 7] = formatC(dat_promo$MPRN[nchar(dat_promo$MPRN) < 7], width = 7, format = "d", flag = "0")
# 
# dat_promo2 <- sqldf("SELECT *
#              FROM dat_promo as l
#              LEFT JOIN dat2 as r
#              on l.CUSTOMER_ID = r.CUSTOMER_ID
#              and l.PREMISE_ID = r.PREMISE_ID
#              and l.MPRN = r.MPRN 
#              and l.UTILITY_TYPE_CODE = r.UTILITY_TYPE_CODE 
#              and l.SERVICENUM = r.SERVICENUM 
#              and l.REGISTERNUM = r.REGISTERNUM 
#              and l.DG = r.DG
#              ")
# 
# dat_promo2[dat_promo2$CUSTOMER_ID == 2358223,]
# 
# dat_promo2 <- dat_promo2[, -c(max(which(colnames(dat2) == 'CUSTOMER_ID')),
#                               max(which(colnames(dat2) == 'PREMISE_ID')),
#                               max(which(colnames(dat2) == 'MPRN')),
#                               max(which(colnames(dat2) == 'UTILITY_TYPE_CODE')),
#                               max(which(colnames(dat2) == 'SERVICENUM')),
#                               max(which(colnames(dat2) == 'REGISTERNUM')),
#                               max(which(colnames(dat2) == 'REGISTER_DESC')),
#                               max(which(colnames(dat2) == 'DG')),
#                               max(which(colnames(dat2) == 'PROMO_CODE')))]
# 
# dat_promo2$CUSTOMER_ID [ !dat_promo2$CUSTOMER_ID %in% dat_promo$Customer_ID]

table (dat2$UTILITY_TYPE_CODE, dat2$REGISTERNUM, useNA = 'ifany')
dat2$REGISTERNUM[is.na(dat2$REGISTERNUM) & dat2$UTILITY_TYPE_CODE == 'G'] = '0'

# dat3 = dat2[duplicated(dat2),]
# dat_churn2 = dat_churn[duplicated(dat_churn),]

# dat2 [ dat2$CUSTOMER_ID == 117135, ]
# dat2 [ dat2$MPRN == '10303344814', ]
# 
# dat2 [ dat2$REGISTER_DESC == ' N/A', ]

# diff1 = setdiff (dat2$MPRN, dat_churn$MPRN) # 1
# [1] "10303344814"    
# 
# 
# diff2 = setdiff (dat_churn$MPRN, dat2$MPRN) # 0
# character(0)

# # code segment to aggregate load to a meter level, bin it as per REMM NI load segments,
# # and tabulate it to report counts by load bucket, by segment, by jurisdiction and by utility
#
# load_table = aggregate(dat2$AVG_YEARLY_CONSUMPTION,
#                        list(dat2$CUSTOMER_ID,
#                             dat2$PREMISE_ID,
#                             dat2$MPRN,
#                             dat2$JURISDICTION,
#                             dat2$BE_SEGMENT,
#                             dat2$UTILITY_TYPE_CODE),
#                        FUN = sum)
# colnames(load_table) = c('CUSTOMER_ID', 'PREMISE_ID', 'MPRN', 'JURISDICTION',  'SEGMENT',  'UTILITY_TYPE_CODE', 'ANNUAL_LOAD')
# 
# load_table$LOAD_BCKT <- cut(load_table$ANNUAL_LOAD,
#          breaks= c(-Inf, 0, 1000, 2500, 5000, 15000, 20000, 50000, 500000, 2000000, 20000000, 70000000, Inf),
#          labels = c ('<=0 MWh', '0-1 MWh', '1-2.5 MWh', '2.5-5 MWh', '5-15 MWh', '15-20 MWh', '20-50 MWh', '50-500 MWh', '500-2000 MWh', '2000-20000 MWh',  '20000-70000 MWh', ' > 70000 MWh' ))
# 
# load_table_sum <- as.data.frame(table('Annual_Load' = load_table$LOAD_BCKT, 
#                                       'Jurisdiction' = load_table$JURISDICTION, 
#                                       'Segment' = load_table$SEGMENT, 
#                                       'Utility' = load_table$UTILITY_TYPE_CODE,
#                                       useNA = 'ifany'))
# 
# setwd("S:/Retail/IRL/Supply/Marketing/MARKETING SUPPLY/Analytics & Products/Analytics/MM WIP/BE Value Model")
# day_month_yr = paste0(format(Sys.time(), "%d"), format(Sys.time(), "%b"), format(Sys.time(), "%y"))
# write.csv(load_table_sum, paste0('load_table_sum', day_month_yr, '.csv'), row.names = F)

dat2 %>% 
  group_by(JURISDICTION, BE_SEGMENT) %>% 
  summarise(CATEGORY = paste(unique(CATGRY), collapse = ', '))

# A tibble: 4 x 3
# Groups:   JURISDICTION [2]
# JURISDICTION BE_SEGMENT CATEGORY                                    
#   <chr>        <chr>      <chr>                                       
# 1 NI           Enterprise Popular, Nightsaver, Weekender, Off-Peak, NA
# 2 NI           SME        Popular, Nightsaver, Weekender, Off-Peak, NA
# 3 ROI          Enterprise GP, DG2, DG1, CBC, GPNS, LVMD, NA           
# 4 ROI          SME        GPNS, GP, CBC, DG1, DG2, LVMD, NA, DG5   


# # test that we have the expected number of meters
# dat3 = dat2[, c(which(colnames(dat2)=='MPRN'),
#                which(colnames(dat2)=='UTILITY_TYPE_CODE'),
#                which(colnames(dat2)=='JURISDICTION'))]
# 
# dat4 = distinct(dat3)
# 
# CNT = function(x, na.rm = T) sum( !is.na(x) ) # gets the counts
# 
# mu <- dat4 %>%
#   group_by(UTILITY_TYPE_CODE,JURISDICTION) %>%
#   summarize_at ('MPRN', funs(grp.count = CNT), na.rm = T)
# mu
# # A tibble: 3 x 3
# # Groups:   UTILITY_TYPE_CODE [?]
# 
# # UTILITY_TYPE_CODE JURISDICTION grp.count
# # <chr>             <chr>            <int>
# # 1 E                 NI              161487
# # 2 E                 ROI             229251
# # 3 G                 ROI              84684
# 
# indx = dat2$JURISDICTION == 'ROI' & dat2$UTILITY_TYPE_CODE == 'G'
# summary(dat2[indx,])
# table(dat2$MAX_TO_MEAN_READ_RATIO[indx], dat2$CONSUMPTION_VAR[indx], useNA = 'ifany')

dat2 = dat2[,-c(which(colnames(dat2) == 'LAST_UPDATE_DATE'))]

# # some code to compare NI usage with AUF/EUF from csv flatfiles
# indx = dat2$JURISDICTION == 'NI' & dat2$AVG_YEARLY_CONSUMPTION != dat2$EAC
# dat2_NI = dat2 [indx,]
# 
# agg_NI_E = aggregate(dat2_NI$AVG_YEARLY_CONSUMPTION,
#                      list(dat2_NI$MPRN, dat2_NI$PAYMENT_CHANNEL_CATEGORY),
#                      FUN = sum)
# 
# colnames(agg_NI_E) = c('MPRN', 'PAYMENT', 'USAGE')
# 
# setwd("S:/Retail/IRL/Supply/Marketing/MARKETING SUPPLY/Analytics & Products/Analytics/MM WIP/Value Model/churn projections")
# NI_EUF = read.csv('NI_EUF_JUL19.csv', header = T)
# NI_AUF = read.csv('NI_AUF_JUL19.csv', header = T)
# agg_NI_E = merge(agg_NI_E, NI_EUF, by = 'MPRN', all.x = T, all.y = F)
# agg_NI_E = merge(agg_NI_E, NI_AUF, by = 'MPRN', all.x = T, all.y = F)
# 
# # agg_NI_E = na.omit(agg_NI_E)
# indx2 = !is.na(agg_NI_E$EUF) & !is.na(agg_NI_E$USAGE)
# agg_NI_E$EUF_RATIO [indx2 ] = agg_NI_E$USAGE[indx2 ]/agg_NI_E$EUF[indx2 ]
# indx3 = !is.na(agg_NI_E$AUF) & !is.na(agg_NI_E$USAGE)
# agg_NI_E$AUF_RATIO [indx3] = agg_NI_E$USAGE[indx3]/agg_NI_E$AUF[indx3]
# 
# summary(agg_NI_E$RATIO)
# summary(agg_NI_E$RATIO)
# indx4 = agg_NI_E$PAYMENT == 'Credit' & !is.na(agg_NI_E$EUF_RATIO) & agg_NI_E$EUF_RATIO < 3
# hist(agg_NI_E$EUF_RATIO[indx4],
#      xlim = c(0,3),
#      main = 'Histogram of Actual Usage/EUF for NI Elec CREDIT',
#      xlab = 'Ratio of Usage to EUF',
#      breaks = 200,
#      col = 3)
# 
# indx4 = agg_NI_E$PAYMENT == 'Credit' & !is.na(agg_NI_E$AUF_RATIO) & agg_NI_E$AUF_RATIO < 3
# hist(agg_NI_E$AUF_RATIO[indx4],
#      xlim = c(0,3),
#      main = 'Histogram of Actual Usage/AUF for NI Elec CREDIT',
#      xlab = 'Ratio of Usage to AUF',
#      breaks = 200,
#      col = 3)
# 
# indx4 = agg_NI_E$PAYMENT == 'Prepay' & !is.na(agg_NI_E$EUF_RATIO) & agg_NI_E$EUF_RATIO < 3
# hist(agg_NI_E$EUF_RATIO[indx4],
#      #xlim = c(0,3),
#      main = 'Histogram of Actual Usage/EUF for NI Elec Prepay',
#      xlab = 'Ratio of Usage to EUF',
#      breaks = 200,
#      col = 3)
# 
# indx4 = agg_NI_E$PAYMENT == 'Prepay' & !is.na(agg_NI_E$AUF_RATIO) & agg_NI_E$AUF_RATIO < 3
# hist(agg_NI_E$AUF_RATIO[indx4],
#      #xlim = c(0,3),
#      main = 'Histogram of Actual Usage/AUF for NI Elec Prepay',
#      xlab = 'Ratio of Usage to AUF',
#      breaks = 200,
#      col = 3)

table(dat2$DF_SF, dat2$DF_SF2, useNA = 'ifany')

#       DF    SF
# DF  2405     8
# SF     0 50168

# replace null DF_SF values with the calculated value
dat2$DF_SF[is.na(dat2$DF_SF)] <- dat2$DF_SF2[is.na(dat2$DF_SF)] 

table(dat2$DF_SF, dat2$DF_SF2, useNA = 'ifany')

#       DF    SF
# DF  2405     8
# SF     0 50168

hist(dat2$EAC[is.finite(dat2$EAC) & is.finite(dat2$CONSUMPTION_VAR) & dat2$AVG_YEARLY_CONSUMPTION > 0]/dat2$AVG_YEARLY_CONSUMPTION[is.finite(dat2$EAC) & is.finite(dat2$CONSUMPTION_VAR) & dat2$AVG_YEARLY_CONSUMPTION > 0],
     xlim = c(0,5),
     xlab = 'Usage Ratio',
     main = 'Ratio of EAC to Actual Annual Consumption', 
     breaks = 1000000, 
     col = 3)

hist(dat2$EAC[is.finite(dat2$EAC) & is.finite(dat2$CONSUMPTION_VAR) & dat2$AVG_YEARLY_CONSUMPTION > 0 & dat2$EAC != dat2$AVG_YEARLY_CONSUMPTION]/dat2$AVG_YEARLY_CONSUMPTION[is.finite(dat2$EAC) & is.finite(dat2$CONSUMPTION_VAR) & dat2$AVG_YEARLY_CONSUMPTION > 0 & dat2$EAC != dat2$AVG_YEARLY_CONSUMPTION],
     xlim = c(0,5),
     xlab = 'Usage Ratio',
     main = 'Ratio of EAC to Actual Annual Consumption', 
     breaks = 1000000, 
     col = 3)

table(dat2$WEL_CREDIT, useNA = 'ifany')
# -100  -120  -135  -150  -180  -200   -55     0 
#    8    10    10    18     2    10     1 52746  




# head(dat2[dat2$NUM_ELEC_MTRS >2 ,])
# head(dat2[dat2$NUM_REG >2 ,])
# head(dat2[dat2$NUM_REG == 2 ,])
# head(dat2[dat2$CUSTOMER_ID == 476487,])

#head(dat2[dat2$NUM_REG == 2,])

sum(dat2$WEL_CREDIT < 0) # count of welcome credits
# [1] 43


table(dat2$PAYMENT_CHANNEL_CATEGORY, dat2$METER_TYPE, useNA = 'ifany')

#        CREDIT   PPM
# Credit  50008    12
# Prepay      5     0

# replace null PAYMENT_CHANNEL_CATEGORY values with the equivalent METER_TYPE value
dat2$PAYMENT_CHANNEL_CATEGORY[dat2$PAYMENT_CHANNEL_CATEGORY == ' N/A' & dat2$METER_TYPE == 'CREDIT'] <- 'Credit'
dat2$PAYMENT_CHANNEL_CATEGORY[dat2$PAYMENT_CHANNEL_CATEGORY == ' N/A' & dat2$METER_TYPE == 'PPM'] <- 'Prepay'

table(dat2$REGISTER_DESC, dat2$JURISDICTION, useNA = 'ifany')

#                            NI   ROI
#  N/A                      750  1469
# 24hr                        0 18243
# Day                      1417  7452
# GAS_24HR                    0  1946
# Heating                   591     0
# Heating Auto               20     0
# Low                       526     0
# Night                    1308  7365
# Normal                    526     0
# NSH                         0  1901
# Off Peak 11 Hour           35     0
# Off Peak 15 Hour           29     0
# Off Peak 8 Hour            32     0
# Popular                  9504     0
# Summer Weekday Shoulder     0     1
# Winter Weekday Shoulder     0     1
# <NA>                      426  1290

table(dat2$REGISTER_DESC, dat2$DG, useNA = 'ifany')

table(dat2$REGISTER_DESC, dat2$METER_TYPE, useNA = 'ifany')

#          CREDIT   PPM  <NA>
# 24hr      26723    32  1029
# Day        8246     3   304
# GAS_24HR   1791     0   109
# Heating     900     0    30
# Low         722     0    18
# Night      8064     1   311
# Normal      718     0    20
# NSH        1771     0    86
# Off Peak    103     0     7

table(dat2$REGISTER_DESC, dat2$PAYMENT_CHANNEL_CATEGORY, useNA = 'ifany')

#          Credit Prepay
# 24hr      27755     29
# Day        8553      0
# GAS_24HR   1900      0
# Heating     930      0
# Low         740      0
# Night      8376      0
# Normal      738      0
# NSH        1857      0
# Off Peak    110      0

table(dat2$REGISTER_DESC[dat2$JURISDICTION == 'NI'], dat2$PAYMENT_CHANNEL_CATEGORY[dat2$JURISDICTION == 'NI'], useNA = 'ifany')

#          Credit Prepay
# 24hr       9611     21
# Day        2012      0
# Heating     930      0
# Low         740      0
# Night      1837      0
# Normal      738      0
# Off Peak    110      0

# table(dat2$REGISTER_DESC[dat2$JURISDICTION == 'NI'], dat2$DG [dat2$JURISDICTION == 'NI'], useNA = 'ifany')

table(dat2$REGISTER_DESC[dat2$JURISDICTION == 'ROI'], dat2$PAYMENT_CHANNEL_CATEGORY[dat2$JURISDICTION == 'ROI'], useNA = 'ifany')

#          Credit Prepay
# 24hr      18144      8
# Day        6541      0
# GAS_24HR   1900      0
# Night      6539      0
# NSH        1857      0

table(dat2$REGISTER_DESC[dat2$JURISDICTION == 'ROI' & dat2$DG == 'DG6'], useNA = 'ifany')
# N/A                    24hr                     Day                   Night                     NSH 
# 137                      26                     435                     416                       5 
# Summer Weekday Shoulder Winter Weekday Shoulder                    <NA> 
#                       1                       1                   13696 

table(dat2$REGISTER_DESC[dat2$JURISDICTION == 'ROI'], dat2$DG[dat2$JURISDICTION == 'ROI'], useNA = 'ifany')

#            CBC   DG1   DG2   DG5
# 24hr         0  1300   782 16070
# Day          0   405   156  5980
# GAS_24HR  1900     0     0     0
# Night        0   404   155  5980
# NSH          0    71    22  1764

table(dat2$REGISTER_DESC[dat2$JURISDICTION == 'NI'], dat2$DG[dat2$JURISDICTION == 'NI'], useNA = 'ifany')

#          T011 T012 T014 T015 T021 T022 T024 T031 T032 T033 T034 T041 T042 T043 T052 T053 T062 T063
# 24hr      536    0    0    0   21    0    0 8939    1    0    0   41   38   30    0   22    1    3
# Day         0   11   12    8    2   16    3    1  730    0 1219    0    0    0   10    0    0    0
# Heating     0    0   10    8    0    0    3    1    0    0  908    0    0    0    0    0    0    0
# Low         0    0    0    0    0    0    0    0    0  740    0    0    0    0    0    0    0    0
# Night       0   11   11    8    2   16    3    1  729    0 1054    0    0    0    2    0    0    0
# Normal      0    0    0    0    0    0    0    0    0  738    0    0    0    0    0    0    0    0
# Off Peak    0    0    0    0    0    0    0    1    0    0    0   39   37   29    0    0    1    3


table(dat2$EXPRT [ dat2$EXPRT == 1 ], dat2$REGISTER_DESC [ dat2$EXPRT == 1 ], useNA = 'ifany')

#   24hr Day Night
# 1  151  25    25

table(dat2$REGISTER_DESC, dat2$NUM_REG, useNA = 'ifany')


# Tidy up some NA's
dat2$REGISTER_DESC[dat2$NUM_REG==1 & dat2$ELEC==1 & (dat2$REGISTER_DESC == ' N/A' | is.na(dat2$REGISTER_DESC))] <- '24hr'
dat2$REGISTER_DESC[dat2$NUM_REG==1 & dat2$ELEC==0 & (dat2$REGISTER_DESC == ' N/A' | is.na(dat2$REGISTER_DESC))] <- 'GAS_24HR'

table(dat2$REGISTER_DESC, dat2$NUM_REG, useNA = 'ifany')

#              1     2     3     4     5     6     7
# 24hr     23149  3209   929   341   134    15     7
# Day          0  7219   831   480    16     4     3
# GAS_24HR  1900     0     0     0     0     0     0
# Heating      0     0   738   183     7     2     0
# Low          0   740     0     0     0     0     0
# Night        0  7213   834   311    13     2     3
# Normal       0   738     0     0     0     0     0
# NSH          0  1615   171    49    20     1     1
# Off Peak     0   107     3     0     0     0     0


dat2$METER_TYPE[is.na(dat2$METER_TYPE)] <- toupper(dat2$PAYMENT_CHANNEL_CATEGORY[is.na(dat2$METER_TYPE)])
dat2$METER_TYPE[dat2$METER_TYPE == 'PREPAY'] <- 'PPM'

table(dat2$METER_TYPE, dat2$PAYMENT_CHANNEL_CATEGORY, useNA = 'ifany')
#        Credit Prepay
# CREDIT  50930     21
# PPM        29      8

table(dat2$JURISDICTION, dat2$ROI, useNA = 'ifany')


table(dat2$UTILITY_TYPE_CODE, dat2$ELEC, useNA = 'ifany')


table(dat2$PAYMENT_CHANNEL_CATEGORY, dat2$CREDIT, useNA = 'ifany')


table(dat2$DF_SF, useNA = 'ifany')


table(dat2$DF_SF2, useNA = 'ifany')


table(dat2$EBILL, useNA = 'ifany')


table(dat2$DD_PAY, useNA = 'ifany')


# table(dat2$ELEC_METER_TYPE, useNA = 'ifany')
# table(dat2$ELEC_METER_TYPE, dat2$METER_TYPE, useNA = 'ifany')
# table(dat2$ELEC_METER_TYPE, dat2$ELEC, useNA = 'ifany')
table(dat2$METER_TYPE, useNA = 'ifany')

table(dat2$JURISDICTION, useNA = 'ifany')


dat2$EBILL[is.na(dat2$EBILL)] = 0
dat2$DD_PAY[is.na(dat2$DD_PAY)] = 'N'


# convert to factors
dat2$ROI = as.factor(dat2$ROI)
dat2$ELEC = as.factor(dat2$ELEC)
dat2$CREDIT = as.factor(dat2$CREDIT)
dat2$PAYMENT_CHANNEL_CATEGORY = as.factor(dat2$PAYMENT_CHANNEL_CATEGORY)
dat2$EBILL = as.factor(dat2$EBILL)
dat2$DD_PAY = as.factor(dat2$DD_PAY)
dat2$MARKETING_OPT_IN = as.factor(dat2$MARKETING_OPT_IN)
dat2$EXPRT = as.factor(dat2$EXPRT)
dat2$DF_SF = as.factor(dat2$DF_SF)
dat2$ACQ_CHANNEL = as.factor(dat2$ACQ_CHANNEL)


# hist(dat2$AVG_YEARLY_CONSUMPTION [dat2$AVG_YEARLY_CONSUMPTION < 5e4], 
#      xlab = 'Annual Consumption (KWh)',
#      main = 'Home Energy Annual Usage Histogram',
#      breaks = 500)
# 
# indx = !is.na(dat2$AVG_YEARLY_CONSUMPTION) & !is.na(dat2$CONSUMPTION_VAR) & !is.na(dat2$EAC) & !is.na(dat2$MAX_TO_MEAN_READ_RATIO)
# 
# dat3 = dat2[indx,]
# 
# # dat3 = dat2[,-c(which(colnames(dat2)=='CONSUMPTION_VAR')
# #                ,which(colnames(dat2)=='EAC')
# #                )]
# 
# dat3$CUSTOMER_ID[dat3$AVG_YEARLY_CONSUMPTION == max(dat3$AVG_YEARLY_CONSUMPTION, na.rm = T)]
# # 2157758
# dat3$PREMISE_ID[dat3$AVG_YEARLY_CONSUMPTION == max(dat3$AVG_YEARLY_CONSUMPTION, na.rm = T)]
# # 590871
# dat3$MPRN[dat3$AVG_YEARLY_CONSUMPTION == max(dat3$AVG_YEARLY_CONSUMPTION, na.rm = T)]
# # "10302547710"
# dat3$CONSUMPTION_VAR[dat3$AVG_YEARLY_CONSUMPTION == max(dat3$AVG_YEARLY_CONSUMPTION, na.rm = T)]
# # 0.55
# dat3$AVG_YEARLY_CONSUMPTION[dat3$AVG_YEARLY_CONSUMPTION == max(dat3$AVG_YEARLY_CONSUMPTION, na.rm = T)]
# # 43495453
# 
# 
# summary(dat3$AVG_YEARLY_CONSUMPTION)
# # Min.  1st Qu.   Median     Mean  3rd Qu.     Max. 
# #    0     2467     3931     5182     5953 43495453
# 
# sum(dat3$AVG_YEARLY_CONSUMPTION>1E5 & dat3$AVG_YEARLY_CONSUMPTION != dat3$EAC)
# # 304
# sum(dat3$EAC>1E5 & dat3$AVG_YEARLY_CONSUMPTION != dat3$EAC)
# # 24
# 
# setdiff(dat3$CUSTOMER_ID[dat3$AVG_YEARLY_CONSUMPTION>1E5], dat3$CUSTOMER_ID[dat3$EAC>1E5])
# # [1]  614874  805479  889856 1174044 1214311 2158730
# 
# setdiff(dat3$CUSTOMER_ID[dat3$EAC>1E5], dat3$CUSTOMER_ID[dat3$AVG_YEARLY_CONSUMPTION>1E5])
# #  [1]  235776  327663  454208  630442  807520  798052  740194  817564  975643 1290708 1339160 1592406
# # [13] 1827366 1921877 1912264 2005364 2136619 2170992 2202483 2308407
# 
# indx = dat3$AVG_YEARLY_CONSUMPTION>1E5 & dat3$AVG_YEARLY_CONSUMPTION != dat3$EAC
# table(dat3$CONSUMPTION_VAR[indx], dat3$AVG_YEARLY_CONSUMPTION[indx], useNA = 'ifany' )
# 
# indx = dat3$AVG_YEARLY_CONSUMPTION != dat3$EAC & dat3$UTILITY_TYPE_CODE == 'E' & dat3$MAX_TO_MEAN_READ_RATIO > 400 & dat3$AVG_YEARLY_CONSUMPTION > 50000
# # & dat3$EXPRT == 0
# # & dat3$MAX_TO_MEAN_READ_RATIO < 160 
# sum(indx)
# dat3$CUSTOMER_ID[indx]
# # export meter: [1] 1156147
# dat3$PREMISE_ID[indx]
# 
# hist(log(dat3$MAX_TO_MEAN_READ_RATIO[indx]), breaks = 500)
# hist(log(dat3$AVG_YEARLY_CONSUMPTION[indx]), breaks = 500)
# 
# hist(log(dat3$MAX_TO_MEAN_READ_RATIO[dat3$EXPRT == 1]), breaks = 50)
# hist(log(dat3$MAX_TO_MEAN_READ_RATIO[dat3$EXPRT == 0 & dat3$AVG_YEARLY_CONSUMPTION<3000]), breaks = 50)
# 
# dat3[dat3$CUSTOMER_ID == 1156147,]
# 
# 
# table(dat3$CONSUMPTION_VAR[indx], dat3$AVG_YEARLY_CONSUMPTION[indx], useNA = 'ifany' )
# table(dat3$MAX_TO_MEAN_READ_RATIO[indx], dat3$AVG_YEARLY_CONSUMPTION[indx], useNA = 'ifany' )
# plot(dat3$MAX_TO_MEAN_READ_RATIO[indx], dat3$AVG_YEARLY_CONSUMPTION[indx])
# indx = dat3$AVG_YEARLY_CONSUMPTION > 2*dat3$EAC | dat3$AVG_YEARLY_CONSUMPTION > 500
# indx = dat3$MAX_TO_MEAN_READ_RATIO < 150
# plot(log(dat3$MAX_TO_MEAN_READ_RATIO[indx]), log(dat3$AVG_YEARLY_CONSUMPTION[indx]))
# 
# 
# hist(log(dat3$AVG_YEARLY_CONSUMPTION[dat3$UTILITY_TYPE_CODE == 'G']), breaks = 500)
# hist(log(dat3$AVG_YEARLY_CONSUMPTION[dat3$UTILITY_TYPE_CODE == 'E']), breaks = 500)
# 
# plot(log(dat3$MAX_TO_MEAN_READ_RATIO[indx]), log(dat3$CONSUMPTION_VAR[indx]))
# abline(v=log(150), col = 2)
# abline(h=log(50), col = 2)
# 
# plot(log(dat3$MAX_TO_MEAN_READ_RATIO), log(dat3$CONSUMPTION_VAR))
# plot(log(dat3$MAX_TO_MEAN_READ_RATIO), log(dat3$AVG_YEARLY_CONSUMPTION))
# 
# dat3$CUSTOMER_ID [dat3$AVG_YEARLY_CONSUMPTION == 1565 & dat3$CONSUMPTION_VAR > 1500]
# 
# 
# ecdf1 = t(data.frame('percentiles' = ecdf(dat3$MAX_TO_MEAN_READ_RATIO) (seq(0, 65000, by = 1000))))
# colnames(ecdf1) = seq(0, 65000, by = 1000)
# ecdf1
# 
# ecdf(na.omit(dat2$CONSUMPTION_VAR)) (100)
# 
# ecdf(na.omit(dat2$CONSUMPTION_VAR)) (5)
# # [1] 0.9985171
# 
# sum(dat3$CONSUMPTION_VAR>10)
# 0

# From Niamh (28 Mar 2019)
# Cost to acquire       Commissions
# Home Energy	          Elec Only	Dual Fuel
# Doors - ROI	          80.42	    114.89
# Doors - NI	          79.72	    113.88
# Call In	              7.73	    11.05
# Online 	              0.00	    0.00
# ICS	                  30.33	    60.65
# Home Moves	          12.31	    17.59
# Property Button	      44.64	    89.29
# Call Centre Outbound 	46.00	    65.71
# Total	                60.70	    86.72


# if usage is very high then assume an error and set it to NA
indx = dat2$AVG_YEARLY_CONSUMPTION %/% 10 <= 0 | 
  is.na(dat2$AVG_YEARLY_CONSUMPTION) | 
  (!is.na(dat2$MAX_TO_MEAN_READ_RATIO) & 
     !is.na(dat2$AVG_YEARLY_CONSUMPTION) & 
     dat2$MAX_TO_MEAN_READ_RATIO > 150 & 
     dat2$AVG_YEARLY_CONSUMPTION > 50000)
indx = dat2$AVG_YEARLY_CONSUMPTION < 0 | is.na(dat2$AVG_YEARLY_CONSUMPTION)
sum(indx)
# [1] 8819, 10618 for the high usage filters 
sum(is.na(dat2$AVG_YEARLY_CONSUMPTION))
# [1] 8819

dat2$AVG_YEARLY_CONSUMPTION[indx] = NA
dat2$CONSUMPTION_VAR[indx] = NA

dat2$CATGRY[is.na(dat2$CATGRY)] <-'NULL'
# if usage varibility is very high then assume an error and set it to NA
# dat2$AVG_YEARLY_CONSUMPTION[dat2$CONSUMPTION_VAR>5] = NA
# dat2$CONSUMPTION_VAR[dat2$CONSUMPTION_VAR>5] = NA

# get a subset of the raw data without any missing values
dat4 <- na.omit(dat2[,c(which(colnames(dat2)=='AVG_YEARLY_CONSUMPTION'),
                        which(colnames(dat2)=='ELEC'),
                        which(colnames(dat2)=='PAYMENT_CHANNEL_CATEGORY'), 
                        which(colnames(dat2)=='EBILL'),
                        which(colnames(dat2)=='DD_PAY'),
                        which(colnames(dat2)=='ROI'),
                        which(colnames(dat2)=='MARKETING_OPT_IN'),
                        which(colnames(dat2)=='REGISTER_DESC'),
                        which(colnames(dat2)=='DF_SF'),
                        which(colnames(dat2)=='CATGRY'),
                        which(colnames(dat2)=='EXPRT'))])

# calculates the median usage by cohort and store it in a table
sum_table = aggregate(dat4$AVG_YEARLY_CONSUMPTION, 
                      list(dat4$ELEC, 
                           dat4$PAYMENT_CHANNEL_CATEGORY,
                           dat4$EBILL, 
                           dat4$DD_PAY, 
                           dat4$ROI, 
                           dat4$MARKETING_OPT_IN, 
                           dat4$REGISTER_DESC,
                           dat4$DF_SF,
                           dat4$CATGRY,
                           dat4$EXPRT), 
                      FUN = median)

# label the median usage table for correct merge
colnames(sum_table) = c(names(dat4) [2:length(names(dat4))], 'MED_ANNUAL_USAGE')

dat2 <- sqldf("SELECT *
             FROM dat2 as l
             LEFT JOIN sum_table as r
             on l.ELEC = r.ELEC
             and l.PAYMENT_CHANNEL_CATEGORY = r.PAYMENT_CHANNEL_CATEGORY
             and l.EBILL = r.EBILL 
             and l.DD_PAY = r.DD_PAY 
             and l.ROI = r.ROI 
             and l.MARKETING_OPT_IN = r.MARKETING_OPT_IN 
             and l.REGISTER_DESC = r.REGISTER_DESC
             and l.DF_SF = r.DF_SF
             and l.CATGRY = r.CATGRY
             and l.EXPRT = r.EXPRT")

dat2 <- dat2[, -c(max(which(colnames(dat2) == 'ELEC')),
                  max(which(colnames(dat2) == 'PAYMENT_CHANNEL_CATEGORY')),
                  max(which(colnames(dat2) == 'EBILL')),
                  max(which(colnames(dat2) == 'DD_PAY')),
                  max(which(colnames(dat2) == 'ROI')),
                  max(which(colnames(dat2) == 'MARKETING_OPT_IN')),
                  max(which(colnames(dat2) == 'REGISTER_DESC')),
                  max(which(colnames(dat2) == 'DF_SF')),
                  max(which(colnames(dat2) == 'CATGRY')),
                  max(which(colnames(dat2) == 'EXPRT')))]


# replace NA's with median usage
dat2$AVG_YEARLY_CONSUMPTION[is.na(dat2$AVG_YEARLY_CONSUMPTION)] <- dat2$MED_ANNUAL_USAGE[is.na(dat2$AVG_YEARLY_CONSUMPTION)]

summary(dat2$AVG_YEARLY_CONSUMPTION)
# Min.   1st Qu.    Median      Mean   3rd Qu.      Max.      NA's 
#    0      2614      6864     44481     17234 436084957       862

summary(dat2$MED_ANNUAL_USAGE)
# Min. 1st Qu.  Median    Mean 3rd Qu.    Max.    NA's 
#    0    5795    6702   11301   10118  518315     862

table(dat2$REGISTER_DESC, is.na(dat2$AVG_YEARLY_CONSUMPTION), useNA = 'ifany')
#                         FALSE  TRUE
#  N/A                     1535   105
# 24hr                    19722   651
# Day                      8871     0
# GAS_24HR                 1997    66
# Heating                   591     0
# Heating Auto               19     1
# Low                       528     1
# Night                    8676     0
# Normal                    528     1
# NSH                      1900     6
# Off Peak 11 Hour           34     1
# Off Peak 15 Hour           28     1
# Off Peak 8 Hour            32     0
# Popular                  9482    27
# Summer Weekday Shoulder     0     1
# Winter Weekday Shoulder     0     1

table(dat2$DG, is.na(dat2$AVG_YEARLY_CONSUMPTION), useNA = 'ifany')
#      FALSE  TRUE
# CBC   1997     0
# DG1   2229    32
# DG2   1149    16
# DG5  31462    66
# DG6   2037    28
# T031  9172   309
# T032  1126    19
# T033  1089    31
# T034  2094    22
# T041    66     1
# T042    66     2
# T043    53     2
# <NA>  1403   334

table(dat2$CATGRY, is.na(dat2$AVG_YEARLY_CONSUMPTION), useNA = 'ifany')
#            FALSE  TRUE
# CBC         1997     0
# DG1         2229    32
# DG2         1149    16
# DG5           25    19
# GP         19134    29
# GPNS       12303    18
# LVMD        2037    28
# Nightsaver  3220    41
# NULL        1403   334
# Off-Peak     185     5
# Popular     9172   309
# Weekender   1089    31

sum_table[sum_table$MED_ANNUAL_USAGE == max(sum_table$MED_ANNUAL_USAGE), ]
#     ELEC PAYMENT_CHANNEL_CATEGORY EBILL DD_PAY ROI MARKETING_OPT_IN REGISTER_DESC DF_SF CATGRY
# 198    1                   Credit     1      Y   1                1           N/A    DF   LVMD
#     EXPRT MED_ANNUAL_USAGE
# 198     0           518315

summary(dat2$MED_ANNUAL_USAGE[dat2$REGISTER_DESC == 'Day' & dat2$ELEC == 1])
# Min. 1st Qu.  Median    Mean 3rd Qu.    Max.    NA's
#    0    8114   10148   16131   11312  276113    3149

summary(dat2$MED_ANNUAL_USAGE[dat2$REGISTER_DESC == 'Night' & dat2$ELEC == 1])
# Min. 1st Qu.  Median    Mean 3rd Qu.    Max.   NA's
#   0    8114   10148   16131   11312  276113    3055

summary(dat2$MED_ANNUAL_USAGE[dat2$REGISTER_DESC == '24hr' & dat2$ELEC == 1])
# Min. 1st Qu.  Median    Mean 3rd Qu.    Max.    NA's
#   89    5743    5795    6053    6702   41904    8140 

summary(dat2$AVG_YEARLY_CONSUMPTION[dat2$REGISTER_DESC == 'Day' & dat2$ELEC == 1])
# Min. 1st Qu.  Median    Mean 3rd Qu.    Max.    NA's 
#    0    3922   10864   26846   27664 1438157     545

summary(dat2$AVG_YEARLY_CONSUMPTION[dat2$REGISTER_DESC == 'Night' & dat2$ELEC == 1])
# Min.  1st Qu.   Median     Mean  3rd Qu.      Max.    NA's
#   0      2284     6220    74963    13753 348172014     521

summary(dat2$AVG_YEARLY_CONSUMPTION[dat2$REGISTER_DESC == '24hr' & dat2$ELEC == 1])
# Min. 1st Qu.  Median    Mean 3rd Qu.    Max.    NA's
#    0    2425    6263   11117   11108 2610124    2803


# get a subset of the raw data without any missing values
dat4 <- na.omit(dat2[,c(which(colnames(dat2)=='CONSUMPTION_VAR'),
                        which(colnames(dat2)=='ELEC'),
                        which(colnames(dat2)=='PAYMENT_CHANNEL_CATEGORY'), 
                        which(colnames(dat2)=='EBILL'),
                        which(colnames(dat2)=='DD_PAY'),
                        which(colnames(dat2)=='ROI'),
                        which(colnames(dat2)=='MARKETING_OPT_IN'),
                        which(colnames(dat2)=='REGISTER_DESC'),
                        which(colnames(dat2)=='DF_SF'),
                        which(colnames(dat2)=='CATGRY'),
                        which(colnames(dat2)=='EXPRT'))])

# calculates the median usage by cohort and store it in a table
sum_table2 = aggregate(dat4$CONSUMPTION_VAR, 
                       list(dat4$ELEC, 
                            dat4$PAYMENT_CHANNEL_CATEGORY,
                            dat4$EBILL, 
                            dat4$DD_PAY, 
                            dat4$ROI, 
                            dat4$MARKETING_OPT_IN, 
                            dat4$REGISTER_DESC,
                            dat4$DF_SF,
                            dat4$CATGRY,
                            dat4$EXPRT), 
                       FUN = median)

# label the median usage table for correct merge
colnames(sum_table2) = c(names(dat4) [2:length(names(dat4))], 'MED_CONSUM_VAR')

# left join
dat2 <- sqldf("SELECT *
             FROM dat2 as l
             LEFT JOIN sum_table2 as r
             on l.ELEC = r.ELEC
             and l.PAYMENT_CHANNEL_CATEGORY = r.PAYMENT_CHANNEL_CATEGORY
             and l.EBILL = r.EBILL 
             and l.DD_PAY = r.DD_PAY 
             and l.ROI = r.ROI 
             and l.MARKETING_OPT_IN = r.MARKETING_OPT_IN 
             and l.REGISTER_DESC = r.REGISTER_DESC
             and l.DF_SF = r.DF_SF
             and l.CATGRY = r.CATGRY
             and l.EXPRT = r.EXPRT")

dat2 <- dat2[, -c(max(which(colnames(dat2) == 'ELEC')),
                  max(which(colnames(dat2) == 'PAYMENT_CHANNEL_CATEGORY')),
                  max(which(colnames(dat2) == 'EBILL')),
                  max(which(colnames(dat2) == 'DD_PAY')),
                  max(which(colnames(dat2) == 'ROI')),
                  max(which(colnames(dat2) == 'MARKETING_OPT_IN')),
                  max(which(colnames(dat2) == 'REGISTER_DESC')),
                  max(which(colnames(dat2) == 'DF_SF')),
                  max(which(colnames(dat2) == 'CATGRY')),
                  max(which(colnames(dat2) == 'EXPRT')))]


# replace NA's with median usage
dat2$CONSUMPTION_VAR[is.na(dat2$CONSUMPTION_VAR)] <- dat2$MED_CONSUM_VAR[is.na(dat2$CONSUMPTION_VAR)]


hist(dat2$AVG_YEARLY_CONSUMPTION [dat2$AVG_YEARLY_CONSUMPTION < 2e4], 
     xlab = 'Annual Consumption (KWh)',
     main = 'Home Energy Annual Usage Histogram',
     breaks = 500)

hist(dat2$CONSUMPTION_VAR [dat2$CONSUMPTION_VAR < 3], 
     xlab = 'Consumption (Std.Dev./Avg.)',
     main = 'Home Energy Meter Read (Std.Dev./Avg) Histogram',
     breaks = 500)

# summary(dat2)
# ELEC       PAYMENT_CHANNEL_CATEGORY EBILL      DD_PAY     ROI        MARKETING_OPT_IN CITY      
# 0: 67569    N/A  :     0            N: 70568   N: 55181   0:113106   0:101400         0:250300  
# 1:334894   Credit:368987            Y:331895   Y:347282   1:289357   1:301063         1:152163  
#            Prepay: 33476                                                                        
# 
# 
# 
# 
# REGISTER_DESC      DF_SF       EXPRT              ACQ_CHANNEL      CUSTOMER_ID     
# Length:402463      DF:134271   0:402463   Door to Door   :248009   Min.   : 100879  
# Class :character   SF:268192              Call Centre    : 62504   1st Qu.:1144725  
# Mode  :character                          Homemoves      : 35159   Median :1953171  
#                                           Online         : 33156   Mean   :1700755  
#                                           Online - ICS   :  8797   3rd Qu.:2232061  
#                                           Property Button:  6335   Max.   :2406956  
#                                           (Other)        :  8503                    
# PREMISE_ID          MPRN           UTILITY_TYPE_CODE  AVG_YEARLY_CONSUMPTION CONSUMPTION_VAR    
# Min.   :   1302   Length:402463      Length:402463      Min.   :  -296         Min.   :   0.0000  
# 1st Qu.: 471163   Class :character   Class :character   1st Qu.:  1995         1st Qu.:   0.4200  
# Median : 793552   Mode  :character   Mode  :character   Median :  3227         Median :   0.5900  
# Mean   : 782206                                         Mean   :  4392         Mean   :   0.6764  
# 3rd Qu.:1128628                                         3rd Qu.:  5104         3rd Qu.:   0.8200  
# Max.   :1325565                                         Max.   :730527         Max.   :2407.0300  
# 
# METER_TYPE        JURISDICTION       CREDIT      CON_END_MTHS      RATE_NOW         RATE_OFF_CON      
# Length:402463      Length:402463      0: 33476   Min.   : 0.00   Min.   :0.00000   Min.   :0.00000  
# Class :character   Class :character   1:368987   1st Qu.: 0.00   1st Qu.:0.04000   1st Qu.:0.04000  
# Mode  :character   Mode  :character              Median : 0.00   Median :0.04000   Median :0.04000  
#                                                  Mean   : 1.89   Mean   :0.05754   Mean   :0.03768  
#                                                  3rd Qu.: 3.00   3rd Qu.:0.10000   3rd Qu.:0.04000  
#                                                  Max.   :60.00   Max.   :0.10000   Max.   :0.06000  
# 
# SF_COMM         DF_COMM       MED_ANNUAL_USAGE  MED_CONSUM_VAR 
# Min.   : 0.00   Min.   :  0.00   Min.   :   51.5   Min.   :0.370  
# 1st Qu.:46.00   1st Qu.: 65.71   1st Qu.: 2425.0   1st Qu.:0.500  
# Median :80.00   Median :114.40   Median : 3300.0   Median :0.550  
# Mean   :58.88   Mean   : 84.97   Mean   : 3799.1   Mean   :0.616  
# 3rd Qu.:80.00   3rd Qu.:114.40   3rd Qu.: 3651.0   3rd Qu.:0.690  
# Max.   :80.00   Max.   :114.40   Max.   :14778.0   Max.   :2.580 

# drop the median usage and median variability columns
dat2 = dat2[,-c(which(colnames(dat2)=='MED_ANNUAL_USAGE'),
                which(colnames(dat2)=='MED_CONSUM_VAR'))]

month_day = paste0(format(Sys.time(), "%b"), format(Sys.time(), "%d"))
# write.csv(dat2, paste0("CLV_usage_",month_day,".csv"))

indx = dat2$JURISDICTION == 'ROI' & dat2$UTILITY_TYPE_CODE == 'E'
summary(dat2[indx,])


table(dat2$ACQ_CHANNEL, useNA = 'ifany')

#  Anything  Commercial TPI             DOM    Door to Door        Employee 
#      1220           15647            2466            3955             920 
# Homemoves          Online           Other Property Button          Rigney 
#        16             208            2669              87            3238 
#       SME   SME Telesales         Unknown 
#       443            1881            1060 


hist(dat2$CTA[dat2$CTA>0], breaks= 50)

# setwd("S:/Retail/IRL/Supply/Marketing/MARKETING SUPPLY/Analytics & Products/Analytics/MM WIP/BE Value Model")

# dat2$REGISTER_DESC = as.factor(dat2$REGISTER_DESC)

# DISC$DISC = DISC$DISC/100
# 
# # add leading zeros when the MPRN is 6 digits or less
# DISC$MPRN[nchar(DISC$MPRN) < 7] = formatC(DISC$MPRN[nchar(DISC$MPRN) < 7], width = 7, format = "d", flag = "0")
# 
# ##########################################
# 
# 
# # dat3 = merge(dat2, DISC, by = c('CUSTOMER_ID', 
# #                                 'PREMISE_ID',
# #                                 'MPRN', 
# #                                 'UTILITY_TYPE_CODE',
# #                                 'JURISDICTION',
# #                                 'SERVICENUM',
# #                                 'CATGRY',
# #                                 'DG',
# #                                 'PROFILE_E'),
# #              all.x = T, all.y = F)
# 
# dat3 = dat2
# 
# dat2 <- sqldf("SELECT *
#              FROM dat2 as l
#              INNER JOIN DISC as r
#              on l.CUSTOMER_ID = r.CUSTOMER_ID
#              and l.PREMISE_ID = r.PREMISE_ID
#              and trim(l.MPRN) = trim(r.MPRN)
#              and trim(l.UTILITY_TYPE_CODE) = trim(r.UTILITY_TYPE_CODE)
#              and trim(l.JURISDICTION) = trim(r.JURISDICTION)
#              and trim(l.SERVICENUM) = trim(r.SERVICENUM)
#              and trim(l.REGISTERNUM) = trim(r.REGISTERNUM)
#              and trim(l.CATGRY) = trim(r.CATGRY)
#              and trim(l.PROFILE_E) = trim(r.PROFILE_E)
#              and trim(l.DG) = trim(r.DG)
#              ")
# 
# 
# dat2 <- dat2[, -c(max(which(colnames(dat2) == 'CUSTOMER_ID')),
#                   max(which(colnames(dat2) == 'PREMISE_ID')),
#                   max(which(colnames(dat2) == 'MPRN')),
#                   max(which(colnames(dat2) == 'UTILITY_TYPE_CODE')),
#                   max(which(colnames(dat2) == 'JURISDICTION')),
#                   max(which(colnames(dat2) == 'SERVICENUM')),
#                   max(which(colnames(dat2) == 'REGISTERNUM')),
#                   max(which(colnames(dat2) == 'CATGRY')),
#                   max(which(colnames(dat2) == 'PROFILE_E')),
#                   max(which(colnames(dat2) == 'DG')))]
# 
# dat5 = dat2[dat2$REGISTER_DESC != dat2$REGISTER_DESC.1, ]
# # dat6 = dat3[dat3$CATGRY != dat3$CATGRY.1, ]
# # dat7 = dat3[dat3$PROFILE_E != dat3$PROFILE_E.1, ]
# # dat8 = dat3[dat3$DG != dat3$DG.1, ]
# 
# dim(dat5) [1] # count of register differences = 8715
# 
# table (dat2$REGISTER_DESC, dat2$REGISTER_DESC.1, useNA = 'ifany')
# # table (dat3$CATGRY, dat3$CATGRY.1, useNA = 'ifany')
# # table (dat3$DG, dat3$DG.1, useNA = 'ifany')
# # table (dat3$PROFILE_E, dat3$PROFILE_E.1, useNA = 'ifany')
# 
# dat2$REGISTER_DESC = dat2$REGISTER_DESC.1
# dat2 <- dat2[, -which(colnames(dat2) == 'REGISTER_DESC.1')]
#                   
# table(dat2$DG, useNA = 'ifany')
# table(dat2$DG, dat2$UTILITY_TYPE_CODE, useNA = 'ifany')
# 
# table(dat2$REGISTER_DESC)
# # 24hr              Day         GAS_24HR          Heating     Heating Auto              Low            Night 
# # 26034             7642             1764              523                9              523             7519 
# # Normal              NSH Off Peak 11 Hour Off Peak 15 Hour  Off Peak 8 Hour 
# # 516             1778               26               18               27
# 
# table(dat2$DG, dat2$JURISDICTION,   useNA = 'ifany')

dat2$DG[is.na(dat2$DG) & dat2$UTILITY_TYPE_CODE=='G'] <- 'CBC'

# if NA & out of contract then replace with the RATE_OFF_CON value
sum(is.na(dat2$DISC) & dat2$CON_END_MTHS>=0) # 45568
dat2$DISC[is.na(dat2$DISC) & dat2$CON_END_MTHS>=0] = dat2$RATE_OFF_CON[is.na(dat2$DISC)& dat2$CON_END_MTHS>=0]
sum(is.na(dat2$DISC) & dat2$CON_END_MTHS>=0) # 0
# 0

table(dat2$DISC, useNA = 'ifany')

#   -7.69     0  0.01  0.02  0.04  0.05  0.06  0.99     2  2.94  3.03  3.06  3.07  3.11  3.13  4.05  4.18  4.25  5.16  5.25  5.99 
#       1 15182  1472 18643  7162  4726  3367     2     1     1     1     6     1     1     1     2     2     7     2     1     5 
#    6.22  6.24  6.28  6.33  8.22  8.26  8.28  8.99  9.11  9.35  9.57    10 11.43 11.45 11.48 12.48 12.65 13.37 13.41 13.43 15.15 
#       8    15     8     4     9     3     2    12     1     9    31     1    12    64    18     2    17    39    13    11     2 
#   16.63 16.66 16.68 16.73 19.55  19.8 19.81 19.83  22.9 22.91 22.97 27.07 27.09 
#       2    22     2     2     3    19    23    19    11     6     5     1     6 

dat2$DISC[dat2$DISC<0] = 0

setwd("S:/Retail/IRL/Supply/Marketing/MARKETING SUPPLY/Analytics & Products/Analytics/MM WIP/BE Value Model")

# STD_RATES <- read.csv('BE_STD_RATES_AUG_2022.csv', header = T, skip = 0)
# Added the code to fetch CTA data directly from DM2.0 Analytics
conn <- dbConnect(drv, username =  dbname = connect.string, prefetch = FALSE,
                  bulk_read = 1000L, stmt_cache = 0L, external_credentials = FALSE,
                  sysdba = FALSE)
query_res_std_rates <- dbGetQuery(conn, "select * from BE_STD_RATES");
STD_RATES <- as.data.frame(query_res_std_rates)
dbDisconnect(conn)
STD_RATES = STD_RATES[,-which(colnames(STD_RATES) == 'PROFILE_E')]

dat2 = merge(dat2, STD_RATES, by = c('JURISDICTION', 
                                     'UTILITY_TYPE_CODE',
                                     'REGISTER_DESC',
                                     'CATGRY',
                                     'DG'),
             all.x = T, all.y = F)


sum(is.na(dat2$STD_RATE)) # 942
table(dat2$JURISDICTION[is.na(dat2$STD_RATE)], useNA = 'ifany') # all NI
table(dat2$UTILITY_TYPE_CODE[is.na(dat2$STD_RATE)], useNA = 'ifany') # all elec
table(dat2$REGISTER_DESC[is.na(dat2$STD_RATE)], dat2$JURISDICTION[is.na(dat2$STD_RATE)], useNA = 'ifany')
table(dat2$CATGRY[is.na(dat2$STD_RATE)], useNA = 'ifany')
table(dat2$DG[is.na(dat2$STD_RATE)], useNA = 'ifany')
table(dat2$REGISTER_DESC[is.na(dat2$STD_RATE)], dat2$DG[is.na(dat2$STD_RATE)], dat2$JURISDICTION[is.na(dat2$STD_RATE)], useNA = 'ifany')
# , ,  = NI
# 
# 
#          T011 T012 T014 T015 T021 T022 T024 T031 T034 T041 T042 T043 T052 T053 T062 T063
# 24hr      535    0    0    0   21    0    0    0    1   40   39   30    0   22    1    3
# Day         0   10   12    8    2   15    3    1    0    0    0    0   10    0    0    0
# Heating     0    0   10    8    0    0    3    1    0    0    0    0    0    0    0    0
# Night       0   10   11    8    2   15    3    1    0    0    0    0    2    0    0    0
# Off Peak    0    0    0    0    0    0    0    1    0   38   38   29    0    0    1    3

std_rte_tab = as.data.frame(table(dat2$JURISDICTION, dat2$UTILITY_TYPE_CODE, dat2$REGISTER_DESC, dat2$CATGRY, dat2$DG, useNA = 'ifany'))
# write.csv(std_rte_tab, paste0("BE_std_rte_tab_",day_month_yr,".csv"), row.names = F )

# write.csv(dat2, paste0("BE_CLV_usage_",day_month_yr,".csv"), row.names = F )


# PROMO_CODES <- read.csv('PROMOCODES_DATA_AUG22.csv', header = T, skip = 0)
conn <- dbConnect(drv, username = , dbname = connect.string, prefetch = FALSE,
                  bulk_read = 1000L, stmt_cache = 0L, external_credentials = FALSE,
                  sysdba = FALSE)
query_res_promo_codes <- dbGetQuery(conn, "select * from BE_PROMO_CODES");
PROMO_CODES <- as.data.frame(query_res_promo_codes)
dbDisconnect(conn)
# PROMO_CODES = rename(PROMO_CODES, 'PROMO_CODE' = 'ï..PROMO_CODE')
PROMO_CODES <- data.frame(lapply(PROMO_CODES, function(x) if(class(x)=="character") trimws(x) else(x)), stringsAsFactors=F)
table(PROMO_CODES$CATGRY, useNA = 'ifany')
#   LVMD Nightsaver      P5         P6      Popular  Weekender 
# 3656       3736       5452       5452       3736       3736

table(PROMO_CODES$PROMO_CODE, useNA = 'ifany')

dat2 = merge(dat2, PROMO_CODES, by = c('PROMO_CODE', 
                                       'CATGRY'),
             all.x = T, all.y = F)

table(!is.na(dat2$PROMO_CODE), dat2$GR_MARGIN, useNA = 'ifany')
#        0.05 0.065  0.07 0.085  <NA>
# FALSE     0     0     0     0 44775
# TRUE   1469   327  2230  2077   110

table(!is.na(dat2$PROMO_CODE), dat2$CATGRY, dat2$GR_MARGIN, useNA = 'ifany')
# , ,  = 0.05
# 
# 
#         CBC   DG1   DG2    GP  GPNS Nightsaver Off-Peak Popular  T011  T012  T014  T015  T021  T022  T024  T052  T053  T062  T063 Weekender
# FALSE     0     0     0     0     0          0        0       0     0     0     0     0     0     0     0     0     0     0     0         0
# TRUE      0     0     0     0     0        757        0     437     0     0     0     0     0     0     0     0     0     0     0         0
# 
# , ,  = 0.065
# 
# 
#         CBC   DG1   DG2    GP  GPNS Nightsaver Off-Peak Popular  T011  T012  T014  T015  T021  T022  T024  T052  T053  T062  T063 Weekender
# FALSE     0     0     0     0     0          0        0       0     0     0     0     0     0     0     0     0     0     0     0         0
# TRUE      0     0     0     0     0          0        0     334     0     0     0     0     0     0     0     0     0     0     0         0
# 
# , ,  = 0.07
# 
# 
#         CBC   DG1   DG2    GP  GPNS Nightsaver Off-Peak Popular  T011  T012  T014  T015  T021  T022  T024  T052  T053  T062  T063 Weekender
# FALSE     0     0     0     0     0          0        0       0     0     0     0     0     0     0     0     0     0     0     0         0
# TRUE      0     0     0     0     0       1791        0       0     0     0     0     0     0     0     0     0     0     0     0         0
# 
# , ,  = 0.085
# 
# 
#         CBC   DG1   DG2    GP  GPNS Nightsaver Off-Peak Popular  T011  T012  T014  T015  T021  T022  T024  T052  T053  T062  T063 Weekender
# FALSE     0     0     0     0     0          0        0       0     0     0     0     0     0     0     0     0     0     0     0         0
# TRUE      0     0     0     0     0          0        0    2064     0     0     0     0     0     0     0     0     0     0     0         0
# 
# , ,  = NA
# 
# 
#         CBC   DG1   DG2    GP  GPNS Nightsaver Off-Peak Popular  T011  T012  T014  T015  T021  T022  T024  T052  T053  T062  T063 Weekender
# FALSE  1866  1772   958 16907 10825       2123      106    6049   533    20    33    24    25    30     9    12    22     2     6       658
# TRUE      0     0     0     0     0          0      108       2     0     0     0     0     0     0     0     0     0     0     0       806

# table(dat2$PROMO_CODE[is.na(dat2$GR_MARGIN) & dat2$CATGRY == 'Weekender'], useNA = 'ifany')
# 
# table(dat2$PROMO_CODE[!is.na(dat2$GR_MARGIN) & dat2$CATGRY == 'Weekender'], useNA = 'ifany')

promo_diff = setdiff(dat2$PROMO_CODE, PROMO_CODES$PROMO_CODE); promo_diff
# [1] "MARK19K2"

promo_diff = promo_diff [-which(is.na(promo_diff))] # remove NA from the difference list
table(dat2$PROMO_CODE %in% promo_diff, useNA = 'ifany')
# FALSE  TRUE 
# 50987     1 

tab_promo = as.data.frame(table(dat2$PROMO_CODE[dat2$PROMO_CODE %in% promo_diff], useNA = 'ifany'))
colnames(tab_promo) = c('PROMO_CODE', 'COUNTS')
tab_promo <- tab_promo[ order( tab_promo[,2], decreasing = T ), ]
# write.csv(tab_promo, 'promo_code_in_DM2_not_in_lookup.csv', row.names = F)

dat2$promo_yrmth = substr(dat2$PROMO_CODE,1, 5)
dat2$promo_yr = substr(dat2$PROMO_CODE,4, 5)
dat2$promo_mth = factor(paste0(substr(dat2$PROMO_CODE,1, 1), tolower(substr(dat2$PROMO_CODE,2, 3))), month.abb)

# get the expected promo expiry date
table(dat2$CONTRACT_START_DATE, useNA = 'ifany')
dat2$CONTRACT_START_DATE <- as.POSIXct(format(as.POSIXct(dat2$CONTRACT_START_DATE,format='%Y-%m-%d %H:%M:%S'),format='%Y-%m-%d'))
dat2$CONTRACT_END_DATE <- as.POSIXct(format(as.POSIXct(dat2$CONTRACT_END_DATE,format='%Y-%m-%d %H:%M:%S'),format='%Y-%m-%d'))
table(dat2$CONTRACT_START_DATE, useNA = 'ifany')

promo_indx = !is.na(dat2$CONTRACT_START_DATE) & !is.na(dat2$NUM_YRS_CON) & grepl("^[1-9]+$", dat2$promo_yr)

y = dat2$CONTRACT_START_DATE[promo_indx]
x = dat2$NUM_YRS_CON[promo_indx]*12
dat2$promo_con_end =  rep(as.POSIXct('1900-01-01', format='%Y-%m-%d'), dim(dat2) [1])
dat2$promo_con_end[promo_indx] = AddMonths(y, x)
table(dat2$promo_con_end, useNA = 'ifany')

# check the difference between contract end date and calculated contract end date from the promocode
dat2$promo_con_end_diff = floor((dat2$promo_con_end - dat2$CONTRACT_END_DATE) / (24*60*60))
table(dat2$promo_con_end_diff[promo_indx], useNA = 'ifany')

dat2$PROMO_EXPIRY = trunc(as.POSIXlt(as.character(dat2$PROMO_EXPIRY),format="%d/%m/%Y"), units = 'day')
dat2$PROMO_EXPIRY2 =  trunc(rep(as.POSIXlt('1900-01-01 00:00:00', format='%Y-%m-%d %H:%M:%S'), dim(dat2) [1]), units = 'day')
dat2$PROMO_EXPIRY2[!is.na(dat2$PROMO_EXPIRY)] = dat2$PROMO_EXPIRY[!is.na(dat2$PROMO_EXPIRY)]

dat2$promo_con_end = trunc(dat2$promo_con_end, units = 'day')
dat2$promo_con_end_diff2 = (dat2$PROMO_EXPIRY2 - dat2$promo_con_end)
table(dat2$promo_con_end_diff2[promo_indx], useNA = 'ifany')


dat2$promo_expired <- ifelse(dat2$PROMO_EXPIRY>trunc(as.POSIXlt(Today(),format="%Y-$m-%d", tz = 'GMT'), units = 'day'), 'N', 'Y')
# promo_expired will contain expired promo codes as well as promo codes that don't match Chris's lookup

dat2$contract_expired <- ifelse((dat2$CONTRACT_END_DATE<trunc(as.POSIXlt(Today(),format="%Y-$m-%d", tz = 'GMT'), units = 'day') |
                                   is.na(dat2$CONTRACT_END_DATE)), 'Y', 'N')
table(dat2$promo_expired, useNA = 'ifany')
#     N     Y  <NA> 
#  4543   846 45857

table(dat2$contract_expired, useNA = 'ifany')
#     N     Y 
# 26386 24860

table(dat2$promo_expired, dat2$contract_expired, useNA = 'ifany')
#          N     Y
# N     4542     1
# Y      602   244
# <NA> 21242 24615

tab_promo = as.data.frame(table(dat2$PROMO_CODE[!dat2$PROMO_CODE %in% promo_diff & 
                                                  dat2$promo_expired == 'Y'], 
                                dat2$PROMO_EXPIRY2[!dat2$PROMO_CODE %in% promo_diff & 
                                                     dat2$promo_expired == 'Y'], 
                                dat2$CONTRACT_END_DATE[!dat2$PROMO_CODE %in% promo_diff & 
                                                         dat2$promo_expired == 'Y'], 
                                dat2$contract_expired[!dat2$PROMO_CODE %in% promo_diff & 
                                                        dat2$promo_expired == 'Y'], 
                                useNA = 'ifany'))
colnames(tab_promo) = c('PROMO_CODE', 'PROMO EXPIRY', 'CONTRACT EXPIRY', 'CON_EXPIRT_FLAG', 'COUNTS')
tab_promo <- tab_promo[ order( tab_promo[,5], decreasing = T ), ]
tab_promo <- tab_promo[tab_promo$COUNTS != 0 & !is.na(tab_promo$PROMO_CODE), ]
tab_promo
# write.csv(tab_promo, 'expired_promo_code_in_DM2.csv', row.names = F)
sum(tab_promo$COUNTS) # 1019 expired

tab_promo2 = as.data.frame(table(dat2$PROMO_CODE[!dat2$PROMO_CODE %in% promo_diff & 
                                                   dat2$promo_expired == 'Y'], 
                                 dat2$CONTRACT_END_DATE[!dat2$PROMO_CODE %in% promo_diff &
                                                          dat2$promo_expired == 'Y'], 
                                 useNA = 'ifany'))
colnames(tab_promo2) = c('PROMO_CODE', 'CONTRACT_EXPIRY', 'COUNTS')
tab_promo2 <- tab_promo2[ order( tab_promo2[,3], decreasing = T ), ]
tab_promo2 <- tab_promo2[tab_promo2$COUNTS != 0 & !is.na(tab_promo2$PROMO_CODE), ]
tab_promo2

sum(tab_promo2$COUNTS) # 1019 expired
# write.csv(tab_promo2, 'expired_promo_code_in_DM2.csv', row.names = F)

##################################################

table(dat2$CON_END_MTHS[dat2$PROMO_CODE %in% promo_diff], dat2$promo_mth [dat2$PROMO_CODE %in% promo_diff], dat2$promo_yr [dat2$PROMO_CODE %in% promo_diff], useNA = 'ifany')

# should be null!


tab_promo = table(dat2$PROMO_CODE[dat2$PROMO_CODE %in% promo_diff], dat2$CON_END_MTHS[dat2$PROMO_CODE %in% promo_diff], useNA = 'ifany')
# write.csv(tab_promo, 'promo_code_in_DM2_not_in_lookup.csv', row.names = F)

table(dat2$STD_RATE[!is.na(dat2$PROMO_CODE)], useNA = 'ifany')
# 0.1044 0.1261 0.1293 0.1528 0.1662 0.1893 0.2056 0.2268   <NA> 
#   1494    403     18     20     17   2898   1054    403      2  

table(dat2$STD_RATE[is.na(dat2$PROMO_CODE)], useNA = 'ifany')
# 0.0576 0.0988 0.1035 0.1044 0.1261 0.1293 0.1528 0.1662  0.187 0.1893 0.1996 0.2056 0.2069 0.2234 0.2268   <NA> 
#   1875    652   7804   1218    330     20     19     13   2047   6106    561    913  16124   6042    328    885   

table(dat2$STD_RATE[!is.na(dat2$PROMO_CODE) & dat2$PROMO_CODE %in% promo_diff & dat2$CON_END_MTHS == 0], useNA = 'ifany')
# < table of extent 0 >  

dat3 = dat2[is.na(dat2$STD_RATE) & is.na(dat2$PROMO_CODE), ]
getwd() # "S:/Retail/IRL/Supply/Marketing/MARKETING SUPPLY/Analytics & Products/Analytics/MM WIP/BE Value Model"
# write.csv(dat3, 'null_std_rate_no_promo_code.csv', row.names = F)

dat3 = dat2[is.na(dat2$STD_RATE) & !is.na(dat2$PROMO_CODE), ]
# write.csv(dat3, 'null_std_rate_with_promo_code.csv', row.names = F)

# change dat2 to dat3 in the merge statement above to trouble shoot merging issues
# table(dat2$REGISTER_DESC[is.na(dat2$STD_RATE) & !is.na(dat2$UNITRATE)], useNA = 'ifany')
# 24HR   DAY NIGHT 
#    5     2     2
# table(dat2$JURISDICTION[is.na(dat2$STD_RATE) & !is.na(dat2$UNITRATE)], useNA = 'ifany')
# ROI 
#   9 
# table(dat2$PAYMENT_CHANNEL_CATEGORY[is.na(dat2$STD_RATE) & !is.na(dat2$UNITRATE)], useNA = 'ifany')
# Credit Prepay 
#      9      0
# table(dat2$DF_SF[is.na(dat2$STD_RATE) & !is.na(dat2$UNITRATE)], useNA = 'ifany')
# DF SF 
#  0  9  
# table(dat2$UTILITY_TYPE_CODE[is.na(dat2$STD_RATE) & !is.na(dat2$UNITRATE)], useNA = 'ifany')
# E 
# 9
# table(dat2$DG[is.na(dat2$STD_RATE) & !is.na(dat2$UNITRATE)], useNA = 'ifany')
# CBC DG1 DG2 DG5 DG6  NI 
#   0   0   0   0   0   9

# there are 13 records that aren't merging where there's a clash between the jurisdiction and the meter DG code
# dat2$CUSTOMER_ID[is.na(dat2$STD_RATE) & !is.na(dat2$UNITRATE)]
# [1]  917816  917816  917816 2275889  917816 1069804 2060978 1069804 2060978

sum(is.na(dat2$UNITRATE)) # 0
sum(is.na(dat2$STD_RATE)) #942

sum(!is.na(dat2$UNITRATE) & !is.na(dat2$PROMO_CODE)) # 6309
sum(is.na(dat2$UNITRATE) & !is.na(dat2$PROMO_CODE)) # 0
sum(!is.na(dat2$STD_RATE) & !is.na(dat2$PROMO_CODE)) # 6307
sum(is.na(dat2$STD_RATE) & !is.na(dat2$PROMO_CODE)) # 2

table(!is.na(dat2$PROMO_CODE), dat2$STD_RATE, useNA = 'ifany')
#       0.0576 0.0988 0.1035 0.1044 0.1261 0.1293 0.1528 0.1662 0.187 0.1893 0.1996 0.2056 0.2069 0.2234 0.2268  <NA>
# FALSE   1875    652   7804   1218    330     20     19     13  2047   6106    561    913  16124   6042    328   885
# TRUE       0      0      0   1494    403     18     20     17     0   2898      0   1054      0      0    403     2


table(!is.na(dat2$PROMO_CODE), dat2$UNITRATE, useNA = 'ifany')

# replace NA's or zeros with std_rate
dat2$UNITRATE [(is.na(dat2$UNITRATE)|dat2$UNITRATE <= 0) & is.na(dat2$PROMO_CODE)] <- dat2$STD_RATE [(is.na(dat2$UNITRATE)|dat2$UNITRATE <= 0) & is.na(dat2$PROMO_CODE)]
sum(is.na(dat2$UNITRATE)) #  0
sum(dat2$UNITRATE <= 0) # 6721

# calculate the discount
dat2$DISC2 = round(1 - dat2$UNITRATE/dat2$STD_RATE, 3)

table(!is.na(dat2$PROMO_CODE), dat2$DISC2, useNA = 'ifany')
dat2[!is.na(dat2$PROMO_CODE) & dat2$DISC2 < 0, ]

# if still in contract replace rate_now with contract discount
dat2$RATE_NOW = dat2$RATE_OFF_CON
table(dat2$RATE_NOW, useNA = 'ifany')
#     0  0.01  0.02  0.04  0.05  0.06 
# 13666  1370 17729  7166  4854  3555

dat2$RATE_NOW[dat2$CON_END_MTHS > 0 & !is.na(dat2$DISC2)] = dat2$DISC2[dat2$CON_END_MTHS > 0 & !is.na(dat2$DISC2)]

# if the discount is extreme or negative or smaller than the 
# out of contract discount then replace with the out of contract discount rate
dat2$RATE_NOW[dat2$RATE_NOW > 0.35 | dat2$RATE_NOW < 0 |dat2$RATE_NOW < dat2$RATE_OFF_CON] = dat2$RATE_OFF_CON[dat2$RATE_NOW > 0.35 | dat2$RATE_NOW < 0 | dat2$RATE_NOW < dat2$RATE_OFF_CON]

dat2$RATE_NOW = round(dat2$RATE_NOW,2)
# table(dat2$RATE_NOW, useNA = 'ifany')
# 
#     0  0.01  0.02  0.03  0.04  0.05  0.06  0.08   0.1  0.11  0.15  0.16  0.18  0.19   0.2  0.21  0.22  0.23  0.25  0.26  0.27 
# 46624     6   189    12   103    45     8     5    41    13    89   176   169    67  2332   486     3   151    51    11   621 
# 0.29   0.3 
# 41     3

hist(dat2$RATE_NOW[dat2$RATE_NOW > 0], breaks = 100)

# write.csv(dat2, paste0("CLV_usage_",month_day,".csv"))

# table(dat2$CON_END_MTHS, dat2$RATE_NOW)
# table(dat2$UTILITY_TYPE_CODE, dat2$CON_END_MTHS)
# table(dat2$PAYMENT_CHANNEL_CATEGORY, dat2$REGISTER_DESC)
# table(dat2$REGISTER_DESC, dat2$CON_END_MTHS)
# table(dat2$JURISDICTION)
# table(dat2$RATE[dat2$CON_END_MTHS == 5 & dat2$EBILL == 'N' & dat2$DD_PAY == 'N' & dat2$JURISDICTION == 'ROI'])


setwd("S:/Retail/IRL/Supply/Marketing/MARKETING SUPPLY/Analytics & Products/Analytics/MM WIP/BE Value Model")
day_month_yr = paste0(format(Sys.time(), "%d"), format(Sys.time(), "%b"), format(Sys.time(), "%y"))
churn_rte = dat_churn_rates
# churn_rte = read.csv(paste0("BE_CLV_ret_",day_month_yr,".csv"), header = T)
# churn_rte = churn_rte[,-which(colnames(churn_rte)=='X')]
#churn_rte$MPRN = formatC(churn_rte$MPRN, width = 7, format = "d", flag = "0")

# # test that we have the expected number of meters
# churn_rte2 = churn_rte[, c(which(colnames(churn_rte)=='MPRN'),
#                which(colnames(churn_rte)=='UTILITY_TYPE_CODE'))]
# 
# churn_rte3 = distinct(churn_rte2)
# 
# CNT = function(x, na.rm = T) sum( !is.na(x) ) # gets the counts
# 
# mu <- churn_rte3 %>%
#   group_by(UTILITY_TYPE_CODE) %>%
#   summarize_at ('MPRN', funs(grp.count = CNT), na.rm = T)
# mu
# A tibble: 2 x 2
# UTILITY_TYPE_CODE grp.count
#   <fct>                 <int>
# 1 E                    324778
# 2 G                     65713

# churn_rte = churn_rte[,-c(which(colnames(churn_rte) == 'X'),
#                          # which(colnames(churn_rte) == 'REGISTERNUM'),
#                          # which(colnames(churn_rte) == 'DG'),
#                           which(colnames(churn_rte) == 'PROFILE_E'))]

churn_rte = unique(churn_rte)
churn_rte = churn_rte[!is.na(churn_rte$MPRN) & churn_rte$MPRN != ' N/A' & !is.na(churn_rte$CUSTOMER_ID) & churn_rte$CUSTOMER_ID > 0 & !is.na(churn_rte$PREMISE_ID) & churn_rte$PREMISE_ID > 0, ]
dat2 = unique(dat2)

# REGISTER_DESC from DM2 has errors, so don't use it to join and use value from tariffSetup reports
churn_rte = churn_rte[, -c(which(colnames(churn_rte)== 'DG'),
                           which(colnames(churn_rte)== 'PROFILE')
                           #,which(colnames(churn_rte)== 'REGISTER_DESC')
)]
# # left join
# dat3 <- sqldf("SELECT *
#              FROM dat2 as l
#              LEFT JOIN churn_rte as r
#              on l.CUSTOMER_ID = r.CUSTOMER_ID
#              and l.PREMISE_ID = r.PREMISE_ID
#              and l.MPRN = r.MPRN 
#              and l.SERVICENUM = r.SERVICENUM
#              and l.REGISTER_CODE = r.REGISTER_CODE 
#              and l.REGISTER_DESC = r.REGISTER_DESC
#              and l.DG = r.DG
#              and l.UTILITY_TYPE_CODE = r.UTILITY_TYPE_CODE")
# 
# dat2 <- dat2[, -c(max(which(colnames(dat2) == 'CUSTOMER_ID')),
#                   max(which(colnames(dat2) == 'REMISE_ID')),
#                   max(which(colnames(dat2) == 'MPRN')),
#                   max(which(colnames(dat2) == 'SERVICENUM')),
#                   max(which(colnames(dat2) == 'REGISTERNUM')),
#                   max(which(colnames(dat2) == 'REGISTER_DESC')),
#                   max(which(colnames(dat2) == 'DG')),
#                   max(which(colnames(dat2) == 'UTILITY_TYPE_CODE')))]

dat2 = merge(dat2, churn_rte, by = c('CUSTOMER_ID',
                                     'PREMISE_ID',
                                     'MPRN',
                                     'SERVICENUM',
                                     'REGISTERNUM',
                                     'REGISTER_DESC',
                                     'UTILITY_TYPE_CODE'),
             all.x = T, all.y = F)



# small error as we gained 10 rows!

sum(is.na(dat2$avg_churn_5y))


# [1] 8
sum(is.na(churn_rte$avg_churn_5y))
# [1] 0

table(dat2$REGISTER_DESC[is.na(dat2$avg_churn_5y)], useNA = 'ifany' )
# Day  Heating Off Peak 
#   2        5        1

table(dat2$UTILITY_TYPE_CODE[is.na(dat2$avg_churn_5y)], useNA = 'ifany' )
#   E 
#   8  

table(dat2$DG[is.na(dat2$avg_churn_5y)], useNA = 'ifany' )
# T031 T034 
#    1    7

setdiff(churn_rte$CUSTOMER_ID, dat2$CUSTOMER_ID)
# 1159

setdiff(dat2$CUSTOMER_ID,churn_rte$CUSTOMER_ID)
# 0

num = dim(dat2) [1]

# churn rate for duration of contract
dat2$con_y1h1 = rep(0L,num)
dat2$con_y1h2 = rep(0L,num) 
dat2$con_m12 = rep(0L,num) 
dat2$con_y2h1 = rep(0L,num) 
dat2$con_y2h2 = rep(0L,num) 
dat2$con_m24 = rep(0L,num) 
dat2$con_y3p = rep(0L,num) 
dat2$CON_END_MTHS = as.integer(dat2$CON_END_MTHS)

# sum(dat2$TENURE_MTHS <= 6 & dat2$CON_END_MTHS > 0 & !is.na(dat2$TENURE_MTHS))
# head(dat2[dat2$TENURE_MTHS <= 6 & dat2$CON_END_MTHS > 0 & !is.na(dat2$TENURE_MTHS),c('TENURE_MTHS', 'CON_END_MTHS', colnames(dat2)[grep("con",colnames(dat2))])],10)

table(is.na(dat2$PROMO_CODE), dat2$CON_END_MTHS > 0, useNA = 'ifany')
#       FALSE  TRUE
# FALSE   263  5950
# TRUE  25279 19496

indx = !is.na(dat2$TENURE_MTHS)

dat2$con_y1h1[indx & dat2$TENURE_MTHS < 6 & dat2$CON_END_MTHS > 0] =  pmin((6L  - 
                                                                              dat2$TENURE_MTHS[indx & dat2$TENURE_MTHS < 6 & dat2$CON_END_MTHS > 0]), 
                                                                           dat2$CON_END_MTHS[indx & dat2$TENURE_MTHS < 6 & dat2$CON_END_MTHS > 0],na.rm=T)

dat2$con_y1h2 [indx & dat2$TENURE_MTHS == 11 & dat2$CON_END_MTHS > 0] = pmin((11L - 
                                                                               dat2$TENURE_MTHS [indx & dat2$TENURE_MTHS < 11 & dat2$CON_END_MTHS > 0] - 
                                                                               dat2$con_y1h1 [indx & dat2$TENURE_MTHS < 11 & dat2$CON_END_MTHS > 0]), 
                                                                            (dat2$CON_END_MTHS[indx & dat2$TENURE_MTHS < 11 & dat2$CON_END_MTHS > 0] - 
                                                                               dat2$con_y1h1 [indx & dat2$TENURE_MTHS < 11 & dat2$CON_END_MTHS > 0]),na.rm=T)

dat2$con_m12 [indx & dat2$TENURE_MTHS < 12 & dat2$CON_END_MTHS > 0] = pmin(1L,
                                                                           (dat2$CON_END_MTHS[indx & dat2$TENURE_MTHS < 12 & dat2$CON_END_MTHS > 0] - 
                                                                              dat2$con_y1h1 [indx & dat2$TENURE_MTHS < 12 & dat2$CON_END_MTHS > 0] - 
                                                                              dat2$con_y1h2 [indx & dat2$TENURE_MTHS < 12 & dat2$CON_END_MTHS > 0]),na.rm=T)

dat2$con_y2h1 [indx & dat2$TENURE_MTHS < 18 & dat2$CON_END_MTHS > 0] = pmin ((18L - 
                                                                                dat2$TENURE_MTHS[indx & dat2$TENURE_MTHS < 18 & dat2$CON_END_MTHS > 0] - 
                                                                                dat2$con_y1h1[indx & dat2$TENURE_MTHS < 18 & dat2$CON_END_MTHS > 0] -
                                                                                dat2$con_y1h2[indx & dat2$TENURE_MTHS < 18 & dat2$CON_END_MTHS > 0] - 
                                                                                dat2$con_m12[indx & dat2$TENURE_MTHS < 18 & dat2$CON_END_MTHS > 0]),
                                                                             (dat2$CON_END_MTHS[indx & dat2$TENURE_MTHS < 18 & dat2$CON_END_MTHS > 0] - 
                                                                                dat2$con_y1h1 [indx & dat2$TENURE_MTHS < 18 & dat2$CON_END_MTHS > 0] - 
                                                                                dat2$con_y1h2 [indx & dat2$TENURE_MTHS < 18 & dat2$CON_END_MTHS > 0] -
                                                                                dat2$con_m12 [indx & dat2$TENURE_MTHS < 18 & dat2$CON_END_MTHS > 0] ) ,na.rm=T)

dat2$con_y2h2 [indx & dat2$TENURE_MTHS < 23 & dat2$CON_END_MTHS > 0] = pmin ((23L - 
                                                                                dat2$TENURE_MTHS [indx & dat2$TENURE_MTHS < 23 & dat2$CON_END_MTHS > 0] - 
                                                                                dat2$con_y1h1 [indx & dat2$TENURE_MTHS < 23 & dat2$CON_END_MTHS > 0] - 
                                                                                dat2$con_y1h2 [indx & dat2$TENURE_MTHS < 23 & dat2$CON_END_MTHS > 0] - 
                                                                                dat2$con_m12 [indx & dat2$TENURE_MTHS < 23 & dat2$CON_END_MTHS > 0] - 
                                                                                dat2$con_y2h1 [indx & dat2$TENURE_MTHS < 23 & dat2$CON_END_MTHS > 0]),
                                                                             (dat2$CON_END_MTHS[indx & dat2$TENURE_MTHS < 23 & dat2$CON_END_MTHS > 0] - 
                                                                                dat2$con_y1h1 [indx & dat2$TENURE_MTHS < 23 & dat2$CON_END_MTHS > 0] - 
                                                                                dat2$con_y1h2 [indx & dat2$TENURE_MTHS < 23 & dat2$CON_END_MTHS > 0] - 
                                                                                dat2$con_m12 [indx & dat2$TENURE_MTHS < 23 & dat2$CON_END_MTHS > 0]  - 
                                                                                dat2$con_y2h1 [indx & dat2$TENURE_MTHS < 23 & dat2$CON_END_MTHS > 0]) ,na.rm=T)

dat2$con_m24 [indx & dat2$TENURE_MTHS < 24 & dat2$CON_END_MTHS > 0] = pmin(1L,
                                                                           (dat2$CON_END_MTHS[indx & dat2$TENURE_MTHS < 24 & dat2$CON_END_MTHS > 0] - 
                                                                              dat2$con_y1h1 [indx & dat2$TENURE_MTHS < 24 & dat2$CON_END_MTHS > 0] - 
                                                                              dat2$con_y1h2 [indx & dat2$TENURE_MTHS < 24 & dat2$CON_END_MTHS > 0] -
                                                                              dat2$con_m12 [indx & dat2$TENURE_MTHS < 24 & dat2$CON_END_MTHS > 0]  - 
                                                                              dat2$con_y2h1 [indx & dat2$TENURE_MTHS < 24 & dat2$CON_END_MTHS > 0] -
                                                                              dat2$con_y2h2 [indx & dat2$TENURE_MTHS < 24 & dat2$CON_END_MTHS > 0] ) ,na.rm=T)

dat2$con_y3p [indx & dat2$CON_END_MTHS > 0]  = dat2$CON_END_MTHS [dat2$CON_END_MTHS > 0] - 
  dat2$con_y1h1 [indx & dat2$CON_END_MTHS > 0]- 
  dat2$con_y1h2 [indx & dat2$CON_END_MTHS > 0]- 
  dat2$con_m12 [indx & dat2$CON_END_MTHS > 0]- 
  dat2$con_y2h1 [indx & dat2$CON_END_MTHS > 0]- 
  dat2$con_y2h2 [indx & dat2$CON_END_MTHS > 0]- 
  dat2$con_m24 [indx & dat2$CON_END_MTHS > 0]

dat2$churn_cont [indx & dat2$CON_END_MTHS > 0] = (dat2$con_y1h1 [indx & dat2$CON_END_MTHS > 0] * dat2$ChurnY1H1 [indx & dat2$CON_END_MTHS > 0] +
                                                    dat2$con_y1h2 [indx & dat2$CON_END_MTHS > 0] * dat2$ChurnY1H2 [indx & dat2$CON_END_MTHS > 0] +
                                                    dat2$con_m12 [indx & dat2$CON_END_MTHS > 0] * dat2$Churn12M [indx & dat2$CON_END_MTHS > 0] +
                                                    dat2$con_y2h1 [indx & dat2$CON_END_MTHS > 0] * dat2$ChurnY2H1 [indx & dat2$CON_END_MTHS > 0] +
                                                    dat2$con_y2h2 [indx & dat2$CON_END_MTHS > 0] * dat2$ChurnY2H2 [indx & dat2$CON_END_MTHS > 0] +
                                                    dat2$con_m24 [indx & dat2$CON_END_MTHS > 0] * dat2$Churn24M [indx & dat2$CON_END_MTHS > 0] +
                                                    dat2$con_y3p [indx & dat2$CON_END_MTHS > 0] * dat2$ChurnY3P [indx & dat2$CON_END_MTHS > 0]) / 
  dat2$CON_END_MTHS [indx & dat2$CON_END_MTHS > 0]

# churn rate for duration out of contract (where contract still active)

# 5 year forecast from current customer's tenure
dat2$out_con_y1h1 = rep(0L,num) 
dat2$out_con_y1h2 = rep(0L,num) 
dat2$out_con_m12 = rep(0L,num) 
dat2$out_con_y2h1 = rep(0L,num) 
dat2$out_con_y2h2 = rep(0L,num) 
dat2$out_con_m24 = rep(0L,num) 
dat2$out_con_y3p = rep(0L,num) 

table (dat2$TENURE_MTHS, dat2$CON_END_MTHS, useNA = 'ifany' )
table (dat2$TENURE_MTHS, useNA = 'ifany' )

dat2$TENURE_MTHS2 = dat2$TENURE_MTHS + dat2$CON_END_MTHS


indx = !is.na(dat2$TENURE_MTHS2)

dat2$out_con_y1h1[dat2$TENURE_MTHS2 < 6 & indx] =  6L - 
  dat2$TENURE_MTHS2[dat2$TENURE_MTHS2 < 6 & indx] -
  dat2$TENURE_MTHS2[dat2$TENURE_MTHS2 < 6 & indx]
dat2$out_con_y1h2 [dat2$TENURE_MTHS2 < 11 & indx] =  11L - 
  dat2$TENURE_MTHS2 [dat2$TENURE_MTHS2 < 11 & indx] - 
  dat2$out_con_y1h1 [dat2$TENURE_MTHS2 < 11 & indx]
dat2$out_con_m12 [dat2$TENURE_MTHS2 < 12 & indx] = 1L
dat2$out_con_y2h1 [dat2$TENURE_MTHS2 < 18 & indx] = 18L - 
  dat2$TENURE_MTHS2[dat2$TENURE_MTHS2 < 18 & indx] - 
  dat2$out_con_y1h1[dat2$TENURE_MTHS2 < 18 & indx] - 
  dat2$out_con_y1h2[dat2$TENURE_MTHS2 < 18 & indx] - 
  dat2$out_con_m12[dat2$TENURE_MTHS2 < 18 & indx]
dat2$out_con_y2h2 [dat2$TENURE_MTHS2 < 23 & indx] =  23L - 
  dat2$TENURE_MTHS2 [dat2$TENURE_MTHS2 < 23 & indx] - 
  dat2$out_con_y1h1 [dat2$TENURE_MTHS2 < 23 & indx] - 
  dat2$out_con_y1h2 [dat2$TENURE_MTHS2 < 23 & indx] - 
  dat2$out_con_m12 [dat2$TENURE_MTHS2 < 23 & indx] - 
  dat2$out_con_y2h1 [dat2$TENURE_MTHS2 < 23 & indx]
dat2$out_con_m24 [dat2$TENURE_MTHS2 < 24 & indx] = 1L
dat2$out_con_y3p [indx]  = 60L -
  dat2$CON_END_MTHS [indx] - 
  dat2$out_con_y1h1 [indx] - 
  dat2$out_con_y1h2 [indx] - 
  dat2$out_con_m12 [indx] - 
  dat2$out_con_y2h1 [indx] - 
  dat2$out_con_y2h2 [indx] - 
  dat2$out_con_m24 [indx] 

dat2$churn_out_con[indx]= (dat2$out_con_y1h1 [indx] * dat2$ChurnY1H1 [indx] +
                             dat2$out_con_y1h2 [indx] * dat2$ChurnY1H2 [indx] +
                             dat2$out_con_m12 [indx] * dat2$Churn12M [indx] +
                             dat2$out_con_y2h1 [indx] * dat2$ChurnY2H1 [indx] +
                             dat2$out_con_y2h2 [indx] * dat2$ChurnY2H2 [indx] +
                             dat2$out_con_m24 [indx] * dat2$Churn24M [indx] +
                             dat2$out_con_y3p [indx] * dat2$ChurnY3P [indx]) / (60 - dat2$CON_END_MTHS [indx])

# code to deal with Promo_codes
table(!is.na(dat2$PROMO_CODE), !is.na(dat2$PROMO_RATE) & dat2$PROMO_RATE>0, useNA = 'ifany')
#       FALSE  TRUE
# FALSE 42096     0
# TRUE      0  6302
# good all promo_codes have a rate

table(dat2$PROMO_RATE[!is.na(dat2$PROMO_CODE)], useNA = 'ifany')
# lots of different rates!

table(dat2$STD_RATE[!is.na(dat2$PROMO_CODE)], useNA = 'ifany')
# 0.1005 0.1214 0.1823  0.198 0.2184   <NA> 
#   1494    403   2829   1054    403    109

table(dat2$RATE_NOW[!is.na(dat2$PROMO_CODE)], useNA = 'ifany')
#    0 0.01 0.04 0.05 
# 1244  195 3043 1810
dat2$RATE_NOW[!is.na(dat2$PROMO_CODE)] <- 0

table(dat2$STD_CHG[!is.na(dat2$PROMO_CODE)], useNA = 'ifany')
#      0 0.115  <NA> 
#   1897  4286   109

table(dat2$RATE_OFF_CON[!is.na(dat2$PROMO_CODE)], useNA = 'ifany')
#    0 0.01 0.04 0.05 
# 1244  195 3043 1810

table(!is.na(dat2$PROMO_CODE), dat2$EBILL, dat2$DD_PAY, dat2$REGISTER_DESC %in% c('24hr', 'Day', 'Night', 'Heating'), useNA = 'ifany')


table(dat2$COMM[!is.na(dat2$PROMO_CODE)], useNA = 'ifany')
# replace null values
dat2$COMM[!is.na(dat2$PROMO_CODE)] <- ifelse (is.na(dat2$COMM[!is.na(dat2$PROMO_CODE)]), 0, dat2$COMM[!is.na(dat2$PROMO_CODE)])

# get promo_rate_now and promo_std_charge
dat2$PROMO_RATE_NOW <- 0
dat2$PROMO_RATE_NOW[!is.na(dat2$PROMO_CODE)] <- dat2$PROMO_RATE[!is.na(dat2$PROMO_CODE)]

dat2$PROMO_STD_CHG <- 0
dat2$PROMO_STD_CHG[!is.na(dat2$PROMO_CODE)] <- dat2$SC_COMM[!is.na(dat2$PROMO_CODE)]/365

# ensure churn rates are not zero to avoid divide by zero error, replace with a very small value
dat2$ChurnY1H1 <- ifelse(dat2$ChurnY1H1 == 0, 0.00001, dat2$ChurnY1H1)
dat2$ChurnY1H2 <- ifelse(dat2$ChurnY1H2 == 0, 0.00001, dat2$ChurnY1H2)
dat2$Churn12M <- ifelse(dat2$Churn12M == 0, 0.00001, dat2$Churn12M)
dat2$ChurnY2H1 <- ifelse(dat2$ChurnY2H1 == 0, 0.00001, dat2$ChurnY2H1)
dat2$ChurnY2H2 <- ifelse(dat2$ChurnY2H2 == 0, 0.00001, dat2$ChurnY2H2)
dat2$Churn24M <- ifelse(dat2$Churn24M == 0, 0.00001, dat2$Churn24M)
dat2$ChurnY3P <- ifelse(dat2$ChurnY3P == 0, 0.00001, dat2$ChurnY3P)
dat2$multip_fact5y <- ifelse(dat2$multip_fact5y == 0, 1, dat2$multip_fact5y)


# revenue forecast for customers out of contract

indx = dat2$contract_expired == 'Y'

# REV_M0 is the month zero revenue estimate

dat2$REV_M0[indx] = (((dat2$AVG_YEARLY_CONSUMPTION[indx]/12) *
                        dat2$STD_RATE[indx] *
                        (1-dat2$RATE_NOW[indx]) )  +
                       (dat2$STD_CHG[indx] * 365/12))


# dat2$REV_EST = (A *
#                    ((((1-(1-(dat2$avg_churn_5y*dat2$multip_fact5y))^61))/(dat2$avg_churn_5y*dat2$multip_fact5y))-1)) 


# the following calculates the 5 yr rev estimate for each of the seven discrete time intervals
# M1-6, M7-11, M12, M13-18, M19-23, M24, M25+
# we use a geometric sequence to sum the monthly revenue for each segment
# sum over k from k=0 to k=n-1 of a(r^k) is a(1-r^n)/(1-r)
# where r is the retention rate = (1-c), where c is the churn rate
# a is the pre-churn adjusted monthly revenue i.e. monthly rev at month 0
# the equation is adjusted as follows
# sum over k from k = 1 to k = n of a((1-c)^k) is a(((1-(1-c)^n+1)/c)-1)
# at the start of the month 7-12 segment the rev at month 0 needs to be scaled down to represent the rev at month 6
# the same logic applies to the other subsequent segments



dat2$REV_EST[indx] =
  (dat2$REV_M0[indx] *
     ((((1-(1-(dat2$ChurnY1H1[indx]*dat2$multip_fact5y[indx]))^(dat2$out_con_y1h1[indx]+1)))/(dat2$ChurnY1H1[indx]*dat2$multip_fact5y[indx]))-1)) +
  
  (dat2$REV_M0[indx] *
     (1-(dat2$ChurnY1H1[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_y1h1[indx] *
     ((((1-(1-(dat2$ChurnY1H2[indx]*dat2$multip_fact5y[indx]))^(dat2$out_con_y1h2[indx]+1)))/(dat2$ChurnY1H2[indx]*dat2$multip_fact5y[indx]))-1)) +
  
  (dat2$REV_M0[indx] *
     (1-(dat2$ChurnY1H1[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_y1h1[indx] *
     (1-(dat2$ChurnY1H2[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_y1h2[indx] *
     ((((1-(1-(dat2$Churn12M[indx]*dat2$multip_fact5y[indx]))^(dat2$out_con_m12[indx]+1)))/(dat2$Churn12M[indx]*dat2$multip_fact5y[indx]))-1)) +
  
  (dat2$REV_M0[indx] *
     (1-(dat2$ChurnY1H1[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_y1h1[indx] *
     (1-(dat2$ChurnY1H2[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_y1h2[indx] *
     (1-(dat2$Churn12M[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_m12[indx] *
     ((((1-(1-(dat2$ChurnY2H1[indx]*dat2$multip_fact5y[indx]))^(dat2$out_con_y2h1[indx]+1)))/(dat2$ChurnY2H1[indx]*dat2$multip_fact5y[indx]))-1)) +
  
  (dat2$REV_M0[indx] *
     (1-(dat2$ChurnY1H1[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_y1h1[indx] *
     (1-(dat2$ChurnY1H2[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_y1h2[indx] *
     (1-(dat2$Churn12M[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_m12[indx] *
     (1-(dat2$ChurnY2H1[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_y2h1[indx] *
     ((((1-(1-(dat2$ChurnY2H2[indx]*dat2$multip_fact5y[indx]))^(dat2$out_con_y2h2[indx]+1)))/(dat2$ChurnY2H2[indx]*dat2$multip_fact5y[indx]))-1)) +
  
  (dat2$REV_M0[indx] *
     (1-(dat2$ChurnY1H1[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_y1h1[indx] *
     (1-(dat2$ChurnY1H2[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_y1h2[indx] *
     (1-(dat2$Churn12M[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_m12[indx] *
     (1-(dat2$ChurnY2H1[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_y2h1[indx] *
     (1-(dat2$ChurnY2H2[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_y2h2[indx] *
     ((((1-(1-(dat2$Churn24M[indx]*dat2$multip_fact5y[indx]))^(dat2$out_con_m24[indx]+1)))/(dat2$Churn24M[indx]*dat2$multip_fact5y[indx]))-1)) +
  
  (dat2$REV_M0[indx] *
     (1-(dat2$ChurnY1H1[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_y1h1[indx] *
     (1-(dat2$ChurnY1H2[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_y1h2[indx] *
     (1-(dat2$Churn12M[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_m12[indx] *
     (1-(dat2$ChurnY2H1[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_y2h1[indx] *
     (1-(dat2$ChurnY2H2[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_y2h2[indx] *
     (1-(dat2$Churn24M[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_m24[indx] *
     ((((1-(1-(dat2$ChurnY3P[indx]*dat2$multip_fact5y[indx]))^(dat2$out_con_y3p[indx]+1)))/(dat2$ChurnY3P[indx]*dat2$multip_fact5y[indx]))-1))



# dat2$REV_EST3 =
#   (A *
#      ((((1-(1-(dat2$avg_churn_5y*dat2$multip_fact5y))^(dat2$out_con_y1h1+1)))/(dat2$avg_churn_5y*dat2$multip_fact5y))-1)) +
#   
#   (A *
#      (1-(dat2$avg_churn_5y*dat2$multip_fact5y))^dat2$out_con_y1h1 *
#      ((((1-(1-(dat2$avg_churn_5y*dat2$multip_fact5y))^(dat2$out_con_y1h2+1)))/(dat2$avg_churn_5y*dat2$multip_fact5y))-1)) +
#   
#   (A *
#      (1-(dat2$avg_churn_5y*dat2$multip_fact5y))^dat2$out_con_y1h1 *
#      (1-(dat2$avg_churn_5y*dat2$multip_fact5y))^dat2$out_con_y1h2 *
#      ((((1-(1-(dat2$avg_churn_5y*dat2$multip_fact5y))^(dat2$out_con_m12+1)))/(dat2$avg_churn_5y*dat2$multip_fact5y))-1)) +
#   
#   (A *
#      (1-(dat2$avg_churn_5y*dat2$multip_fact5y))^dat2$out_con_y1h1 *
#      (1-(dat2$avg_churn_5y*dat2$multip_fact5y))^dat2$out_con_y1h2 *
#      (1-(dat2$avg_churn_5y*dat2$multip_fact5y))^dat2$out_con_m12 *
#      ((((1-(1-(dat2$avg_churn_5y*dat2$multip_fact5y))^(dat2$out_con_y2h1+1)))/(dat2$avg_churn_5y*dat2$multip_fact5y))-1)) +
#   
#   (A *
#      (1-(dat2$avg_churn_5y*dat2$multip_fact5y))^dat2$out_con_y1h1 *
#      (1-(dat2$avg_churn_5y*dat2$multip_fact5y))^dat2$out_con_y1h2 *
#      (1-(dat2$avg_churn_5y*dat2$multip_fact5y))^dat2$out_con_m12 *
#      (1-(dat2$avg_churn_5y*dat2$multip_fact5y))^dat2$out_con_y2h1 *
#      ((((1-(1-(dat2$avg_churn_5y*dat2$multip_fact5y))^(dat2$out_con_y2h2+1)))/(dat2$avg_churn_5y*dat2$multip_fact5y))-1)) +
#   
#   (A *
#      (1-(dat2$avg_churn_5y*dat2$multip_fact5y))^dat2$out_con_y1h1 *
#      (1-(dat2$avg_churn_5y*dat2$multip_fact5y))^dat2$out_con_y1h2 *
#      (1-(dat2$avg_churn_5y*dat2$multip_fact5y))^dat2$out_con_m12 *
#      (1-(dat2$avg_churn_5y*dat2$multip_fact5y))^dat2$out_con_y2h1 *
#      (1-(dat2$avg_churn_5y*dat2$multip_fact5y))^dat2$out_con_y2h2 *
#      ((((1-(1-(dat2$avg_churn_5y*dat2$multip_fact5y))^(dat2$out_con_m24+1)))/(dat2$avg_churn_5y*dat2$multip_fact5y))-1)) +
#   
#   (A *
#      (1-(dat2$avg_churn_5y*dat2$multip_fact5y))^dat2$out_con_y1h1 *
#      (1-(dat2$avg_churn_5y*dat2$multip_fact5y))^dat2$out_con_y1h2 *
#      (1-(dat2$avg_churn_5y*dat2$multip_fact5y))^dat2$out_con_m12 *
#      (1-(dat2$avg_churn_5y*dat2$multip_fact5y))^dat2$out_con_y2h1 *
#      (1-(dat2$avg_churn_5y*dat2$multip_fact5y))^dat2$out_con_y2h2 *
#      (1-(dat2$avg_churn_5y*dat2$multip_fact5y))^dat2$out_con_m24 *
#      ((((1-(1-(dat2$avg_churn_5y*dat2$multip_fact5y))^(dat2$out_con_y3p+1)))/(dat2$avg_churn_5y*dat2$multip_fact5y))-1))
# 
# summary(dat2$REV_EST3[ dat2$CON_END_MTHS == 0]/dat2$REV_EST [ dat2$CON_END_MTHS == 0], na.rm = T)
#    Min. 1st Qu.  Median    Mean 3rd Qu.    Max.    NA's 
#       1       1       1       1       1       1   23293 

# mean(dat2$REV_EST2[ dat2$CON_END_MTHS == 0]/dat2$REV_EST [ dat2$CON_END_MTHS == 0], na.rm = T)
# [1] 0.9951065
# so this shows that we need to sum the seven separate geometric functions

# adjust revenue forecast for customer still in contract
# this uses the same logic but is just slightly more complex as
# we need to account for time periods in and out of contract

table(!is.na(dat2$REV_EST), useNA = 'ifany')

# FALSE  TRUE 
# 11621 36720  

table(!is.na(dat2$REV_EST), !is.na(dat2$PROMO_CODE))

#       FALSE  TRUE
# FALSE  8951  2670
# TRUE  33088  3632

table (!is.na(dat2$PROMO_CODE), dat2$contract_expired, useNA = 'ifany')

# still in contact and no promo code (or it doesn't map to Chris's codes)

indx1 = dat2$contract_expired == 'N' & is.na(dat2$PROMO_CODE)
sum(indx1)

dat2$REV_M0[indx1] = ((dat2$AVG_YEARLY_CONSUMPTION[indx1]/12)*
                        dat2$STD_RATE[indx1]*
                        (1-dat2$RATE_NOW[indx1]) +
                        (dat2$STD_CHG[indx1] * 365/12))

# REV_MX is the ratio of the out of contract revenue estimate to the in contract revenue estimate
dat2$REV_MX[indx1] = ((dat2$AVG_YEARLY_CONSUMPTION[indx1]/12)*
                        dat2$STD_RATE[indx1]*
                        (1-dat2$RATE_OFF_CON[indx1]) +
                        (dat2$STD_CHG[indx1] * 365/12))

dat2$REV_MX[indx1] = round(dat2$REV_MX[indx1]/dat2$REV_M0[indx1],2)
dat2$REV_MX[!is.finite(dat2$REV_MX) | dat2$REV_MX <= 0] <- 1

indx2 = dat2$contract_expired == 'N' & !is.na(dat2$PROMO_CODE)
sum(indx2)

dat2$REV_M0[indx2] = (((dat2$AVG_YEARLY_CONSUMPTION[indx2]/12) *
                         (dat2$PROMO_RATE_NOW[indx2])) +
                        (dat2$PROMO_STD_CHG[indx2] * 365/12))

dat2$REV_MX[indx2] = ((dat2$AVG_YEARLY_CONSUMPTION[indx2]/12) *
                        dat2$STD_RATE[indx2]*
                        (1-dat2$RATE_OFF_CON[indx2]) +
                        (dat2$STD_CHG[indx2] * 365/12))

dat2$REV_MX[indx2] = round(dat2$REV_MX[indx2]/dat2$REV_M0[indx2],2)
dat2$REV_MX[!is.finite(dat2$REV_MX) | dat2$REV_MX <= 0] <- 1

# calculate revenue for those in contract - promo_code and non-promo code at same time
indx1 = indx1 | indx2

table(is.na(dat2$REV_EST[indx1]), useNA = 'ifany')



dat2$REV_EST [indx1] = 
  (dat2$REV_M0[indx1] *
     ((((1-(1-(dat2$ChurnY1H1[indx1] *dat2$multip_fact5y[indx1] ))^(dat2$con_y1h1[indx1]+1) ))/(dat2$ChurnY1H1[indx1] *dat2$multip_fact5y[indx1]))-1)) +
  
  (dat2$REV_M0[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h1[indx1] *
     ((((1-(1-(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))^(dat2$con_y1h2[indx1]+1)))/(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))-1)) +
  
  (dat2$REV_M0[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h1[indx1] *
     (1-(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h2[indx1] *
     ((((1-(1-(dat2$Churn12M[indx1]*dat2$multip_fact5y[indx1]))^(dat2$con_m12[indx1]+1)))/(dat2$Churn12M[indx1]*dat2$multip_fact5y[indx1]))-1)) +
  
  (dat2$REV_M0[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h1[indx1] *
     (1-(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h2[indx1] *
     (1-(dat2$Churn12M[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_m12[indx1] *
     ((((1-(1-(dat2$ChurnY2H1[indx1]*dat2$multip_fact5y[indx1]))^(dat2$con_y2h1[indx1]+1)))/(dat2$ChurnY2H1[indx1]*dat2$multip_fact5y[indx1]))-1)) +
  
  (dat2$REV_M0[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h1[indx1] *
     (1-(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h2[indx1] *
     (1-(dat2$Churn12M[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_m12[indx1] *
     (1-(dat2$ChurnY2H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y2h1[indx1] *
     ((((1-(1-(dat2$ChurnY2H2[indx1]*dat2$multip_fact5y[indx1]))^(dat2$con_y2h2[indx1]+1)))/(dat2$ChurnY2H2[indx1]*dat2$multip_fact5y[indx1]))-1)) +
  
  (dat2$REV_M0[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h1[indx1] *
     (1-(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h2[indx1] *
     (1-(dat2$Churn12M[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_m12[indx1] *
     (1-(dat2$ChurnY2H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y2h1[indx1] *
     (1-(dat2$ChurnY2H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y2h2[indx1] *
     ((((1-(1-(dat2$Churn24M[indx1]*dat2$multip_fact5y[indx1]))^(dat2$con_m24[indx1]+1)))/(dat2$Churn24M[indx1]*dat2$multip_fact5y[indx1]))-1)) +
  
  (dat2$REV_M0[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h1[indx1] *
     (1-(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h2[indx1] *
     (1-(dat2$Churn12M[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_m12[indx1] *
     (1-(dat2$ChurnY2H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y2h1[indx1] *
     (1-(dat2$ChurnY2H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y2h2[indx1] *
     (1-(dat2$Churn24M[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_m24[indx1] *
     ((((1-(1-(dat2$ChurnY3P[indx1]*dat2$multip_fact5y[indx1]))^(dat2$con_y3p[indx1]+1)))/(dat2$ChurnY3P[indx1]*dat2$multip_fact5y[indx1]))-1)) + 
  
  (dat2$REV_M0[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h1[indx1] * 
     dat2$REV_MX[indx1] *
     ((((1-(1-(dat2$ChurnY1H1[indx1] *dat2$multip_fact5y[indx1] ))^(dat2$out_con_y1h1[indx1]+1)))/(dat2$ChurnY1H1[indx1] *dat2$multip_fact5y[indx1]))-1)) +
  
  (dat2$REV_M0[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h1[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_y1h1[indx1] *
     (1-(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h2[indx1] *
     dat2$REV_MX[indx1] *
     ((((1-(1-(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))^(dat2$out_con_y1h2[indx1]+1)))/(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))-1)) +
  
  (dat2$REV_M0[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h1[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_y1h1[indx1] *
     (1-(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h2[indx1] *
     (1-(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_y1h2[indx1] *
     (1-(dat2$Churn12M[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_m12[indx1] *
     dat2$REV_MX[indx1] *
     ((((1-(1-(dat2$Churn12M[indx1]*dat2$multip_fact5y[indx1]))^(dat2$out_con_m12[indx1]+1)))/(dat2$Churn12M[indx1]*dat2$multip_fact5y[indx1]))-1)) +
  
  (dat2$REV_M0[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h1[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_y1h1[indx1] *
     (1-(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h2[indx1] *
     (1-(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_y1h2[indx1] *
     (1-(dat2$Churn12M[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_m12[indx1] *
     (1-(dat2$Churn12M[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_m12[indx1] *
     (1-(dat2$ChurnY2H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y2h1[indx1] *
     dat2$REV_MX[indx1] *
     ((((1-(1-(dat2$ChurnY2H1[indx1]*dat2$multip_fact5y[indx1]))^(dat2$out_con_y2h1[indx1]+1)))/(dat2$ChurnY2H1[indx1]*dat2$multip_fact5y[indx1]))-1)) +
  
  (dat2$REV_M0[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h1[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_y1h1[indx1] *
     (1-(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h2[indx1] *
     (1-(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_y1h2[indx1] *
     (1-(dat2$Churn12M[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_m12[indx1] *
     (1-(dat2$Churn12M[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_m12[indx1] *
     (1-(dat2$ChurnY2H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y2h1[indx1] *
     (1-(dat2$ChurnY2H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_y2h1[indx1] *
     (1-(dat2$ChurnY2H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y2h2[indx1] *
     dat2$REV_MX[indx1] *
     ((((1-(1-(dat2$ChurnY2H2[indx1]*dat2$multip_fact5y[indx1]))^(dat2$out_con_y2h2[indx1]+1)))/(dat2$ChurnY2H2[indx1]*dat2$multip_fact5y[indx1]))-1)) +
  
  (dat2$REV_M0[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h1[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_y1h1[indx1] *
     (1-(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h2[indx1] *
     (1-(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_y1h2[indx1] *
     (1-(dat2$Churn12M[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_m12[indx1] *
     (1-(dat2$Churn12M[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_m12[indx1] *
     (1-(dat2$ChurnY2H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y2h1[indx1] *
     (1-(dat2$ChurnY2H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_y2h1[indx1] *
     (1-(dat2$ChurnY2H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y2h2[indx1] *
     (1-(dat2$ChurnY2H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_y2h2[indx1] *
     (1-(dat2$Churn24M[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_m24[indx1] *
     dat2$REV_MX[indx1] *
     ((((1-(1-(dat2$Churn24M[indx1]*dat2$multip_fact5y[indx1]))^(dat2$out_con_m24[indx1]+1)))/(dat2$Churn24M[indx1]*dat2$multip_fact5y[indx1]))-1)) +
  
  (dat2$REV_M0[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h1[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_y1h1[indx1] *
     (1-(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h2[indx1] *
     (1-(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_y1h2[indx1] *
     (1-(dat2$Churn12M[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_m12[indx1] *
     (1-(dat2$Churn12M[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_m12[indx1] *
     (1-(dat2$ChurnY2H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y2h1[indx1] *
     (1-(dat2$ChurnY2H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_y2h1[indx1] *
     (1-(dat2$ChurnY2H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y2h2[indx1] *
     (1-(dat2$ChurnY2H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_y2h2[indx1] *
     (1-(dat2$Churn24M[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_m24[indx1] *
     (1-(dat2$Churn24M[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_m24[indx1] *
     (1-(dat2$ChurnY3P[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y3p[indx1] *
     dat2$REV_MX[indx1] *
     ((((1-(1-(dat2$ChurnY3P[indx1]*dat2$multip_fact5y[indx1]))^(dat2$out_con_y3p[indx1]+1)))/(dat2$ChurnY3P[indx1]*dat2$multip_fact5y[indx1]))-1))


table(!is.na(dat2$REV_EST), useNA = 'ifany')

# FALSE  TRUE 
#   887 47393

table(!is.na(dat2$REV_EST), !is.na(dat2$PROMO_CODE), useNA = 'ifany')

#       FALSE  TRUE
# FALSE   769   118
# TRUE  41211  6182

# tab = as.data.frame(table(is.finite(dat2$REV_EST), dat2$promo_expired, dat2$contract_expired, is.finite(dat2$multip_fact5y), is.finite(dat2$REV_M0), 
#                           is.finite(dat2$REV_MX), is.finite(dat2$ChurnY1H1), is.finite(dat2$ChurnY1H2), is.finite(dat2$Churn12M), is.finite(dat2$ChurnY2H1), 
#                           is.finite(dat2$ChurnY2H2), is.finite(dat2$Churn24M), is.finite(dat2$ChurnY3P), is.finite(dat2$con_y1h1), is.finite(dat2$con_y1h2), 
#                           is.finite(dat2$con_m12), is.finite(dat2$con_y2h1), is.finite(dat2$con_y2h2), is.finite(dat2$con_m24), is.finite(dat2$con_y3p), 
#                           is.finite(dat2$out_con_y1h1), is.finite(dat2$out_con_y1h2), is.finite(dat2$out_con_m12), is.finite(dat2$out_con_y2h1), is.finite(dat2$out_con_y2h2),
#                           is.finite(dat2$out_con_m24), is.finite(dat2$out_con_y3p), useNA = 'ifany'))
# 
# tab2 <- tab[tab$Var1 == F & tab$Freq > 0,] # filtered for those with a count but not getting a revenue estimate
# tab2[order (tab2$Freq, decreasing = T),]
#       Var1 Var2 Var3  Var4  Var5 Var6  Var7  Var8  Var9 Var10 Var11 Var12 Var13 Var14 Var15 Var16 Var17 Var18 Var19 Var20 Var21 Var22 Var23 Var24 Var25 Var26 Var27 Freq
# 6137 FALSE <NA>    N  TRUE  TRUE TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE 1776
# 6143 FALSE <NA>    Y  TRUE  TRUE TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE 1517
# 6133 FALSE    N    N  TRUE  TRUE TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE 1322
# 6113 FALSE <NA>    N  TRUE FALSE TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  492
# 6119 FALSE <NA>    Y  TRUE FALSE TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  387
# 6135 FALSE    Y    N  TRUE  TRUE TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  254
# 6141 FALSE    Y    Y  TRUE  TRUE TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE   56
# 25   FALSE    N    N FALSE  TRUE TRUE FALSE FALSE FALSE FALSE FALSE FALSE FALSE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE  TRUE    8

# of the 5812 that have no revenue estimate 879 have no M0 value and 8 have no churn values data all the rest have churn data and counts of months in and out of contract

# mean(dat2$multip_fact5y[!is.finite(dat2$REV_EST) & is.finite(dat2$ChurnY1H1)])
# # [1] 0.5902534
# mean(dat2$REV_M0[!is.finite(dat2$REV_EST) & is.finite(dat2$ChurnY1H1)])
# # [1] NA 
# mean(dat2$REV_MX[!is.finite(dat2$REV_EST) & is.finite(dat2$ChurnY1H1)])
# # [1] 1.086904
# mean(dat2$ChurnY1H1[!is.finite(dat2$REV_EST) & is.finite(dat2$ChurnY1H1)])
# # [1] 0.002759769
# mean(dat2$ChurnY1H2[!is.finite(dat2$REV_EST) & is.finite(dat2$ChurnY1H1)])
# # [1] 0.01881447
# mean(dat2$Churn12M[!is.finite(dat2$REV_EST) & is.finite(dat2$ChurnY1H1)])
# # [1] 0.03368194
# mean(dat2$ChurnY2H1[!is.finite(dat2$REV_EST) & is.finite(dat2$ChurnY1H1)])
# # [1] 0.01687416
# mean(dat2$ChurnY2H2[!is.finite(dat2$REV_EST) & is.finite(dat2$ChurnY1H1)])
# # [1] 0.005165541
# mean(dat2$Churn24M[!is.finite(dat2$REV_EST) & is.finite(dat2$ChurnY1H1)])
# # [1] 0.001558615
# mean(dat2$ChurnY3P[!is.finite(dat2$REV_EST) & is.finite(dat2$ChurnY1H1)])
# # [1] 0.006057254
# mean(dat2$con_y1h1[!is.finite(dat2$REV_EST) & is.finite(dat2$ChurnY1H1)])
# # [1] 0.1328394
# mean(dat2$con_y1h2[!is.finite(dat2$REV_EST) & is.finite(dat2$ChurnY1H1)])
# # [1] 0.3535493
# mean(dat2$con_m12[!is.finite(dat2$REV_EST) & is.finite(dat2$ChurnY1H1)])
# # [1] 0.1073398
# mean(dat2$con_y2h1[!is.finite(dat2$REV_EST) & is.finite(dat2$ChurnY1H1)])
# # [1] 0.4350448
# mean(dat2$con_y2h2[!is.finite(dat2$REV_EST) & is.finite(dat2$ChurnY1H1)])
# # [1] 0.6278429
# mean(dat2$con_m24[!is.finite(dat2$REV_EST) & is.finite(dat2$ChurnY1H1)])
# # [1] 0.1443832
# mean(dat2$con_y3p[!is.finite(dat2$REV_EST) & is.finite(dat2$ChurnY1H1)])
# #[1] 4.290317
# mean(dat2$out_con_y1h1[!is.finite(dat2$REV_EST) & is.finite(dat2$ChurnY1H1)])
# # [1] -0.006891799 ############## problem
# mean(dat2$out_con_y1h2[!is.finite(dat2$REV_EST) & is.finite(dat2$ChurnY1H1)])
# # [1] 0.07494831
# mean(dat2$out_con_m12[!is.finite(dat2$REV_EST) & is.finite(dat2$ChurnY1H1)])
# # [1] 0.07322536
# mean(dat2$out_con_y2h1[!is.finite(dat2$REV_EST) & is.finite(dat2$ChurnY1H1)])
# # [1] 0.5422123
# mean(dat2$out_con_y2h2[!is.finite(dat2$REV_EST) & is.finite(dat2$ChurnY1H1)])
# # [1] 0.6261199
# mean(dat2$out_con_m24[!is.finite(dat2$REV_EST) & is.finite(dat2$ChurnY1H1)])
# # [1] 0.1976223
# mean(dat2$out_con_y3p[!is.finite(dat2$REV_EST) & is.finite(dat2$ChurnY1H1)])
# # [1] 52.40145
# 
# mean(dat2$REV_M0[!is.finite(dat2$REV_EST) & is.finite(dat2$ChurnY1H1)], na.rm = T)
# # [1] 3707.203
# 
# summary(dat2$REV_M0[!is.finite(dat2$REV_EST) & is.finite(dat2$ChurnY1H1)])
# #     Min.  1st Qu.   Median     Mean  3rd Qu.     Max.     NA's 
# #        0       40      109     3707      249 12324663      879
# 
# summary(dat2$out_con_y1h1[!is.finite(dat2$REV_EST) & is.finite(dat2$ChurnY1H1)])
# #      Min.   1st Qu.    Median      Mean   3rd Qu.      Max. 
# # -6.000000  0.000000  0.000000 -0.006892  0.000000  6.000000
# 
# summary(dat2$out_con_y1h1[!is.finite(dat2$REV_EST) & is.finite(dat2$ChurnY1H1)]<0)
# #    Mode   FALSE    TRUE 
# # logical    5786      18
# 
# summary (dat2$ChurnY1H1[!is.finite(dat2$REV_EST) & is.finite(dat2$ChurnY1H1)]==0)
# #    Mode   FALSE    TRUE 
# # logical    2610    3194

mean(dat2$REV_EST[indx1], na.rm = T)/mean(dat2$REV_EST[-indx1], na.rm = T)
# [1] 0.884512

mean(dat2$REV_EST[indx], na.rm = T)/mean(dat2$REV_EST[-indx], na.rm = T)
# [1] 0.884512

hist(log(dat2$REV_EST[indx]), breaks = 100, col = 3) 
hist(log(dat2$REV_EST[indx1]), breaks = 100, col = 3)

promo_sum <- dat2 %>% 
  group_by(promo_yr, promo_mth) %>% 
  summarise(GR_MARGINS = paste(unique(fct_explicit_na(as.character(GR_MARGIN))), collapse = ', '))

promo_sum

# # A tibble: 28 x 3
# # Groups:   promo_yr [5]
# promo_yr promo_mth GR_MARGINS                         
# <chr>    <fct>     <chr>                              
#   1 18       Mar       (Missing)                          
# 2 18       Apr       0.085, 0.065, (Missing), 0.07, 0.05
# 3 18       May       (Missing), 0.085, 0.065, 0.07      
# 4 18       Jun       0.085, (Missing), 0.065, 0.05      
# 5 18       Jul       0.085, (Missing), 0.065, 0.07      
# 6 18       Aug       0.085, 0.065, 0.05, 0.07, (Missing)
# 7 18       Sep       0.085, 0.065, 0.05, (Missing), 0.07
# 8 18       Oct       0.085, (Missing), 0.065, 0.07, 0.05
# 9 18       Nov       0.085, 0.065, 0.07, (Missing), 0.05
# 10 18       Dec       0.065, (Missing), 0.085, 0.07      
# # ... with 18 more rows

#########################################################




# indx1 = which(dat2$CON_END_MTHS > 0 & dat2$JURISDICTION == 'ROI' & !is.na(dat2$REV_EST))
# indx2 = which(dat2$CON_END_MTHS > 0 & dat2$JURISDICTION == 'NI' & !is.na(dat2$REV_EST))
# indx3 = which(dat2$CON_END_MTHS == 0 & dat2$JURISDICTION == 'ROI' & !is.na(dat2$REV_EST))
# indx4 = which(dat2$CON_END_MTHS == 0 & dat2$JURISDICTION == 'NI' & !is.na(dat2$REV_EST))
# 
# # add percentiles for the revenue so that we can filter out the extremes for summary stats purposes
# dat2$rev_percentile [indx1] = round(ecdf(dat2$REV_EST[indx1])(dat2$REV_EST[indx1])*100,2)
# dat2$rev_percentile [indx2] = round(ecdf(dat2$REV_EST[indx2])(dat2$REV_EST[indx2])*100,2)
# dat2$rev_percentile [indx3] = round(ecdf(dat2$REV_EST[indx3])(dat2$REV_EST[indx3])*100,2)
# dat2$rev_percentile [indx4] = round(ecdf(dat2$REV_EST[indx4])(dat2$REV_EST[indx4])*100,2)
# 
# indx1 = which(dat2$rev_percentile[indx1] <= 99 & dat2$rev_percentile[indx1] >= 1)
# indx2 = which(dat2$rev_percentile[indx2] <= 99 & dat2$rev_percentile[indx2] >= 1)
# indx3 = which(dat2$rev_percentile[indx3] <= 99 & dat2$rev_percentile[indx3] >= 1)
# indx4 = which(dat2$rev_percentile[indx4] <= 99 & dat2$rev_percentile[indx4] >= 1)
# 
# summary(dat2$REV_EST [indx1])
# # ROI in Contract
# #  Min. 1st Qu.  Median    Mean 3rd Qu.    Max.    NA's 
# #     0    2277    5658   11708   13454  275072    3591
# 
# summary(dat2$REV_EST [indx2])
# # NI in Contract
# #  Min. 1st Qu.  Median    Mean 3rd Qu.    Max.    NA's 
# #     0    3141    7666   14393   17376  241404     979
# 
# summary(dat2$REV_EST [indx3])
# # ROI Out of Contract
# #  Min. 1st Qu.  Median    Mean 3rd Qu.    Max.    NA's 
# #     0    2303    5751   11937   13774  275072    3184 
# 
# summary(dat2$REV_EST [indx4])
# # NI Out of Contract
# #  Min. 1st Qu.  Median    Mean 3rd Qu.    Max.    NA's 
# #     0    2870    7114   13736   16492  241404    1340



####################################################################

setwd("S:/Retail/IRL/Supply/Marketing/MARKETING SUPPLY/Analytics & Products/Analytics/MM WIP/BE Value Model")

# CTS <- read.csv('BE_CTS_SEP_2021.csv', header = T, skip = 1)
# Added the code to fetch CTS data directly from DM2.0 Analytics
conn <- dbConnect(drv, username = dbname = connect.string, prefetch = FALSE,
                  bulk_read = 1000L, stmt_cache = 0L, external_credentials = FALSE,
                  sysdba = FALSE)
query_res_cts <- dbGetQuery(conn, "select * from BE_CTS");
CTS <- as.data.frame(query_res_cts)
dbDisconnect(conn)


dat2 = merge(dat2, CTS, by = c('JURISDICTION', 
                               'UTILITY_TYPE_CODE',
                               'REGISTER_DESC',
                               'DG',
                               'CATGRY'),
             all.x = T, all.y = F)

sum(!is.na(dat2$ENERGY_COST_MWH)) # 47519
sum(is.na(dat2$ENERGY_COST_MWH))  #   822

table(dat2$REGISTER_DESC[is.na(dat2$ENERGY_COST_MWH)], dat2$DG[is.na(dat2$ENERGY_COST_MWH)], dat2$JURISDICTION [is.na(dat2$ENERGY_COST_MWH)], useNA='ifany')
# , ,  = NI
# 
# 
#          T011 T012 T014 T015 T021 T022 T024 T031 T034 T041 T042 T043 T052 T053 T062 T063
# 24hr      535    0    0    0   21    0    0    0    1   40   39   30    0   22    1    3
# Day         0   10   12    8    2   15    3    1    0    0    0    0   10    0    0    0
# Heating     0    0   10    8    0    0    3    0    0    0    0    0    0    0    0    0
# Night       0   10   11    8    2   15    3    1    0    0    0    0    2    0    0    0
# Off Peak    0    0    0    0    0    0    0    1    0   38   38   29    0    0    1    3

# we again use the equation for geometric sum to calculate the cost
# over the months taking into account the seven churn time segments
# usage as monthly mega watts

table(!is.na(dat2$GR_MARGIN), !is.na(dat2$COMM), useNA = 'ifany')
table(dat2$GR_MARGIN, dat2$COMM, useNA = 'ifany')

# out of contract 

indx = dat2$contract_expired == 'Y' 


dat2$COST_NRG_M0[indx] = (dat2$AVG_YEARLY_CONSUMPTION[indx]/12000) * 
  (dat2$ENERGY_COST_MWH[indx] + dat2$TD_COST_MWH[indx] + dat2$ECO_MWH[indx]) 

dat2$CTS_M0[indx] = (dat2$COST_NRG_M0[indx] * (dat2$BDP[indx])) +
  (dat2$TD_COST_YSC[indx]/12) +
  (((dat2$COST_OH_FXT[indx] + dat2$COST_OH_VAR[indx])/(dat2$NUM_REG[indx]*12))*
     dat2$EUR_STG_RATE[indx])


dat2$ENRG_CST[indx] = (dat2$COST_NRG_M0[indx]  *
                         ((((1-(1-(dat2$ChurnY1H1[indx]*dat2$multip_fact5y[indx]))^(dat2$out_con_y1h1[indx]+1)))/(dat2$ChurnY1H1[indx]*dat2$multip_fact5y[indx]))-1)) +
  
  (dat2$COST_NRG_M0[indx]  *
     (1-(dat2$ChurnY1H1[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_y1h1[indx] *
     ((((1-(1-(dat2$ChurnY1H2[indx]*dat2$multip_fact5y[indx]))^(dat2$out_con_y1h2[indx]+1)))/(dat2$ChurnY1H2[indx]*dat2$multip_fact5y[indx]))-1)) +
  
  (dat2$COST_NRG_M0[indx]  *
     (1-(dat2$ChurnY1H1[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_y1h1[indx] *
     (1-(dat2$ChurnY1H2[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_y1h2[indx] *
     ((((1-(1-(dat2$Churn12M[indx]*dat2$multip_fact5y[indx]))^(dat2$out_con_m12[indx]+1)))/(dat2$Churn12M[indx]*dat2$multip_fact5y[indx]))-1)) +
  
  (dat2$COST_NRG_M0[indx] *
     (1-(dat2$ChurnY1H1[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_y1h1[indx] *
     (1-(dat2$ChurnY1H2[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_y1h2[indx] *
     (1-(dat2$Churn12M[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_m12[indx] *
     ((((1-(1-(dat2$ChurnY2H1[indx]*dat2$multip_fact5y[indx]))^(dat2$out_con_y2h1[indx]+1)))/(dat2$ChurnY2H1[indx]*dat2$multip_fact5y[indx]))-1)) +
  
  (dat2$COST_NRG_M0[indx]  *
     (1-(dat2$ChurnY1H1[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_y1h1[indx] *
     (1-(dat2$ChurnY1H2[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_y1h2[indx] *
     (1-(dat2$Churn12M[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_m12[indx] *
     (1-(dat2$ChurnY2H1[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_y2h1[indx] *
     ((((1-(1-(dat2$ChurnY2H2[indx]*dat2$multip_fact5y[indx]))^(dat2$out_con_y2h2[indx]+1)))/(dat2$ChurnY2H2[indx]*dat2$multip_fact5y[indx]))-1)) +
  
  (dat2$COST_NRG_M0[indx]  *
     (1-(dat2$ChurnY1H1[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_y1h1[indx] *
     (1-(dat2$ChurnY1H2[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_y1h2[indx] *
     (1-(dat2$Churn12M[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_m12[indx] *
     (1-(dat2$ChurnY2H1[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_y2h1[indx] *
     (1-(dat2$ChurnY2H2[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_y2h2[indx] *
     ((((1-(1-(dat2$Churn24M[indx]*dat2$multip_fact5y[indx]))^(dat2$out_con_m24[indx]+1)))/(dat2$Churn24M[indx]*dat2$multip_fact5y[indx]))-1)) +
  
  (dat2$COST_NRG_M0[indx]  *
     (1-(dat2$ChurnY1H1[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_y1h1[indx] *
     (1-(dat2$ChurnY1H2[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_y1h2[indx] *
     (1-(dat2$Churn12M[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_m12[indx] *
     (1-(dat2$ChurnY2H1[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_y2h1[indx] *
     (1-(dat2$ChurnY2H2[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_y2h2[indx] *
     (1-(dat2$Churn24M[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_m24[indx] *
     ((((1-(1-(dat2$ChurnY3P[indx]*dat2$multip_fact5y[indx]))^(dat2$out_con_y3p[indx]+1)))/(dat2$ChurnY3P[indx]*dat2$multip_fact5y[indx]))-1))

# cost to serve

dat2$CTS[indx] = (dat2$CTS_M0[indx]* ((((1-(1-(dat2$ChurnY1H1[indx]*dat2$multip_fact5y[indx]))^(dat2$out_con_y1h1[indx]+1)))/(dat2$ChurnY1H1[indx]*dat2$multip_fact5y[indx]))-1)) +
  
  (dat2$CTS_M0[indx]*
     (1-(dat2$ChurnY1H1[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_y1h1[indx] *
     ((((1-(1-(dat2$ChurnY1H2[indx]*dat2$multip_fact5y[indx]))^(dat2$out_con_y1h2[indx]+1)))/(dat2$ChurnY1H2[indx]*dat2$multip_fact5y[indx]))-1)) +
  
  (dat2$CTS_M0[indx]*
     (1-(dat2$ChurnY1H1[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_y1h1[indx] *
     (1-(dat2$ChurnY1H2[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_y1h2[indx] *
     ((((1-(1-(dat2$Churn12M[indx]*dat2$multip_fact5y[indx]))^(dat2$out_con_m12[indx]+1)))/(dat2$Churn12M[indx]*dat2$multip_fact5y[indx]))-1)) +
  
  (dat2$CTS_M0[indx]*
     (1-(dat2$ChurnY1H1[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_y1h1[indx] *
     (1-(dat2$ChurnY1H2[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_y1h2[indx] *
     (1-(dat2$Churn12M[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_m12[indx] *
     ((((1-(1-(dat2$ChurnY2H1[indx]*dat2$multip_fact5y[indx]))^(dat2$out_con_y2h1[indx]+1)))/(dat2$ChurnY2H1[indx]*dat2$multip_fact5y[indx]))-1)) +
  
  (dat2$CTS_M0[indx]*
     (1-(dat2$ChurnY1H1[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_y1h1[indx] *
     (1-(dat2$ChurnY1H2[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_y1h2[indx] *
     (1-(dat2$Churn12M[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_m12[indx] *
     (1-(dat2$ChurnY2H1[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_y2h1[indx] *
     ((((1-(1-(dat2$ChurnY2H2[indx]*dat2$multip_fact5y[indx]))^(dat2$out_con_y2h2[indx]+1)))/(dat2$ChurnY2H2[indx]*dat2$multip_fact5y[indx]))-1)) +
  
  (dat2$CTS_M0[indx]*
     (1-(dat2$ChurnY1H1[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_y1h1[indx] *
     (1-(dat2$ChurnY1H2[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_y1h2[indx] *
     (1-(dat2$Churn12M[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_m12[indx] *
     (1-(dat2$ChurnY2H1[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_y2h1[indx] *
     (1-(dat2$ChurnY2H2[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_y2h2[indx] *
     ((((1-(1-(dat2$Churn24M[indx]*dat2$multip_fact5y[indx]))^(dat2$out_con_m24[indx]+1)))/(dat2$Churn24M[indx]*dat2$multip_fact5y[indx]))-1)) +
  
  (dat2$CTS_M0[indx]*
     (1-(dat2$ChurnY1H1[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_y1h1[indx] *
     (1-(dat2$ChurnY1H2[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_y1h2[indx] *
     (1-(dat2$Churn12M[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_m12[indx] *
     (1-(dat2$ChurnY2H1[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_y2h1[indx] *
     (1-(dat2$ChurnY2H2[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_y2h2[indx] *
     (1-(dat2$Churn24M[indx]*dat2$multip_fact5y[indx]))^dat2$out_con_m24[indx] *
     ((((1-(1-(dat2$ChurnY3P[indx]*dat2$multip_fact5y[indx]))^(dat2$out_con_y3p[indx]+1)))/(dat2$ChurnY3P[indx]*dat2$multip_fact5y[indx]))-1))


indx1 = dat2$contract_expired == 'N' & is.na(dat2$PROMO_CODE)


dat2$COST_NRG_M0 [indx1] = (dat2$AVG_YEARLY_CONSUMPTION[indx1]/12000) * 
  (dat2$ENERGY_COST_MWH[indx1] + dat2$TD_COST_MWH[indx1] + dat2$ECO_MWH[indx1])  

dat2$COST_NRG_MX [indx1] = 1

dat2$CTS_M0 [indx1] = dat2$COST_NRG_M0 [indx1] * (dat2$BDP[indx1]) +
  (dat2$TD_COST_YSC[indx1]/12) +
  (((dat2$COST_OH_FXT[indx1] + dat2$COST_OH_VAR[indx1])/(12*dat2$NUM_REG[indx1]))*
     dat2$EUR_STG_RATE[indx1])

dat2$CTS_MX [indx1] = 1

indx2 = dat2$contract_expired == 'N' & !is.na(dat2$PROMO_CODE)

dat2$COST_NRG_M0 [indx2] = (((dat2$AVG_YEARLY_CONSUMPTION[indx2]/12) * 
                               (dat2$PROMO_RATE[indx2] - dat2$COMM[indx2]) / 
                               (1+dat2$GR_MARGIN[indx2])) +
                              ((dat2$SC_COMMS[indx2]/(dat2$NUM_REG[indx2]*12))*
                                 dat2$EUR_STG_RATE[indx2]))

dat2$COST_NRG_MX [indx2] = (dat2$AVG_YEARLY_CONSUMPTION[indx2]/12000) * 
  (dat2$ENERGY_COST_MWH[indx2] + dat2$TD_COST_MWH[indx2] + dat2$ECO_MWH[indx2])  

dat2$COST_NRG_MX[indx2] = round(dat2$COST_NRG_MX[indx2]/dat2$COST_NRG_M0[indx2],2)
dat2$COST_NRG_MX[!is.finite(dat2$COST_NRG_MX)] <- 1

dat2$CTS_M0 [indx2] = dat2$COST_NRG_M0 [indx2] * (dat2$BDP[indx2]) +
  (dat2$TD_COST_YSC[indx2]/12) +
  (((dat2$COST_OH_FXT[indx2] + dat2$COST_OH_VAR[indx2])/(12*dat2$NUM_REG[indx2]))*
     dat2$EUR_STG_RATE[indx2])

dat2$CTS_MX [indx2] = dat2$COST_NRG_MX [indx2] * (dat2$BDP[indx2]) +
  (dat2$TD_COST_YSC[indx2]/12) +
  (((dat2$COST_OH_FXT[indx2] + dat2$COST_OH_VAR[indx2])/(12*dat2$NUM_REG[indx2]))*
     dat2$EUR_STG_RATE[indx2])

dat2$CTS_MX [indx2] = round(dat2$CTS_MX[indx2]/dat2$CTS_M0[indx2],2)
dat2$CTS_MX[!is.finite(dat2$CTS_MX)] <- 1

# geometric sum for customers still in contract


# dat2$CTS[indx1] = 
#   (B2 *
#      (((1-(1-(dat2$churn_cont[indx1]*dat2$multip_fact5y[indx1]))^(dat2$CON_END_MTHS[indx1]+1))/(dat2$churn_cont[indx1]*dat2$multip_fact5y[indx1]))-1)) +
#   (B2 *
#      (1-(dat2$churn_cont[indx1]*dat2$multip_fact5y[indx1]))^dat2$CON_END_MTHS[indx1] *
#      (((1-(1-(dat2$churn_out_con[indx1]*dat2$multip_fact5y[indx1]))^(61 - dat2$CON_END_MTHS[indx1]))/(dat2$churn_out_con[indx1]*dat2$multip_fact5y[indx1]))-1))

indx1 = indx1 | indx2

dat2$ENRG_CST [indx1] = 
  (dat2$COST_NRG_M0[indx1] *
     ((((1-(1-(dat2$ChurnY1H1[indx1] *dat2$multip_fact5y[indx1] ))^(dat2$con_y1h1[indx1]+1) ))/(dat2$ChurnY1H1[indx1] *dat2$multip_fact5y[indx1]))-1)) +
  
  (dat2$COST_NRG_M0[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h1[indx1] *
     ((((1-(1-(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))^(dat2$con_y1h2[indx1]+1)))/(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))-1)) +
  
  (dat2$COST_NRG_M0[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h1[indx1] *
     (1-(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h2[indx1] *
     ((((1-(1-(dat2$Churn12M[indx1]*dat2$multip_fact5y[indx1]))^(dat2$con_m12[indx1]+1)))/(dat2$Churn12M[indx1]*dat2$multip_fact5y[indx1]))-1)) +
  
  (dat2$COST_NRG_M0[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h1[indx1] *
     (1-(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h2[indx1] *
     (1-(dat2$Churn12M[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_m12[indx1] *
     ((((1-(1-(dat2$ChurnY2H1[indx1]*dat2$multip_fact5y[indx1]))^(dat2$con_y2h1[indx1]+1)))/(dat2$ChurnY2H1[indx1]*dat2$multip_fact5y[indx1]))-1)) +
  
  (dat2$COST_NRG_M0[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h1[indx1] *
     (1-(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h2[indx1] *
     (1-(dat2$Churn12M[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_m12[indx1] *
     (1-(dat2$ChurnY2H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y2h1[indx1] *
     ((((1-(1-(dat2$ChurnY2H2[indx1]*dat2$multip_fact5y[indx1]))^(dat2$con_y2h2[indx1]+1)))/(dat2$ChurnY2H2[indx1]*dat2$multip_fact5y[indx1]))-1)) +
  
  (dat2$COST_NRG_M0[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h1[indx1] *
     (1-(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h2[indx1] *
     (1-(dat2$Churn12M[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_m12[indx1] *
     (1-(dat2$ChurnY2H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y2h1[indx1] *
     (1-(dat2$ChurnY2H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y2h2[indx1] *
     ((((1-(1-(dat2$Churn24M[indx1]*dat2$multip_fact5y[indx1]))^(dat2$con_m24[indx1]+1)))/(dat2$Churn24M[indx1]*dat2$multip_fact5y[indx1]))-1)) +
  
  (dat2$COST_NRG_M0[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h1[indx1] *
     (1-(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h2[indx1] *
     (1-(dat2$Churn12M[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_m12[indx1] *
     (1-(dat2$ChurnY2H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y2h1[indx1] *
     (1-(dat2$ChurnY2H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y2h2[indx1] *
     (1-(dat2$Churn24M[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_m24[indx1] *
     ((((1-(1-(dat2$ChurnY3P[indx1]*dat2$multip_fact5y[indx1]))^(dat2$con_y3p[indx1]+1)))/(dat2$ChurnY3P[indx1]*dat2$multip_fact5y[indx1]))-1)) + 
  
  (dat2$COST_NRG_M0[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h1[indx1] *
     dat2$COST_NRG_MX[indx1] *
     ((((1-(1-(dat2$ChurnY1H1[indx1] *dat2$multip_fact5y[indx1] ))^(dat2$out_con_y1h1[indx1]+1)))/(dat2$ChurnY1H1[indx1] *dat2$multip_fact5y[indx1]))-1)) +
  
  (dat2$COST_NRG_M0[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h1[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_y1h1[indx1] *
     (1-(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h2[indx1] *
     dat2$COST_NRG_MX[indx1] *
     ((((1-(1-(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))^(dat2$out_con_y1h2[indx1]+1)))/(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))-1)) +
  
  (dat2$COST_NRG_M0[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h1[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_y1h1[indx1] *
     (1-(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h2[indx1] *
     (1-(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_y1h2[indx1] *
     (1-(dat2$Churn12M[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_m12[indx1] *
     dat2$COST_NRG_MX[indx1] *
     ((((1-(1-(dat2$Churn12M[indx1]*dat2$multip_fact5y[indx1]))^(dat2$out_con_m12[indx1]+1)))/(dat2$Churn12M[indx1]*dat2$multip_fact5y[indx1]))-1)) +
  
  (dat2$COST_NRG_M0[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h1[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_y1h1[indx1] *
     (1-(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h2[indx1] *
     (1-(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_y1h2[indx1] *
     (1-(dat2$Churn12M[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_m12[indx1] *
     (1-(dat2$Churn12M[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_m12[indx1] *
     (1-(dat2$ChurnY2H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y2h1[indx1] *
     dat2$COST_NRG_MX[indx1] *
     ((((1-(1-(dat2$ChurnY2H1[indx1]*dat2$multip_fact5y[indx1]))^(dat2$out_con_y2h1[indx1]+1)))/(dat2$ChurnY2H1[indx1]*dat2$multip_fact5y[indx1]))-1)) +
  
  (dat2$COST_NRG_M0[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h1[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_y1h1[indx1] *
     (1-(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h2[indx1] *
     (1-(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_y1h2[indx1] *
     (1-(dat2$Churn12M[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_m12[indx1] *
     (1-(dat2$Churn12M[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_m12[indx1] *
     (1-(dat2$ChurnY2H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y2h1[indx1] *
     (1-(dat2$ChurnY2H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_y2h1[indx1] *
     (1-(dat2$ChurnY2H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y2h2[indx1] *
     dat2$COST_NRG_MX[indx1] *
     ((((1-(1-(dat2$ChurnY2H2[indx1]*dat2$multip_fact5y[indx1]))^(dat2$out_con_y2h2[indx1]+1)))/(dat2$ChurnY2H2[indx1]*dat2$multip_fact5y[indx1]))-1)) +
  
  (dat2$COST_NRG_M0[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h1[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_y1h1[indx1] *
     (1-(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h2[indx1] *
     (1-(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_y1h2[indx1] *
     (1-(dat2$Churn12M[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_m12[indx1] *
     (1-(dat2$Churn12M[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_m12[indx1] *
     (1-(dat2$ChurnY2H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y2h1[indx1] *
     (1-(dat2$ChurnY2H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_y2h1[indx1] *
     (1-(dat2$ChurnY2H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y2h2[indx1] *
     (1-(dat2$ChurnY2H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_y2h2[indx1] *
     (1-(dat2$Churn24M[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_m24[indx1] *
     dat2$COST_NRG_MX[indx1] *
     ((((1-(1-(dat2$Churn24M[indx1]*dat2$multip_fact5y[indx1]))^(dat2$out_con_m24[indx1]+1)))/(dat2$Churn24M[indx1]*dat2$multip_fact5y[indx1]))-1)) +
  
  (dat2$COST_NRG_M0[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h1[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_y1h1[indx1] *
     (1-(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h2[indx1] *
     (1-(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_y1h2[indx1] *
     (1-(dat2$Churn12M[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_m12[indx1] *
     (1-(dat2$Churn12M[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_m12[indx1] *
     (1-(dat2$ChurnY2H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y2h1[indx1] *
     (1-(dat2$ChurnY2H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_y2h1[indx1] *
     (1-(dat2$ChurnY2H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y2h2[indx1] *
     (1-(dat2$ChurnY2H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_y2h2[indx1] *
     (1-(dat2$Churn24M[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_m24[indx1] *
     (1-(dat2$Churn24M[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_m24[indx1] *
     (1-(dat2$ChurnY3P[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y3p[indx1] *
     dat2$COST_NRG_MX[indx1] *
     ((((1-(1-(dat2$ChurnY3P[indx1]*dat2$multip_fact5y[indx1]))^(dat2$out_con_y3p[indx1]+1)))/(dat2$ChurnY3P[indx1]*dat2$multip_fact5y[indx1]))-1))

dat2$CTS [indx1] = 
  (dat2$CTS_M0[indx1] *
     ((((1-(1-(dat2$ChurnY1H1[indx1] *dat2$multip_fact5y[indx1] ))^(dat2$con_y1h1[indx1]+1) ))/(dat2$ChurnY1H1[indx1] *dat2$multip_fact5y[indx1]))-1)) +
  
  (dat2$CTS_M0[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h1[indx1] *
     ((((1-(1-(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))^(dat2$con_y1h2[indx1]+1)))/(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))-1)) +
  
  (dat2$CTS_M0[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h1[indx1] *
     (1-(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h2[indx1] *
     ((((1-(1-(dat2$Churn12M[indx1]*dat2$multip_fact5y[indx1]))^(dat2$con_m12[indx1]+1)))/(dat2$Churn12M[indx1]*dat2$multip_fact5y[indx1]))-1)) +
  
  (dat2$CTS_M0[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h1[indx1] *
     (1-(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h2[indx1] *
     (1-(dat2$Churn12M[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_m12[indx1] *
     ((((1-(1-(dat2$ChurnY2H1[indx1]*dat2$multip_fact5y[indx1]))^(dat2$con_y2h1[indx1]+1)))/(dat2$ChurnY2H1[indx1]*dat2$multip_fact5y[indx1]))-1)) +
  
  (dat2$CTS_M0[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h1[indx1] *
     (1-(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h2[indx1] *
     (1-(dat2$Churn12M[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_m12[indx1] *
     (1-(dat2$ChurnY2H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y2h1[indx1] *
     ((((1-(1-(dat2$ChurnY2H2[indx1]*dat2$multip_fact5y[indx1]))^(dat2$con_y2h2[indx1]+1)))/(dat2$ChurnY2H2[indx1]*dat2$multip_fact5y[indx1]))-1)) +
  
  (dat2$CTS_M0[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h1[indx1] *
     (1-(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h2[indx1] *
     (1-(dat2$Churn12M[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_m12[indx1] *
     (1-(dat2$ChurnY2H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y2h1[indx1] *
     (1-(dat2$ChurnY2H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y2h2[indx1] *
     ((((1-(1-(dat2$Churn24M[indx1]*dat2$multip_fact5y[indx1]))^(dat2$con_m24[indx1]+1)))/(dat2$Churn24M[indx1]*dat2$multip_fact5y[indx1]))-1)) +
  
  (dat2$CTS_M0[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h1[indx1] *
     (1-(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h2[indx1] *
     (1-(dat2$Churn12M[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_m12[indx1] *
     (1-(dat2$ChurnY2H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y2h1[indx1] *
     (1-(dat2$ChurnY2H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y2h2[indx1] *
     (1-(dat2$Churn24M[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_m24[indx1] *
     ((((1-(1-(dat2$ChurnY3P[indx1]*dat2$multip_fact5y[indx1]))^(dat2$con_y3p[indx1]+1)))/(dat2$ChurnY3P[indx1]*dat2$multip_fact5y[indx1]))-1)) + 
  
  (dat2$CTS_M0[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h1[indx1] *
     dat2$CTS_MX[indx1] *
     ((((1-(1-(dat2$ChurnY1H1[indx1] *dat2$multip_fact5y[indx1] ))^(dat2$out_con_y1h1[indx1]+1)))/(dat2$ChurnY1H1[indx1] *dat2$multip_fact5y[indx1]))-1)) +
  
  (dat2$CTS_M0[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h1[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_y1h1[indx1] *
     (1-(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h2[indx1] *
     dat2$CTS_MX[indx1] *
     ((((1-(1-(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))^(dat2$out_con_y1h2[indx1]+1)))/(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))-1)) +
  
  (dat2$CTS_M0[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h1[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_y1h1[indx1] *
     (1-(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h2[indx1] *
     (1-(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_y1h2[indx1] *
     (1-(dat2$Churn12M[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_m12[indx1] *
     dat2$CTS_MX[indx1] *
     ((((1-(1-(dat2$Churn12M[indx1]*dat2$multip_fact5y[indx1]))^(dat2$out_con_m12[indx1]+1)))/(dat2$Churn12M[indx1]*dat2$multip_fact5y[indx1]))-1)) +
  
  (dat2$CTS_M0[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h1[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_y1h1[indx1] *
     (1-(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h2[indx1] *
     (1-(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_y1h2[indx1] *
     (1-(dat2$Churn12M[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_m12[indx1] *
     (1-(dat2$Churn12M[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_m12[indx1] *
     (1-(dat2$ChurnY2H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y2h1[indx1] *
     dat2$CTS_MX[indx1] *
     ((((1-(1-(dat2$ChurnY2H1[indx1]*dat2$multip_fact5y[indx1]))^(dat2$out_con_y2h1[indx1]+1)))/(dat2$ChurnY2H1[indx1]*dat2$multip_fact5y[indx1]))-1)) +
  
  (dat2$CTS_M0[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h1[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_y1h1[indx1] *
     (1-(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h2[indx1] *
     (1-(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_y1h2[indx1] *
     (1-(dat2$Churn12M[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_m12[indx1] *
     (1-(dat2$Churn12M[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_m12[indx1] *
     (1-(dat2$ChurnY2H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y2h1[indx1] *
     (1-(dat2$ChurnY2H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_y2h1[indx1] *
     (1-(dat2$ChurnY2H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y2h2[indx1] *
     dat2$CTS_MX[indx1] *
     ((((1-(1-(dat2$ChurnY2H2[indx1]*dat2$multip_fact5y[indx1]))^(dat2$out_con_y2h2[indx1]+1)))/(dat2$ChurnY2H2[indx1]*dat2$multip_fact5y[indx1]))-1)) +
  
  (dat2$CTS_M0[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h1[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_y1h1[indx1] *
     (1-(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h2[indx1] *
     (1-(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_y1h2[indx1] *
     (1-(dat2$Churn12M[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_m12[indx1] *
     (1-(dat2$Churn12M[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_m12[indx1] *
     (1-(dat2$ChurnY2H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y2h1[indx1] *
     (1-(dat2$ChurnY2H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_y2h1[indx1] *
     (1-(dat2$ChurnY2H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y2h2[indx1] *
     (1-(dat2$ChurnY2H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_y2h2[indx1] *
     (1-(dat2$Churn24M[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_m24[indx1] *
     dat2$CTS_MX[indx1] *
     ((((1-(1-(dat2$Churn24M[indx1]*dat2$multip_fact5y[indx1]))^(dat2$out_con_m24[indx1]+1)))/(dat2$Churn24M[indx1]*dat2$multip_fact5y[indx1]))-1)) +
  
  (dat2$CTS_M0[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h1[indx1] *
     (1-(dat2$ChurnY1H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_y1h1[indx1] *
     (1-(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y1h2[indx1] *
     (1-(dat2$ChurnY1H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_y1h2[indx1] *
     (1-(dat2$Churn12M[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_m12[indx1] *
     (1-(dat2$Churn12M[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_m12[indx1] *
     (1-(dat2$ChurnY2H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y2h1[indx1] *
     (1-(dat2$ChurnY2H1[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_y2h1[indx1] *
     (1-(dat2$ChurnY2H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y2h2[indx1] *
     (1-(dat2$ChurnY2H2[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_y2h2[indx1] *
     (1-(dat2$Churn24M[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_m24[indx1] *
     (1-(dat2$Churn24M[indx1]*dat2$multip_fact5y[indx1]))^dat2$out_con_m24[indx1] *
     (1-(dat2$ChurnY3P[indx1]*dat2$multip_fact5y[indx1]))^dat2$con_y3p[indx1] *
     dat2$CTS_MX[indx1] *
     ((((1-(1-(dat2$ChurnY3P[indx1]*dat2$multip_fact5y[indx1]))^(dat2$out_con_y3p[indx1]+1)))/(dat2$ChurnY3P[indx1]*dat2$multip_fact5y[indx1]))-1))

mean(dat2$CTS[indx1], na.rm = T)/mean(dat2$CTS[indx], na.rm = T)
# [1] 0.1961598
mean(dat2$ENRG_CST[indx1], na.rm = T)/mean(dat2$ENRG_CST[indx], na.rm = T)
# [1] 0.7722393

hist(dat2$REV_EST[dat2$REV_EST < 5e4
                  & dat2$REV_EST > 0.1
                  & !is.na(dat2$REV_EST)], 
     xlab = 'Revenue Estimate (local currency)',
     main = '5 yr Revenue Estimate (register level, excl TAX & VAT)',
     col = 3,
     breaks = 100)


incon_indx = dat2$CON_END_MTHS >= 1
dat2[incon_indx,] %>% 
  group_by(PROMO_CODE, REGISTER_DESC) %>% 
  summarise(UNITRATE = paste(unique(UNITRATE), collapse = ', '))


dat2$WC = as.integer(dat2$WEL_CREDIT) * -1
dat2$GR_PROF = dat2$REV_EST - dat2$ENRG_CST 

table(!is.na(dat2$PROMO_CODE), useNA = 'ifany')
# FALSE  TRUE 
# 41980  6300

table(!is.na(dat2$REV_EST),!is.na(dat2$PROMO_CODE), useNA = 'ifany')
#       FALSE  TRUE
# FALSE   769   118
# TRUE  41211  6182
table(!is.na(dat2$ENRG_CST),!is.na(dat2$PROMO_CODE), useNA = 'ifany')
#       FALSE  TRUE
# FALSE   769   303
# TRUE  41211  5997
table(!is.na(dat2$CTS),!is.na(dat2$PROMO_CODE), useNA = 'ifany')
#       FALSE  TRUE
# FALSE   769   303
# TRUE  41211  5997
table(!is.na(dat2$GR_PROF),!is.na(dat2$PROMO_CODE), useNA = 'ifany')
#       FALSE  TRUE
# FALSE   769   303
# TRUE  41211  5997

table(!is.na(dat2$REV_EST), dat2$CATGRY ,!is.na(dat2$PROMO_CODE), useNA = 'ifany')
# NON-PROMO_CODE
# 
# 
#                           CBC   DG1   DG2    GP  GPNS Nightsaver Off-Peak Popular  T011  T012  T014  T015  T021  T022  T024  T052  T053  T062  T063 Weekender
# NO ENERGY COST ESTIMATE     0     0     0     0     2          0       51       0   533    20    33    24    25    30     9    12    22     2     6         0
# ENERGY COST ESTIMATE     1866  1772   958 16907 10823       2123       55    6049     0     0     0     0     0     0     0     0     0     0     0       658
# 
# PROMO_CODE
# 
# 
#                           CBC   DG1   DG2    GP  GPNS Nightsaver Off-Peak Popular  T011  T012  T014  T015  T021  T022  T024  T052  T053  T062  T063 Weekender
# NO ENERGY COST ESTIMATE     0     0     0     0     0          7      108       3     0     0     0     0     0     0     0     0     0     0     0         0
# ENERGY COST ESTIMATE        0     0     0     0     0       2541        0    2835     0     0     0     0     0     0     0     0     0     0     0       806
table(!is.na(dat2$ENRG_CST), dat2$CATGRY ,!is.na(dat2$PROMO_CODE), useNA = 'ifany')
# NON-PROMO_CODE
# 
# 
#                            CBC   DG1   DG2    GP  GPNS Nightsaver Off-Peak Popular  T011  T012  T014  T015  T021  T022  T024  T052  T053  T062  T063 Weekender
# NO ENERGY COST ESTIMATE      0     0     0     0     2          0       51       0   533    20    33    24    25    30     9    12    22     2     6         0
# ENERGY COST ESTIMATE      1866  1772   958 16907 10823       2123       55    6049     0     0     0     0     0     0     0     0     0     0     0       658
# 
# PROMO_CODE
# 
# 
#                           CBC   DG1   DG2    GP  GPNS Nightsaver Off-Peak Popular  T011  T012  T014  T015  T021  T022  T024  T052  T053  T062  T063 Weekender
# NO ENERGY COST ESTIMATE     0     0     0     0     0        177      108      16     0     0     0     0     0     0     0     0     0     0     0         2
# ENERGY COST ESTIMATE        0     0     0     0     0       2371        0    2822     0     0     0     0     0     0     0     0     0     0     0       804

hist(dat2$GR_PROF[dat2$GR_PROF < 1e4
                  & dat2$GR_PROF > -1e4
                  & dat2$GR_PROF != 0
                  & !is.na(dat2$GR_PROF)], 
     xlab = 'Profitability Estimate (local currency)',
     main = '5 yr profitability',
     xlim = c(-1e3,2e3),
     col = 3,
     breaks = 500) 



dat2$DISC10 = rep(0, dim(dat2) [1])
dat2$DISC10 [dat2$DISC2 > 0.095 & dat2$DISC2 < 0.105] = 1
dat2$DF_SF2 = as.integer(dat2$DF_SF)-1

# where contracts go longer than 12 months set a limit of 12 for the purposes of calculating the HW cost
dat2$CON_END_MTHS2 = dat2$CON_END_MTHS
dat2$CON_END_MTHS2[dat2$CON_END_MTHS > 12] = 12

# calculate the hardware cost where a welcome credit was not taken
dat2$HW = rep (0, dim(dat2)[1])

# DF ELEC: apply half the HW cost across the elec meters and registers 
indx1 = dat2$DF_SF2 == 0 & dat2$UTILITY_TYPE_CODE == 'E' & dat2$JURISDICTION == 'ROI' & dat2$DISC10 == 1 & dat2$WC == 0
dat2$HW[indx1] = ((139.19 + 25)/2) * 0.9 * (dat2$CON_END_MTHS2[indx1]/12) / dat2$NUM_REG[indx1]

# DF Gas: apply half the HW cost to the gas meter 
indx1 = dat2$DF_SF2 == 0 & dat2$UTILITY_TYPE_CODE == 'G' & dat2$JURISDICTION == 'ROI' & dat2$DISC10 == 1 & dat2$WC == 0 
dat2$HW[indx1] = ((139.19 + 25)/2) * 0.9 * (dat2$CON_END_MTHS2[indx1]/12) 

# SF ELEC: apply the full HW cost across the elec meters and registers 
indx1 = dat2$DF_SF2 == 1 & dat2$UTILITY_TYPE_CODE == 'E' & dat2$JURISDICTION == 'ROI' & dat2$DISC10 == 1 & dat2$WC == 0
dat2$HW[indx1] = (89.92 + 25) * 0.9  * (dat2$CON_END_MTHS2[indx1]/12) / dat2$NUM_REG[indx1]

# SF GAS: apply the full HW cost to the gas meter 
indx1 = dat2$DF_SF2 == 1 & dat2$UTILITY_TYPE_CODE == 'G' & dat2$JURISDICTION == 'ROI' & dat2$DISC10 == 1 & dat2$WC == 0
dat2$HW[indx1] = (89.92 + 25) * 0.9 * (dat2$CON_END_MTHS2[indx1]/12) 


# distribute the welcome credit cost amongst the meters and registers
dat2$WC2 = rep (0, dim(dat2)[1])

# DF ELEC: apply half the HW cost across the elec meters and registers 
indx1 = dat2$DF_SF2 == 0 & dat2$UTILITY_TYPE_CODE == 'E' & dat2$WC > 0
dat2$WC2[indx1] = (dat2$WC[indx1]/2)  * (dat2$CON_END_MTHS2[indx1]/12) / dat2$NUM_REG[indx1]

# DF Gas: ammortise half the welcome credit cost 
indx1 = ( dat2$DF_SF2 == 0 ) & dat2$UTILITY_TYPE_CODE == 'G' & dat2$WC > 0
dat2$WC2[indx1] = (dat2$WC[indx1]/2) * (dat2$CON_END_MTHS2[indx1]/12) 

# SF ELEC: apply the full HW cost across the elec meters and registers 
indx1 = dat2$DF_SF2 == 1 & dat2$UTILITY_TYPE_CODE == 'E' & dat2$WC > 0
dat2$WC2[indx1] = dat2$WC[indx1] * (dat2$CON_END_MTHS2[indx1]/12) / dat2$NUM_REG[indx1]

# SF Gas: ammortise the welcome credit cost 
indx1 = ( dat2$DF_SF2 == 1 ) & dat2$UTILITY_TYPE_CODE == 'G' & dat2$WC > 0
dat2$WC2[indx1] = dat2$WC[indx1] * (dat2$CON_END_MTHS2[indx1]/12)

# 75% redeem and we can exclude the VAT from the cost
dat2$WC2 = (dat2$WC2 * 0.75)/1.135

setwd("S:/Retail/IRL/Supply/Marketing/MARKETING SUPPLY/Analytics & Products/Analytics/MM WIP/BE Value Model")

# CTA <- read.csv('BE_CTA_SEP_2021.csv', header = T, skip =)
# Added the code to fetch CTA data directly from DM2.0 Analytics
conn <- dbConnect(drv, username =  dbname = connect.string, prefetch = FALSE,
                  bulk_read = 1000L, stmt_cache = 0L, external_credentials = FALSE,
                  sysdba = FALSE)
query_res_cta <- dbGetQuery(conn, "select * from BE_CTA");
CTA <- as.data.frame(query_res_cta)
dbDisconnect(conn)

CTA
#   CTA_VAR CTA_FXD
# 1  111.04   62.73

dat2$CTA_VAR = CTA$CTA_VAR * dat2$EUR_STG_RATE
dat2$CTA_FXD = CTA$CTA_FXD * dat2$EUR_STG_RATE

# table(dat2$CON_END_MTHS, useNA = 'ifany')
# table(dat2$CTA_VAR, useNA = 'ifany')
# table(dat2$CTA_FXD, useNA = 'ifany')
# table(dat2$ACQ_CHANNEL, useNA = 'ifany')
# table(dat2$JURISDICTION, useNA = 'ifany')
# table(dat2$DF_SF, useNA = 'ifany')

dat2$CTA = 0
indx = dat2$TENURE_MTHS <= ifelse(is.na(dat2$NUM_YRS_CON), 24, dat2$NUM_YRS_CON*12-dat2$CON_END_MTHS)
dat2$CTA[indx] = (dat2$CTA_VAR[indx] + dat2$CTA_FXD[indx]) * (dat2$CON_END_MTHS[indx]/(12 * ifelse(is.na(dat2$NUM_YRS_CON[indx]), 1, dat2$NUM_YRS_CON[indx]) * dat2$NUM_REG[indx]))
dat2$CTA [indx & dat2$CON_END_MTHS > 12] = (dat2$CTA_VAR[indx & dat2$CON_END_MTHS > 12] + dat2$CTA_FXD[indx & dat2$CON_END_MTHS > 12]) * (dat2$CON_END_MTHS[indx & dat2$CON_END_MTHS > 12]/(12 * ifelse(is.na(dat2$NUM_YRS_CON[indx & dat2$CON_END_MTHS > 12]), 2, dat2$NUM_YRS_CON[indx & dat2$CON_END_MTHS > 12]) * dat2$NUM_REG[indx & dat2$CON_END_MTHS > 12]))
dat2$CTA [indx & dat2$CON_END_MTHS > 24] = (dat2$CTA_VAR[indx & dat2$CON_END_MTHS > 24] + dat2$CTA_FXD[indx & dat2$CON_END_MTHS > 24]) * (dat2$CON_END_MTHS[indx & dat2$CON_END_MTHS > 24]/(12 * ifelse(is.na(dat2$NUM_YRS_CON[indx & dat2$CON_END_MTHS > 24]), 3, dat2$NUM_YRS_CON[indx & dat2$CON_END_MTHS > 24]) * dat2$NUM_REG[indx & dat2$CON_END_MTHS > 24]))
dat2$CTA [indx & dat2$CON_END_MTHS > 36] = (dat2$CTA_VAR[indx & dat2$CON_END_MTHS > 36] + dat2$CTA_FXD[indx & dat2$CON_END_MTHS > 36]) * (dat2$CON_END_MTHS[indx & dat2$CON_END_MTHS > 36]/(12 * ifelse(is.na(dat2$NUM_YRS_CON[indx & dat2$CON_END_MTHS > 36]), 4, dat2$NUM_YRS_CON[indx & dat2$CON_END_MTHS > 36]) * dat2$NUM_REG[indx & dat2$CON_END_MTHS > 36]))
dat2$CTA [indx & dat2$CON_END_MTHS > 48] = (dat2$CTA_VAR[indx & dat2$CON_END_MTHS > 48] + dat2$CTA_FXD[indx & dat2$CON_END_MTHS > 48]) * (dat2$CON_END_MTHS[indx & dat2$CON_END_MTHS > 48]/(12 * ifelse(is.na(dat2$NUM_YRS_CON[indx & dat2$CON_END_MTHS > 48]), 5, dat2$NUM_YRS_CON[indx & dat2$CON_END_MTHS > 48]) * dat2$NUM_REG[indx & dat2$CON_END_MTHS > 48]))

table(dat2$CTA, useNA = 'ifany') # 0 nulls

# dat2[is.na(dat2$CTA),] # ROI Outbound Sales in NI !


dat2$NT_PROF = dat2$GR_PROF - dat2$CTA - dat2$CTS - dat2$WC2 - dat2$HW

lwr = -5e3
upr = 5e4
hist(dat2$NT_PROF [dat2$NT_PROF > lwr & dat2$NT_PROF < upr], 
     xlab = 'Profitability Estimate (Local Currency)',
     main = '5 yr Profitability',
     xlim = c(lwr,upr),
     col = 3,
     breaks = 200) 

hist(dat2$ENERGY_COST_MWH [dat2$ENERGY_COST_MWH > lwr/100 & 
                             dat2$ENERGY_COST_MWH < upr/100], 
     xlab = 'Energy Cost',
     main = '5 yr Profitability',
     xlim = c(lwr/100,upr/100),
     col = 3,
     breaks = 200) 

hist(dat2$AVG_YEARLY_CONSUMPTION [dat2$AVG_YEARLY_CONSUMPTION > lwr & 
                                    dat2$AVG_YEARLY_CONSUMPTION < upr &
                                    dat2$AVG_YEARLY_CONSUMPTION%/%1 != 0], 
     xlab = 'Annual consumption',
     main = '5 yr Profitability',
     xlim = c(lwr,upr),
     col = 3,
     breaks = 200)

hist(dat2$REV_EST [dat2$REV_EST > lwr & 
                     dat2$REV_EST < upr & 
                     round(dat2$REV_EST,0) != 0], 
     xlab = 'Revenue',
     main = '5 yr Profitability',
     xlim = c(lwr,upr),
     col = 3,
     breaks = 200) 

############################################## 18 May 2020

dat2$LOSS[dat2$NT_PROF < 0] = 1
dat2$LOSS[dat2$NT_PROF >= 0] = 0

table(dat2$LOSS, dat2$JURISDICTION, useNA = 'ifany')
#         NI   ROI
# 0     7801 27382
# 1     2620  3656
# <NA>  5536  1346

table(dat2$ACQ_CHANNEL, dat2$LOSS, useNA = 'ifany')

#                     0     1  <NA>
# Anything         1282   304   159
# Commercial TPI  15215  3982   295
# DOM              2091   949    25
# Door to Door     3816   746     7
# Employee         2565  1025    93
# Homemoves         212   130    30
# Online            164   106     7
# Other            4500  1388    33
# Property Button    52     0     0
# Rigney           3110  1412    62
# SME               403   110   251
# SME Telesales    1565   334    64
# Unknown          1292   455    46

table(dat2$LOSS, dat2$REGISTER_DESC, useNA = 'ifany')
#       24hr   Day GAS_24HR Heating   Low Night Normal   NSH Off Peak
# 0    20770  6826     1810     259   210  4381    231   696        0
# 1     2233   290       24     331    38  2598     16   746        0
# <NA>  3335   918       35     345   485   875    484   295      110

table(dat2$LOSS, !is.na(dat2$PROMO_CODE), useNA = 'ifany')
#      FALSE  TRUE
# 0    32296  2887
# 1     5377   899
# <NA>  4366  2516

table(dat2$LOSS, dat2$UTILITY_TYPE_CODE, useNA = 'ifany')
#           E      G
# 0    256614  72199
# 1    201068   5448
# <NA>  14998   7380

table(dat2$LOSS, dat2$EBILL, useNA = 'ifany')
#          0     1
# 0    23166 12017
# 1     3847  2429
# <NA>  4761  2121

table(dat2$LOSS, dat2$DD_PAY, useNA = 'ifany')
#          N     Y
# 0     5142 30041
# 1     1104  5172
# <NA>   732  6150

table(dat2$LOSS, dat2$PAYMENT_CHANNEL_CATEGORY, useNA = 'ifany')
#      Credit Prepay
# 0     35178      5
# 1      6273      3
# <NA>   6862     20

table(dat2$LOSS, dat2$DF_SF, useNA = 'ifany')
#         DF    SF
# 0     1611 33572
# 1       83  6193
# <NA>    56  6826

table(dat2$LOSS, dat2$MARKETING_OPT_IN, useNA = 'ifany')
#          0     1
# 0    23976 11207
# 1     4324  1952
# <NA>  3424  3458

table(dat2$LOSS, dat2$EXPRT, useNA = 'ifany')
#          0     1
# 0    35098    85
# 1     6267     9
# <NA>  6784    98

dat3 = dat2[is.na(dat2$LOSS), ]
getwd()
# write.csv(dat3, 'no_profit_22_May_2020.csv', row.names = F)

# filters for the 3 groups by Jurisdiction and utility
indx1 = dat2$JURISDICTION == 'ROI' & dat2$UTILITY_TYPE_CODE == 'E' &  !is.na(dat2$NT_PROF)
indx2 = dat2$JURISDICTION == 'ROI' & dat2$UTILITY_TYPE_CODE == 'G' & !is.na(dat2$NT_PROF)
indx3 = dat2$JURISDICTION == 'NI' & dat2$UTILITY_TYPE_CODE == 'E' & !is.na(dat2$NT_PROF)


sum(indx1)
# [1] 30460
sum(indx2)
# [1] 1866
sum(indx3)
# [1] 14882


# add deciles and percentiles for net profit
dat2$prof_decile[indx1] = (ecdf(dat2$NT_PROF[indx1])(dat2$NT_PROF[indx1])*1000)%/%100
dat2$prof_decile[indx2] = (ecdf(dat2$NT_PROF[indx2])(dat2$NT_PROF[indx2])*1000)%/%100
dat2$prof_decile[indx3] = (ecdf(dat2$NT_PROF[indx3])(dat2$NT_PROF[indx3])*1000)%/%100


dat2$prof_decile [dat2$prof_decile == 10] = 9
dat2$prof_decile = dat2$prof_decile + 1

table (dat2$prof_decile, dat2$JURISDICTION, dat2$UTILITY_TYPE_CODE, useNA = 'ifany')
# , ,  = E
# 
# 
#        NI  ROI
# 1    1488 3045
# 2    1488 3046
# 3    1488 3046
# 4    1488 3046
# 5    1488 3046
# 6    1489 3046
# 7    1488 3046
# 8    1488 3046
# 9    1488 3046
# 10   1489 3047
# <NA> 1070    2
# 
# , ,  = G
# 
# 
#        NI  ROI
# 1       0  186
# 2       0  187
# 3       0  186
# 4       0  187
# 5       0  186
# 6       0  187
# 7       0  187
# 8       0  186
# 9       0  187
# 10      0  187
# <NA>    0    0

# table(dat2$prof_decile[indx1])
# table(dat2$prof_decile[indx2])
# table(dat2$prof_decile[indx3])

dat2$prof_percentile [indx1] = round(ecdf(dat2$NT_PROF[indx1])(dat2$NT_PROF[indx1])*100,2)
dat2$prof_percentile [indx2] = round(ecdf(dat2$NT_PROF[indx2])(dat2$NT_PROF[indx2])*100,2)
dat2$prof_percentile [indx3] = round(ecdf(dat2$NT_PROF[indx3])(dat2$NT_PROF[indx3])*100,2)

indx = !is.na(dat2$prof_percentile) & dat2$prof_percentile > 0.1 & dat2$prof_percentile < 99.9
# drop the lowest and upper 0.1%

dat2b = dat2[,c(which(colnames(dat2) == 'CUSTOMER_ID'),
                which(colnames(dat2) == 'PREMISE_ID'),
                which(colnames(dat2) == 'MPRN'),
                which(colnames(dat2) == 'UTILITY_TYPE_CODE'),
                which(colnames(dat2) == 'JURISDICTION'),
                which(colnames(dat2) == 'CATGRY'),
                which(colnames(dat2) == 'ACQ_CHANNEL'),
                which(colnames(dat2) == 'NT_PROF'))]

CNT = function(x, na.rm = T) sum( !is.na(x) ) # gets the counts

# aggregate data to MPRN level
sum_dat2b = aggregate(x = dat2b$NT_PROF,
                      by = list(dat2b$CUSTOMER_ID, dat2b$PREMISE_ID, dat2b$MPRN, dat2b$UTILITY_TYPE_CODE, dat2b$JURISDICTION, dat2b$CATGRY, dat2b$ACQ_CHANNEL),
                      FUN = sum)
colnames(sum_dat2b) = c('CUSTOMER_ID', 'PREMISE_ID', 'MPRN', 'UTILITY_TYPE_CODE', 'JURISDICTION', 'CATEGORY', 'ACQ_CHANNEL', 'PROFIT')
sum_dat2b = na.omit(sum_dat2b)


# filters for the 3 groups by Jurisdiction and utility

indx1 = sum_dat2b$JURISDICTION == 'ROI' & sum_dat2b$UTILITY_TYPE_CODE == 'E' & sum_dat2b$CATEGORY == 'GP' & !is.na(sum_dat2b$PROFIT)
indx2 = sum_dat2b$JURISDICTION == 'ROI' & sum_dat2b$UTILITY_TYPE_CODE == 'E' & sum_dat2b$CATEGORY == 'GPNS' & !is.na(sum_dat2b$PROFIT)
indx3 = sum_dat2b$JURISDICTION == 'ROI' & sum_dat2b$UTILITY_TYPE_CODE == 'G' & !is.na(sum_dat2b$PROFIT)
indx4 = sum_dat2b$JURISDICTION == 'NI' & sum_dat2b$UTILITY_TYPE_CODE == 'E' & sum_dat2b$CATEGORY == 'Popular' & !is.na(sum_dat2b$PROFIT)
indx5 = sum_dat2b$JURISDICTION == 'NI' & sum_dat2b$UTILITY_TYPE_CODE == 'E' & sum_dat2b$CATEGORY == 'Nightsaver' & !is.na(sum_dat2b$PROFIT)
indx6 = sum_dat2b$JURISDICTION == 'NI' & sum_dat2b$UTILITY_TYPE_CODE == 'E' & sum_dat2b$CATEGORY == 'Weekender' & !is.na(sum_dat2b$PROFIT)


# add deciles and percentiles for net profit
sum_dat2b$prof_decile[indx1] = (ecdf(sum_dat2b$PROFIT[indx1])(sum_dat2b$PROFIT[indx1])*1000)%/%100
sum_dat2b$prof_decile[indx2] = (ecdf(sum_dat2b$PROFIT[indx2])(sum_dat2b$PROFIT[indx2])*1000)%/%100
sum_dat2b$prof_decile[indx3] = (ecdf(sum_dat2b$PROFIT[indx3])(sum_dat2b$PROFIT[indx3])*1000)%/%100
sum_dat2b$prof_decile[indx4] = (ecdf(sum_dat2b$PROFIT[indx4])(sum_dat2b$PROFIT[indx4])*1000)%/%100
sum_dat2b$prof_decile[indx5] = (ecdf(sum_dat2b$PROFIT[indx5])(sum_dat2b$PROFIT[indx5])*1000)%/%100
sum_dat2b$prof_decile[indx6] = (ecdf(sum_dat2b$PROFIT[indx6])(sum_dat2b$PROFIT[indx6])*1000)%/%100


sum_dat2b$prof_decile [sum_dat2b$prof_decile == 10] = 9
sum_dat2b$prof_decile = sum_dat2b$prof_decile + 1

table (sum_dat2b$prof_decile, sum_dat2b$CATEGORY, sum_dat2b$UTILITY_TYPE_CODE, useNA = 'ifany')

# , ,  = E
# 
# 
# NI  ROI
# 1  1038 2110
# 2  1038 2111
# 3  1039 2110
# 4  1038 2111
# 5  1039 2110
# 6  1038 2111
# 7  1039 2111
# 8  1038 2110
# 9  1039 2111
# 10 1039 2111
# 
# , ,  = G
# 
# 
# NI  ROI
# 1     0  186
# 2     0  186
# 3     0  187
# 4     0  186
# 5     0  187
# 6     0  186
# 7     0  187
# 8     0  186
# 9     0  187
# 10    0  187

# table(sum_dat2b$prof_decile[indx1])
# table(sum_dat2b$prof_decile[indx2])
# table(sum_dat2b$prof_decile[indx3])

sum_dat2b$prof_percentile [indx1] = round(ecdf(sum_dat2b$PROFIT[indx1])(sum_dat2b$PROFIT[indx1])*100,2)
sum_dat2b$prof_percentile [indx2] = round(ecdf(sum_dat2b$PROFIT[indx2])(sum_dat2b$PROFIT[indx2])*100,2)
sum_dat2b$prof_percentile [indx3] = round(ecdf(sum_dat2b$PROFIT[indx3])(sum_dat2b$PROFIT[indx3])*100,2)
sum_dat2b$prof_percentile [indx4] = round(ecdf(sum_dat2b$PROFIT[indx4])(sum_dat2b$PROFIT[indx4])*100,2)
sum_dat2b$prof_percentile [indx5] = round(ecdf(sum_dat2b$PROFIT[indx5])(sum_dat2b$PROFIT[indx5])*100,2)
sum_dat2b$prof_percentile [indx6] = round(ecdf(sum_dat2b$PROFIT[indx6])(sum_dat2b$PROFIT[indx6])*100,2)


indx = !is.na(sum_dat2b$prof_percentile) & sum_dat2b$prof_percentile > 0.1 & sum_dat2b$prof_percentile < 99.9

sum1 <- sum_dat2b[indx, c('prof_decile', 'JURISDICTION' , 'CATEGORY', 'UTILITY_TYPE_CODE', 'PROFIT')]%>%
  group_by(JURISDICTION, CATEGORY, UTILITY_TYPE_CODE, prof_decile) %>%
  summarise_at('PROFIT', funs(mean, min, max), na.rm=TRUE)
sum2 <- sum_dat2b[indx, c('prof_decile', 'JURISDICTION' , 'CATEGORY', 'UTILITY_TYPE_CODE', 'PROFIT')] %>%
  group_by(JURISDICTION, CATEGORY, UTILITY_TYPE_CODE, prof_decile) %>%
  summarise(n = n()) %>%
  full_join(sum1)

# write.csv(sum2, paste0('BE_Profit_by_decile_by_util_by_Jur_', month_day, '.csv'), row.names = F)

print(sum2, n = 400)

lev = levels(sum_dat2b$ACQ_CHANNEL)
l = length(lev)

for (i in 1:l) {
  
  sum_dat2b$prof_decile_a = 0
  sum_dat2b$prof_percentile_a = 0
  
  indx1 = sum_dat2b$JURISDICTION == 'ROI' & sum_dat2b$UTILITY_TYPE_CODE == 'E' & sum_dat2b$ACQ_CHANNEL == lev[i] & !is.na(sum_dat2b$PROFIT)
  indx2 = sum_dat2b$JURISDICTION == 'ROI' & sum_dat2b$UTILITY_TYPE_CODE == 'G' & sum_dat2b$ACQ_CHANNEL == lev[i] & !is.na(sum_dat2b$PROFIT)
  indx3 = sum_dat2b$JURISDICTION == 'NI' & sum_dat2b$UTILITY_TYPE_CODE == 'E' & sum_dat2b$ACQ_CHANNEL == lev[i] & !is.na(sum_dat2b$PROFIT)
  
  # add deciles and percentiles for net profit
  if ( sum(sum_dat2b$ACQ_CHANNEL[indx1] == lev[i]) > 100 ) {sum_dat2b$prof_decile_a[indx1] = (ecdf(sum_dat2b$PROFIT[indx1])(sum_dat2b$PROFIT[indx1])*1000)%/%100}
  if ( sum(sum_dat2b$ACQ_CHANNEL[indx2] == lev[i]) > 100 ) {sum_dat2b$prof_decile_a[indx2] = (ecdf(sum_dat2b$PROFIT[indx2])(sum_dat2b$PROFIT[indx2])*1000)%/%100}
  if ( sum(sum_dat2b$ACQ_CHANNEL[indx3] == lev[i]) > 100 ) {sum_dat2b$prof_decile_a[indx3] = (ecdf(sum_dat2b$PROFIT[indx3])(sum_dat2b$PROFIT[indx3])*1000)%/%100}
  
  sum_dat2b$prof_decile_a [sum_dat2b$prof_decile_a == 10] = 9
  sum_dat2b$prof_decile_a = sum_dat2b$prof_decile_a + 1
  
  if ( sum(sum_dat2b$ACQ_CHANNEL[indx1] == lev[i]) > 100 ) {sum_dat2b$prof_percentile_a [indx1] = round(ecdf(sum_dat2b$PROFIT[indx1])(sum_dat2b$PROFIT[indx1])*100,2)}
  if ( sum(sum_dat2b$ACQ_CHANNEL[indx2] == lev[i]) > 100 ) {sum_dat2b$prof_percentile_a [indx2] = round(ecdf(sum_dat2b$PROFIT[indx2])(sum_dat2b$PROFIT[indx2])*100,2)}
  if ( sum(sum_dat2b$ACQ_CHANNEL[indx3] == lev[i]) > 100 ) {sum_dat2b$prof_percentile_a [indx3] = round(ecdf(sum_dat2b$PROFIT[indx3])(sum_dat2b$PROFIT[indx3])*100,2)}
  
  indx = !is.na(sum_dat2b$prof_percentile_a) & sum_dat2b$prof_percentile_a > 0.1 & sum_dat2b$prof_percentile_a < 99.9
  
  
  sum2 <- sum_dat2b[indx, c('prof_decile_a', 'JURISDICTION' , 'UTILITY_TYPE_CODE', 'ACQ_CHANNEL', 'PROFIT')]%>%
    group_by(JURISDICTION, UTILITY_TYPE_CODE, ACQ_CHANNEL, prof_decile_a) %>%
    summarise_at('PROFIT', funs(mean, min, max), na.rm=TRUE)
  sum3 <- sum_dat2b[indx, c('prof_decile_a', 'JURISDICTION' , 'UTILITY_TYPE_CODE', 'ACQ_CHANNEL', 'PROFIT')] %>%
    group_by(JURISDICTION, UTILITY_TYPE_CODE, ACQ_CHANNEL, prof_decile_a) %>%
    summarise(n = n()) %>%
    full_join(sum2)
  
  # write.csv(sum3, paste0('BE_Profit_by_decile_by_util_by_Jur_by_', lev[i], '_', month_day, '.csv'), row.names = F)
  
  sum3 = sum3[,-c(which(colnames(sum3) == 'ACQ_CHANNEL'),
                  which(colnames(sum3) == 'min'),
                  which(colnames(sum3) == 'max'))]
  
  a = which(colnames(sum3) =='n')
  b = which(colnames(sum3) =='mean')
  
  colnames(sum3) = c(colnames(sum3) [1:(a-1)], paste0('CNT_', lev[i]), lev[i])
  
  if (i == 1) { sum4 = sum3}
  else { sum4 <- sum4 %>% full_join (sum3, by = c("JURISDICTION", "UTILITY_TYPE_CODE","prof_decile_a"))}
  
  # write.csv(sum4, paste0('BE_Profit_by_decile_by_util_by_Jur_by_channel_', month_day, '.csv'), row.names = F)
  
  
}

# Code to aggregate the data so that we can merge it with Product details
setwd("S:/Retail/IRL/Supply/Marketing/MARKETING SUPPLY/Analytics & Products/Analytics/MM WIP/Value Model")


# PE_dat = read.csv(paste0("PE_data_",'Aug14',".csv"), header = T)
# PE_active = PE_dat[PE_dat$STATUS == 0, c(which(colnames(PE_dat) == 'CUSTOMER_ID'),
#                                          which(colnames(PE_dat) == 'PREMISE_ID'),
#                                          which(colnames(PE_dat) == 'MPRN'),
#                                          which(colnames(PE_dat) == 'ACQUISITION_DATE_SID'),
#                                          which(colnames(PE_dat) == 'PRICE_ELIGIBILITY_STATUS_DESC'))]
# 
# 
# PE_active$MPRN[nchar(PE_active$MPRN) < 7] = formatC(PE_active$MPRN[nchar(PE_active$MPRN) < 7], width = 7, format = "d", flag = "0")
# PE_active$MPRN = as.character(PE_active$MPRN)
# 
# 
# dat4 = aggregate(x = list(dat2$NT_PROF[indx],
#                           dat2$EAC[indx],
#                           dat2$AVG_YEARLY_CONSUMPTION[indx]),
#                  by = list(dat2$CUSTOMER_ID[indx],
#                            dat2$PREMISE_ID[indx],
#                            dat2$MPRN[indx]),
#                  FUN = sum)
# 
# colnames(dat4) = c('CUSTOMER_ID',
#                    'PREMISE_ID',
#                    'MPRN',
#                    'PROF',
#                    'EAC',
#                    'ANNUAL_USAGE')
# 
# 
# dat3 = merge(dat4, PE_active, by = c('CUSTOMER_ID',
#                                      'PREMISE_ID',
#                                      'MPRN'))
# 
# dat3$RATIO = rep(0, dim(dat3)[1])
# indx = dat3$ANNUAL_USAGE != dat3$EAC & !is.na(dat3$ANNUAL_USAGE) & !is.na(dat3$EAC)
# dat3$RATIO[indx] = dat3$ANNUAL_USAGE[indx]/dat3$EAC[indx]
# 
# sum_table = aggregate(x = list(dat3$PROF,
#                                dat3$ANNUAL_USAGE,
#                                dat3$EAC),
#                       by = list(dat3$PRICE_ELIGIBILITY_STATUS_DESC),
#                       FUN = median,
#                       na.action = NULL,
#                       na.rm = T)
# colnames(sum_table) = c('PRICE_ELIGIBILITY_STATUS_DESC', 'MED_PROF', 'MED_USAGE', 'MED_EAC')
# sum_table
# 
# 
# sum_table2 = aggregate(x = list(dat3$PROF,
#                                 dat3$ANNUAL_USAGE,
#                                 dat3$EAC),
#                       by = list(dat3$PRICE_ELIGIBILITY_STATUS_DESC),
#                       FUN = mean,
#                       na.action = NULL,
#                       na.rm = T)
# colnames(sum_table2) = c('PRICE_ELIGIBILITY_STATUS_DESC', 'MEAN_PROF', 'MEAN_USAGE', 'MEAN_EAC')
# sum_table2
# 
# sum_table3 = aggregate(x = list(dat3$PROF,
#                                 dat3$ANNUAL_USAGE,
#                                 dat3$EAC),
#                        by = list(dat3$PRICE_ELIGIBILITY_STATUS_DESC),
#                        FUN = sum,
#                        na.action = NULL,
#                        na.rm = T)
# colnames(sum_table3) = c('PRICE_ELIGIBILITY_STATUS_DESC', 'SUM_PROF', 'SUM_USAGE', 'SUM_EAC')
# sum_table3
# 
# 
# sum_table4 = aggregate(x = list(dat3$PROF,
#                                 dat3$ANNUAL_USAGE,
#                                 dat3$EAC),
#                        by = list(dat3$PRICE_ELIGIBILITY_STATUS_DESC),
#                        FUN = CNT)
# colnames(sum_table4) = c('PRICE_ELIGIBILITY_STATUS_DESC', 'COUNT_PROF', 'COUNT_USAGE', 'COUNT_EAC')
# sum_table4
# 
# sum_table = merge (sum_table, sum_table2, by = 'PRICE_ELIGIBILITY_STATUS_DESC', all = T)
# sum_table = merge (sum_table, sum_table3, by = 'PRICE_ELIGIBILITY_STATUS_DESC', all = T)
# sum_table = merge (sum_table, sum_table4, by = 'PRICE_ELIGIBILITY_STATUS_DESC', all = T)
# 
# write.csv(sum_table, paste0('PE_SUMMARY_DATA_', month_day, '.csv'), row.names = F)
# 
# 
# sum_table5 = aggregate(dat3$RATIO[ is.finite(dat3$RATIO) &  dat3$RATIO != 0 ],
#                       list(dat3$PRICE_ELIGIBILITY_STATUS_DESC[ is.finite(dat3$RATIO) & dat3$RATIO != 0 ]),
#                       FUN = median, na.action = NULL, na.rm = T)
# colnames(sum_table5) = c('PRICE_ELIGIBILITY_STATUS_DESC', 'MEDIAN_RATIO')
# sum_table5
# 
# sum_table6 = aggregate(dat3$RATIO [ is.finite(dat3$RATIO) &  dat3$RATIO != 0 ],
#                       list(dat3$PRICE_ELIGIBILITY_STATUS_DESC [ is.finite(dat3$RATIO) &  dat3$RATIO != 0 ]),
#                       FUN = mean, na.action = NULL, na.rm = T)
# colnames(sum_table6) = c('PRICE_ELIGIBILITY_STATUS_DESC', 'MEAN_RATIO')
# sum_table6
# 
# sum_table7 = aggregate(dat3$RATIO [ is.finite(dat3$RATIO) &  dat3$RATIO != 0 ],
#                        list(dat3$PRICE_ELIGIBILITY_STATUS_DESC [ is.finite(dat3$RATIO) &  dat3$RATIO != 0 ]),
#                        FUN = CNT)
# colnames(sum_table7) = c('PRICE_ELIGIBILITY_STATUS_DESC', 'COUNT_RATIO')
# sum_table7
# 
# sum_table = merge (sum_table, sum_table5, by = 'PRICE_ELIGIBILITY_STATUS_DESC', all = T)
# sum_table = merge (sum_table, sum_table6, by = 'PRICE_ELIGIBILITY_STATUS_DESC', all = T)
# sum_table = merge (sum_table, sum_table7, by = 'PRICE_ELIGIBILITY_STATUS_DESC', all = T)
# 
# 
# write.csv(sum_table, paste0('PE_SUMMARY_DATA_V2_', month_day, '.csv'), row.names = F)


# get aggregates of Hardware and welcome credit to make sure they're being distributed
# amongst the registers correctly and that they thus sum up correctly

# filters for the 3 groups by Jurisdiction and utility
indx1 = sum_dat2b$JURISDICTION == 'ROI' & sum_dat2b$UTILITY_TYPE_CODE == 'E' & !is.na(sum_dat2b$PROFIT)
indx2 = sum_dat2b$JURISDICTION == 'ROI' & sum_dat2b$UTILITY_TYPE_CODE == 'G' & !is.na(sum_dat2b$PROFIT)
indx3 = sum_dat2b$JURISDICTION == 'NI' & sum_dat2b$UTILITY_TYPE_CODE == 'E' & !is.na(sum_dat2b$PROFIT)


sum1 <- sum_dat2b[indx1, c('prof_decile', 'PROFIT')]%>%
  group_by(prof_decile) %>%
  summarise_at("PROFIT", funs(mean, min, max), na.rm=TRUE)
sum_dat2b[indx1, c('prof_decile', 'PROFIT')] %>%
  group_by(prof_decile) %>%
  summarise(n = n()) %>%
  full_join(sum1)

# Joining, by = "prof_percentile"
# # A tibble: 9,869 x 5
#   prof_percentile     n      mean       min       max
#             <dbl> <int>     <dbl>     <dbl>     <dbl>
# 1            0        1 -6169506. -6169506. -6169506.
# 2            0.01     2 -2794302. -3086059. -2502545.
# 3            0.02     2 -2141450. -2482323. -1800578.
# 4            0.03     2 -1531190. -1574986. -1487394.
# 5            0.04     2 -1484962. -1485559. -1484365.
# 6            0.05     2 -1413399. -1483322. -1343476.
# 7            0.06     2 -1328483. -1337445. -1319521.
# 8            0.07     2 -1159418. -1218621. -1100215.
# 9            0.08     2 -1043753. -1052465. -1035040.
#10            0.09     3  -560100.  -717810.  -245328.
# # ... with 9,859 more rows

sum1 <- sum_dat2b[indx1, c('prof_percentile', 'PROFIT')]%>%
  group_by(prof_percentile) %>%
  summarise_at("PROFIT", funs(mean, min, max), na.rm=TRUE)
sum_dat2b[indx1, c('prof_percentile', 'PROFIT')] %>%
  group_by(prof_percentile) %>%
  summarise(n = n()) %>%
  full_join(sum1)

mean(sum_dat2b$PROFIT[indx1], na.rm = T)/5
# [1] 931.4486
median(sum_dat2b$PROFIT[indx1], na.rm = T)/5
# [1] 294.664

indx1 = indx1 & sum_dat2b$prof_percentile > 0.1 & sum_dat2b$prof_percentile < 99.9

mean(sum_dat2b$PROFIT[indx1], na.rm = T)/5
# [1] 732.4338
median(sum_dat2b$PROFIT[indx1], na.rm = T)/5
# [1] 294.6635

# max(dat2$NT_PROF, na.rm = T)
# min(dat2$NT_PROF, na.rm = T)

max(sum_dat2b$PROFIT[indx1], na.rm = T) #  90128.04
min(sum_dat2b$PROFIT[indx1], na.rm = T) # -5559.354

# dat2[dat2$NT_PROF == min(dat2$NT_PROF, na.rm=T) & !is.na(dat2$NT_PROF),]
# dat2[dat2$NT_PROF == max(dat2$NT_PROF, na.rm=T) & !is.na(dat2$NT_PROF),]

sum1 <- sum_dat2b[indx1, c('prof_decile', 'PROFIT')]%>%
  group_by(prof_decile) %>%
  summarise_at("PROFIT", funs(mean, min, max), na.rm=TRUE)
sum_dat2b[indx1, c('prof_decile', 'PROFIT')] %>%
  group_by(prof_decile) %>%
  summarise(n = n()) %>%
  full_join(sum1)

# Joining, by = "prof_decile"
# # A tibble: 10 x 5
#   prof_decile     n   mean    min    max
#         <dbl> <int>  <dbl>  <dbl>  <dbl>
# 1           1  2088 -1131. -5559.  -494.
# 2           2  2111  -145.  -494.   121.
# 3           3  2110   294.   121.   495.
# 4           4  2111   734.   495.   986.
# 5           5  2110  1250.   986.  1473.
# 6           6  2111  1722.  1473.  2082.
# 7           7  2111  2556.  2082.  3096.
# 8           8  2110  3951.  3096.  5051.
# 9           9  2111  7047.  5051.  9882.
# 10          10  2088 20471.  9886. 90128.

# ROI Gas

sum1 <- sum_dat2b[indx2, c('prof_decile', 'PROFIT')]%>%
  group_by(prof_decile) %>%
  summarise_at("PROFIT", funs(mean, min, max), na.rm=TRUE)
sum_dat2b[indx2, c('prof_decile', 'PROFIT')] %>%
  group_by(prof_decile) %>%
  summarise(n = n()) %>%
  full_join(sum1)

# Joining, by = "prof_decile"
# # A tibble: 10 x 5
#   prof_decile     n    mean    min     max
#         <dbl> <int>   <dbl>  <dbl>   <dbl>
# 1           1   186    99.2  -855.    285.
# 2           2   186   420.    286.    562.
# 3           3   187   709.    565.    854.
# 4           4   186  1040.    855.   1222.
# 5           5   187  1495.   1222.   1786.
# 6           6   186  2212.   1789.   2704.
# 7           7   187  3572.   2709.   4414.
# 8           8   186  5826.   4429.   7678.
# 9           9   187 10780.   7705.  15200.
# 10          10   187 48450.  15391. 635316. 

mean(sum_dat2b$PROFIT[indx2], na.rm = T)/5
# [1] 1495
median(sum_dat2b$PROFIT[indx2], na.rm = T)/5
# [1] 357.7019

indx2 = indx2 & !is.na(sum_dat2b$prof_percentile) & sum_dat2b$prof_percentile > 0.1 & sum_dat2b$prof_percentile < 99.9

mean(sum_dat2b$PROFIT[indx2], na.rm = T)/5
# [1]  1380.267
median(sum_dat2b$PROFIT[indx2], na.rm = T)/5
# [1]  357.4703


sum1 <- sum_dat2b[indx2, c('prof_decile', 'PROFIT')]%>%
  group_by(prof_decile) %>%
  summarise_at("PROFIT", funs(mean, min, max), na.rm=TRUE)
sum_dat2b[indx2, c('prof_decile', 'PROFIT')] %>%
  group_by(prof_decile) %>%
  summarise(n = n()) %>%
  full_join(sum1)

# Joining, by = "prof_decile"
# # A tibble: 10 x 5
#   prof_decile     n   mean    min     max
#         <dbl> <int>  <dbl>  <dbl>   <dbl>
# 1           1   185   104.  -516.    285.
# 2           2   186   420.   286.    562.
# 3           3   187   709.   565.    854.
# 4           4   186  1040.   855.   1222.
# 5           5   187  1495.  1222.   1786.
# 6           6   186  2212.  1789.   2704.
# 7           7   187  3572.  2709.   4414.
# 8           8   186  5826.  4429.   7678.
# 9           9   187 10780.  7705.  15200.
# 10          10   185 43074. 15391. 274974.

# NI Elec

sum1 <- sum_dat2b[indx3, c('prof_decile', 'PROFIT')]%>%
  group_by(prof_decile) %>%
  summarise_at("PROFIT", funs(mean, min, max), na.rm=TRUE)
sum_dat2b[indx3, c('prof_decile', 'PROFIT')] %>%
  group_by(prof_decile) %>%
  summarise(n = n()) %>%
  full_join(sum1)

# Joining, by = "prof_decile"
# # A tibble: 10 x 5
#   prof_decile     n    mean     min         max
#         <dbl> <int>   <dbl>   <dbl>       <dbl>
# 1           1  1038   -831. -9050.       -389. 
# 2           2  1038   -135.  -389.         98.6
# 3           3  1039    333.    99.9       579. 
# 4           4  1038    838.   580.       1121. 
# 5           5  1039   1438.  1121.       1740. 
# 6           6  1038   2129.  1741.       2589. 
# 7           7  1039   3225.  2589.       3962. 
# 8           8  1038   5184.  3963.       6743. 
# 9           9  1039   9291.  6745.      12824. 
# 10          10  1039 289775. 12835.  181211824.  

mean(sum_dat2b$PROFIT[indx3], na.rm = T)/5
# [1] 6227.807
median(sum_dat2b$PROFIT[indx3], na.rm = T)/5
# [1] 348.1506

indx3 = indx3 & !is.na(sum_dat2b$prof_percentile) & sum_dat2b$prof_percentile >0.1 & sum_dat2b$prof_percentile < 99.9

mean(sum_dat2b$PROFIT[indx3])/5
# [1] 1141.15
median(sum_dat2b$PROFIT[indx3])/5
# [1] 348.1116

sum1 <- sum_dat2b[indx3, c('prof_decile', 'PROFIT')]%>%
  group_by(prof_decile) %>%
  summarise_at("PROFIT", funs(mean, min, max), na.rm=TRUE)
sum_dat2b[indx3, c('prof_decile', 'PROFIT')] %>%
  group_by(prof_decile) %>%
  summarise(n = n()) %>%
  full_join(sum1)

# Joining, by = "prof_decile"
# # A tibble: 10 x 5
#   prof_decile     n   mean     min      max
#         <dbl> <int>  <dbl>   <dbl>    <dbl>
# 1           1  1028  -771. -5444.    -389. 
# 2           2  1038  -135.  -389.      98.6
# 3           3  1039   333.    99.9    579. 
# 4           4  1038   838.   580.    1121. 
# 5           5  1039  1438.  1121.    1740. 
# 6           6  1038  2129.  1741.    2589. 
# 7           7  1039  3225.  2589.    3962. 
# 8           8  1038  5184.  3963.    6743. 
# 9           9  1039  9291.  6745.   12824. 
# 10          10  1028 35761. 12835.  328874. 

indx = !is.na(dat2$prof_percentile) & dat2$prof_percentile > 0.1 & dat2$prof_percentile < 99.9

sum_table = aggregate(x = list(dat2$AVG_YEARLY_CONSUMPTION[indx],
                               dat2$REV_EST[indx],
                               dat2$ENRG_CST[indx],
                               dat2$GR_PROF[indx],
                               dat2$CTA[indx],
                               dat2$CTS[indx],
                               dat2$HW[indx],
                               dat2$WC2[indx],
                               dat2$NT_PROF[indx]),
                      by = list(dat2$CUSTOMER_ID[indx],
                                dat2$JURISDICTION[indx],
                                dat2$DF_SF[indx],
                                dat2$UTILITY_TYPE_CODE[indx]),
                      FUN = sum)
colnames(sum_table) = c('CUSTOMER_ID',
                        'JURISDICTION',
                        'DF_SF',
                        'UTILITY_TYPE_CODE',
                        'AVG_YEARLY_CONSUMPTION',
                        'REV_EST',
                        'ENRG_CST',
                        'GR_PROF',
                        'CTA',
                        'CTS',
                        'HW',
                        'WC2',
                        'NT_PROF')


agg_fun <- function(x, df = sum_table) { sum1 <- df %>%
  group_by(JURISDICTION, UTILITY_TYPE_CODE, DF_SF) %>%
  summarise_at(x, funs(mean, median, sum, CNT), na.rm= T)
return (sum1) }

con_agg <- agg_fun('AVG_YEARLY_CONSUMPTION')
rev_agg <- agg_fun('REV_EST')
energy_cst_agg <- agg_fun('ENRG_CST')
gr_prof_agg <- agg_fun('GR_PROF')
cta_agg <- agg_fun('CTA')
cts_agg <- agg_fun('CTS')
hw_agg <- agg_fun('HW')
wc_agg <- agg_fun('WC2')
nt_prof_agg <- agg_fun('NT_PROF')


tot_agg = rbind(con_agg,
                rev_agg,
                energy_cst_agg,
                gr_prof_agg,
                cta_agg,
                cts_agg,
                hw_agg,
                wc_agg,
                nt_prof_agg)

tot_agg <- cbind(tot_agg, 'type' = c(rep('Energy_Consumption', dim(con_agg)[1]),
                                     rep('Revenue', dim(rev_agg)[1]),
                                     rep('Eneregy_Cost', dim(energy_cst_agg)[1]),
                                     rep('Gross_Profit', dim(gr_prof_agg)[1]),
                                     rep('Cost_To_Acquire', dim(cta_agg)[1]),
                                     rep('Cost_To_Serve', dim(cts_agg)[1]),
                                     rep('Hardware_Cost', dim(hw_agg)[1]),
                                     rep('Welcome_Credit_Cost', dim(wc_agg)[1]),
                                     rep('Net_Profit', dim(nt_prof_agg)[1]) ) )

# write.csv(tot_agg, paste0('CVM_BE_UTIL_SUMMARY_', month_day, '.csv'), row.names = F)

# get more details of electric only usage
indx = (!is.na(dat2$prof_percentile) &
          dat2$prof_percentile > 0.1 &
          dat2$prof_percentile < 99.9 ) # this defines the filters

agg_fun2 <- function(x, ind = indx, df = dat2) {
  sum1 <- df [ind,] %>%
    group_by(JURISDICTION, UTILITY_TYPE_CODE, DF_SF) %>%
    summarise_at(x, funs(mean, median, sum, CNT), na.rm = T)
  return (sum1) }

con_agg <- agg_fun2('AVG_YEARLY_CONSUMPTION')
rev_agg <- agg_fun2('REV_EST')
energy_cst_agg <- agg_fun2('ENRG_CST')
gr_prof_agg <- agg_fun2('GR_PROF')
cta_agg <- agg_fun2('CTA')
cts_agg <- agg_fun2('CTS')
hw_agg <- agg_fun2('HW')
wc_agg <- agg_fun2('WC2')
nt_prof_agg <- agg_fun2('NT_PROF')


tot_agg = rbind(con_agg,
                rev_agg,
                energy_cst_agg,
                gr_prof_agg,
                cta_agg,
                cts_agg,
                hw_agg,
                wc_agg,
                nt_prof_agg)

tot_agg <- cbind(tot_agg, 'type' = c(rep('Energy_Consumption', dim(con_agg)[1]),
                                     rep('Revenue', dim(rev_agg)[1]),
                                     rep('Energy_Cost', dim(energy_cst_agg)[1]),
                                     rep('Gross_Profit', dim(gr_prof_agg)[1]),
                                     rep('Cost_To_Acquire', dim(cta_agg)[1]),
                                     rep('Cost_To_Serve', dim(cts_agg)[1]),
                                     rep('Hardware_Cost', dim(hw_agg)[1]),
                                     rep('Welcome_Credit_Cost', dim(wc_agg)[1]),
                                     rep('Net_Profit', dim(nt_prof_agg)[1]) ) )
print(tot_agg, n = 50)

##write.csv(tot_agg, paste0('CVM_BE_ELEC_SUMMARY_', month_day, '.csv'), row.names = F)

agg_fun3 <- function(x, ind = indx, df = dat2) {
  sum1 <- df [ind,] %>%
    group_by(JURISDICTION, CATGRY, UTILITY_TYPE_CODE, DF_SF) %>%
    summarise_at(x, funs(mean, median, sum, CNT), na.rm = T)
  return (sum1) }

con_agg2 <- agg_fun3('AVG_YEARLY_CONSUMPTION')
rev_agg2 <- agg_fun3('REV_EST')
energy_cst_agg2 <- agg_fun3('ENRG_CST')
gr_prof_agg2 <- agg_fun3('GR_PROF')
cta_agg2 <- agg_fun3('CTA')
cts_agg2 <- agg_fun3('CTS')
hw_agg2 <- agg_fun3('HW')
wc_agg2 <- agg_fun3('WC2')
nt_prof_agg2 <- agg_fun3('NT_PROF')


tot_agg2 = rbind(con_agg2,
                 rev_agg2,
                 energy_cst_agg2,
                 gr_prof_agg2,
                 cta_agg2,
                 cts_agg2,
                 hw_agg2,
                 wc_agg2,
                 nt_prof_agg2)

tot_agg2 <- cbind(tot_agg2, 'type' = c(rep('Energy_Consumption', dim(con_agg2)[1]),
                                       rep('Revenue', dim(rev_agg2)[1]),
                                       rep('Energy_Cost', dim(energy_cst_agg2)[1]),
                                       rep('Gross_Profit', dim(gr_prof_agg2)[1]),
                                       rep('Cost_To_Acquire', dim(cta_agg2)[1]),
                                       rep('Cost_To_Serve', dim(cts_agg2)[1]),
                                       rep('Hardware_Cost', dim(hw_agg2)[1]),
                                       rep('Welcome_Credit_Cost', dim(wc_agg2)[1]),
                                       rep('Net_Profit', dim(nt_prof_agg2)[1]) ) )
print(tot_agg2, n = 108)

###write.csv(tot_agg2, paste0('CVM_BE_CATGRY_SUMMARY_', month_day, '.csv'), row.names = F)

hist_plot = function(x, y) {
  indx2 =  !is.na(dat2$EAC) & !is.na(dat2$AVG_YEARLY_CONSUMPTION) & dat2$EAC != 0 &  is.finite(dat2$AVG_YEARLY_CONSUMPTION/dat2$EAC) &
    dat2$AVG_YEARLY_CONSUMPTION != dat2$EAC & dat2$AVG_YEARLY_CONSUMPTION/dat2$EAC <3 & dat2$AVG_YEARLY_CONSUMPTION/dat2$EAC > 0 &
    dat2$UTILITY_TYPE_CODE == x & dat2$JURISDICTION == y
  if (sum(indx2) > 0) {
    hist(dat2$AVG_YEARLY_CONSUMPTION[indx2]/dat2$EAC[indx2],
         xlim = c(0,3),
         breaks = 200,
         col = 3,
         xlab = 'Ratio of Actual Annual Usage to EAC',
         main = paste0('Histogram of Actual to Estimated Usage ', x, " ", y))
    return(list(sum(indx2), mean(dat2$AVG_YEARLY_CONSUMPTION[indx2]/dat2$EAC[indx2], na.rm = T)))
  }
  else {return ('error: no data')}
}

hist_plot2 = function(x, y, z) {
  indx2 = !is.na(dat2$EAC) & !is.na(dat2$AVG_YEARLY_CONSUMPTION) & dat2$EAC != 0 &  is.finite(dat2$AVG_YEARLY_CONSUMPTION/dat2$EAC) &
    dat2$AVG_YEARLY_CONSUMPTION != dat2$EAC & dat2$AVG_YEARLY_CONSUMPTION/dat2$EAC <3 & dat2$AVG_YEARLY_CONSUMPTION/dat2$EAC > 0 &
    dat2$UTILITY_TYPE_CODE == x & dat2$JURISDICTION == y & dat2$PAYMENT_CHANNEL_CATEGORY == z
  if (sum(indx2) > 0) {
    hist(dat2$AVG_YEARLY_CONSUMPTION[indx2]/dat2$EAC[indx2],
         xlim = c(0,3),
         breaks = 200,
         col = 3,
         xlab = 'Ratio of Actual Annual Usage to EAC',
         main = paste0('Histogram of Actual to Estimated Usage ', x, " ", y, " ", z))
    return(list(sum(indx2), mean(dat2$AVG_YEARLY_CONSUMPTION[indx2]/dat2$EAC[indx2], na.rm = T)))
  }
  else {return ('error: no data')}
}

hist_plot('E', 'ROI')
# [1] "error: no data"

hist_plot('E','NI')
# [[1]]
# [1] 5928
# 
# [[2]]
# [1] 1.241195

hist_plot2('E','NI','Credit')
# [[1]]
# [1] 5927
# 
# [[2]]
# [1] 1.241367

hist_plot2('E','NI','Prepay')
# [[1]]
# [1] 1
# 
# [[2]]
# [1] 0.2212713

hist_plot('G','ROI')
# [[1]]
# [1] 1343
# 
# [[2]]
# [1] 1.264741


hist(dat2$NT_PROF[dat2$NT_PROF > -500 & dat2$NT_PROF<1500],
     xlab = 'Profitability Estimate (local currency)',
     main = '5 yr profitability',
     xlim = c(-0.5e3,1.5e3),
     col = as.integer(as.factor(dat2$JURISDICTION)),
     breaks = 200)


indx = (!is.na(dat2$prof_percentile) &
          dat2$prof_percentile > 0.1 &
          dat2$prof_percentile < 99.9 )
dat3 <- dat2[indx, c( which (colnames(dat2) == 'JURISDICTION'),
                      which (colnames(dat2) == 'UTILITY_TYPE_CODE'),
                      which (colnames(dat2) == 'DF_SF'),
                      which (colnames(dat2) == 'ACQ_CHANNEL'),
                      which (colnames(dat2) == 'NT_PROF') ) ]
mu <- dat3 %>%
  group_by(JURISDICTION, UTILITY_TYPE_CODE) %>%
  summarise_at('NT_PROF', funs(grp.mean = mean, grp.median = median), na.rm = T)
mu

# A tibble: 3 x 4
#   Groups:   JURISDICTION [2]
#   JURISDICTION UTILITY_TYPE_CODE grp.mean grp.median
#   <chr>        <chr>                <dbl>      <dbl>
# 1 NI           E                    4152.      1258.
# 2 ROI          E                    2528.       900.
# 3 ROI          G                    6898.      1786.

a <- ggplot(na.omit(dat3), aes(x = NT_PROF))

# a + geom_area(stat = "bin",  fill = "#00AFBB", binwidth = 5) +
#   coord_cartesian(xlim = c(-500, 1000), ylim = c(0,2000))

a2 <- a +
  geom_histogram(aes(fill = JURISDICTION), alpha = 0.8, binwidth = 20) +
  geom_vline(data = mu, aes(xintercept = grp.mean, color = JURISDICTION), linetype = "dashed") +
  theme_minimal()

a2 +
  scale_color_manual(values = c("#999999", "#E69F00")) +
  coord_cartesian(xlim = c(-500, 2000), ylim = c(0,2000))

# Density plot by Jurisdiction
a2 <- a +
  geom_density(aes(fill = JURISDICTION), alpha = 0.8, bw='nrd0') +
  geom_vline(data = mu, aes(xintercept = grp.mean, color = JURISDICTION),
             linetype = "dashed") +
  theme_minimal()

a2 +
  scale_color_manual(values = c("#999999", "#E69F00")) +
  coord_cartesian(xlim = c(-500, 2000), ylim = c(0,0.003))

# Density plot by SF/DF

mu <- dat3 %>%
  group_by(DF_SF) %>%
  summarise_at('NT_PROF', funs(grp.mean = mean, grp.median = median), na.rm = T)
mu

# A tibble: 2 x 3
#   DF_SF grp.mean grp.median
#   <fct>    <dbl>      <dbl>
# 1 DF       3627.      1326.
# 2 SF       3196.      1013.

a <- ggplot(na.omit(dat3), aes(x = NT_PROF))

a2 <- a +
  geom_density(aes(fill = DF_SF), alpha = 0.8) +
  geom_vline(data = mu, aes(xintercept = grp.mean, color = DF_SF), linetype = "dashed") +
  theme_minimal()

a2 +
  scale_color_manual(values = c("#999999", "#E69F00")) +
  coord_cartesian(xlim = c(-500, 2000), ylim = c(0,0.001))

a2 <- a +
  geom_histogram(aes(fill = DF_SF), alpha = 0.8, binwidth = 20) +
  geom_vline(data = mu, aes(xintercept = grp.mean, color = DF_SF), linetype = "dashed") +
  theme_minimal()

a2 +
  scale_color_manual(values = c("#999999", "#E69F00")) +
  coord_cartesian(xlim = c(-500, 2000), ylim = c(0,1000))

mu <- na.omit(dat3[dat3$JURISDICTION == 'ROI',]) %>%
  group_by(DF_SF) %>%
  summarise_at('NT_PROF', funs(grp.mean = mean, grp.median = median), na.rm = T)
mu
# A tibble: 2 x 3
#   DF_SF grp.mean grp.median
#   <fct>    <dbl>      <dbl>
# 1 DF       3627.      1326.
# 2 SF       2731.       925.

mu <- na.omit(dat3[dat3$JURISDICTION == 'NI',]) %>%
  group_by(DF_SF) %>%
  summarise_at('NT_PROF', funs(grp.mean = mean, grp.median = median), na.rm = T)
mu
# A tibble: 1 x 3
#     DF_SF grp.mean grp.median
#     <fct>    <dbl>      <dbl>
#   1 SF       4152.      1258.


indx = (!is.na(dat2$prof_percentile) &
          dat2$prof_percentile > 0.1 &
          dat2$prof_percentile < 99.9 )

sum_table2 = aggregate(dat2$NT_PROF[indx],
                       list(dat2$CUSTOMER_ID[indx],
                            dat2$MPRN[indx],
                            dat2$JURISDICTION[indx],
                            dat2$DF_SF[indx],
                            dat2$ACQ_CHANNEL[indx],
                            dat2$UTILITY_TYPE_CODE[indx]),
                       FUN = sum)
colnames(sum_table2) = c('CUSTOMER_ID', 'MPRN', 'JURISDICTION',  'DF_SF',  'ACQ_CHANNEL', 'UTILITY_TYPE_CODE', 'NT_PROF')

sum_table4 = aggregate(dat2$TENURE_MTHS,
                       list(dat2$ACQ_CHANNEL,
                            dat2$JURISDICTION),
                       FUN = mean, na.action = F, na.rm = T)
colnames(sum_table4) = c('ACQ_CHANNEL', 'JURISDICTION', 'AVG_TENURE')
sum_table4
#        ACQ_CHANNEL JURISDICTION AVG_TENURE
# 1         Anything           NI  39.930163
# 2   Commercial TPI           NI  53.248367
# 3              DOM           NI  85.808260
# 4     Door to Door           NI  77.797521
# 5         Employee           NI  79.685686
# 6        Homemoves           NI  61.006757
# 7           Online           NI  68.296296
# 8            Other           NI 119.766584
# 9           Rigney           NI  55.319811
# 10             SME           NI   8.010390
# 11   SME Telesales           NI  11.261993
# 12         Unknown           NI  68.732468
# 13        Anything          ROI  37.234142
# 14  Commercial TPI          ROI  41.811671
# 15             DOM          ROI  58.250183
# 16    Door to Door          ROI  40.857407
# 17        Employee          ROI  71.733333
# 18       Homemoves          ROI  67.334821
# 19          Online          ROI  69.652000
# 20           Other          ROI 110.731614
# 21 Property Button          ROI   6.115385
# 22          Rigney          ROI  54.181044
# 23             SME          ROI  16.596306
# 24   SME Telesales          ROI  13.396904
# 25         Unknown          ROI  82.789062

sum_table5 = aggregate(dat2$TENURE_MTHS,
                       list(dat2$ACQ_CHANNEL,
                            dat2$JURISDICTION),
                       FUN = CNT)
colnames(sum_table5) = c('ACQ_CHANNEL', 'JURISDICTION', 'COUNT')
sum_table5
#        ACQ_CHANNEL JURISDICTION COUNT
# 1         Anything           NI   673
# 2   Commercial TPI           NI  8113
# 3              DOM           NI   339
# 4     Door to Door           NI   242
# 5         Employee           NI  2033
# 6        Homemoves           NI   148
# 7           Online           NI    27
# 8            Other           NI  2005
# 9           Rigney           NI  1060
# 10             SME           NI   385
# 11   SME Telesales           NI   542
# 12         Unknown           NI   385
# 13        Anything          ROI  1072
# 14  Commercial TPI          ROI 11379
# 15             DOM          ROI  2726
# 16    Door to Door          ROI  4327
# 17        Employee          ROI  1650
# 18       Homemoves          ROI   224
# 19          Online          ROI   250
# 20           Other          ROI  3916
# 21 Property Button          ROI    52
# 22          Rigney          ROI  3524
# 23             SME          ROI   379
# 24   SME Telesales          ROI  1421
# 25         Unknown          ROI  1408

# sum_table2$percentile = as.integer(ecdf(sum_table2$NT_PROF)(sum_table2$NT_PROF)*100,0)
sum_table2 = aggregate(list(dat2$NT_PROF,
                            dat2$REV_EST),
                       by = list(dat2$CUSTOMER_ID,
                                 dat2$MPRN, 
                                 dat2$JURISDICTION),
                       FUN = sum)

# sum_table100 = aggregate(dat2$NT_PROF,
#                        by = list(dat2$CUSTOMER_ID,
#                                  dat2$MPRN,
#                                  dat2$JURISDICTION),
#                        FUN = sum)
# library(mltools)
# clv_table$P_WEIGHT <- bin_data(clv_table$PERCENTAGE_PROFITABILITY, bins=800, binType = "quantile")
# CLV_WEIGHT_TABLE <- clv_table %>%
#   count(P_WEIGHT) %>%
#   rename(COUNT_MPRN = n)

colnames(sum_table2) = c('CUSTOMER_ID', 'MPRN', 'JURISDICTION', 'NT_PROF', 'REV_EST')
# initiate
sum_table2$percentile = 0
sum_table2$decile = 0
# ROI percentiles and deciles
indx <- sum_table2$JURISDICTION == 'ROI'
sum_table2$percentile[indx] = as.integer(ecdf(sum_table2$NT_PROF[indx])(sum_table2$NT_PROF[indx])*100)
sum_table2$decile[indx] = (ecdf(sum_table2$NT_PROF[indx])(sum_table2$NT_PROF[indx])*1000)%/%100

# NI percentiles and deciles
indx <- sum_table2$JURISDICTION == 'NI'
sum_table2$percentile[indx] = as.integer(ecdf(sum_table2$NT_PROF[indx])(sum_table2$NT_PROF[indx])*100)
sum_table2$decile[indx] = (ecdf(sum_table2$NT_PROF[indx])(sum_table2$NT_PROF[indx])*1000)%/%100

sum_table2$percentile [sum_table2$percentile == 100] = 99
sum_table2$percentile = sum_table2$percentile + 1
sum_table2$decile [sum_table2$decile == 10] = 9
sum_table2$decile = sum_table2$decile + 1

table(sum_table2$percentile , sum_table2$JURISDICTION, useNA = 'ifany' )
table(sum_table2$decile , sum_table2$JURISDICTION, useNA = 'ifany' )

#        NI  ROI
# 1     882 1730
# 2     883 1730
# 3     883 1730
# 4     883 1731
# 5     883 1730
# 6     883 1730
# 7     883 1731
# 8     883 1730
# 9     883 1730
# 10    883 1731
# <NA>  267    1

n = dim(sum_table2) [1]
insert_date = paste0(format(Sys.time(), "%d"), '-', format(Sys.time(), "%b"), '-', format(Sys.time(), "%y"))
clv_table = cbind.data.frame(sum_table2[,c(which(names(sum_table2) == 'CUSTOMER_ID'),
                                           which(names(sum_table2) == 'MPRN'),
                                           which(names(sum_table2) == 'NT_PROF'),
                                           which(names(sum_table2) == 'REV_EST'))],
                             rep(Sys.Date(),n),
                             rep(1,n),
                             sum_table2[,c(which(names(sum_table2) == 'decile'),
                                           which(names(sum_table2) == 'percentile'))])

colnames(clv_table) = c('CUSTOMERID', 'MPRN', 'CLVSCORE', 'REV_EST', 'INSERTEDDATE', 'CURRENTSCORE', 'DECILE', 'PERCENTILE')
clv_table$CLVSCORE = round(clv_table$CLVSCORE,2)
# clv_table$INSERTEDDATE = as.POSIXct(clv_table$INSERTEDDATE, format="%d/%m/%y")
clv_table = na.omit(clv_table)

table(clv_table$PERCENTILE, useNA = 'ifany' )
# 1   2   3   4   5   6   7   8   9  10  11  12  13  14  15  16  17  18  19  20  21  22  23  24  25  26  27  28  29  30  31 
# 261 261 261 262 261 261 262 261 261 261 262 261 261 262 261 261 261 262 261 261 262 261 261 261 262 261 261 262 261 261 261 
# 32  33  34  35  36  37  38  39  40  41  42  43  44  45  46  47  48  49  50  51  52  53  54  55  56  57  58  59  60  61  62 
# 262 261 262 262 261 261 262 261 261 261 262 261 261 259 264 261 261 262 261 261 262 261 261 261 262 261 261 262 261 261 261 
# 63  64  65  66  67  68  69  70  71  72  73  74  75  76  77  78  79  80  81  82  83  84  85  86  87  88  89  90  91  92  93 
# 262 261 261 262 262 261 262 261 261 261 262 261 261 262 261 261 261 262 261 261 262 261 261 261 262 261 261 262 261 261 261 
# 94  95  96  97  98  99 100 
# 262 261 261 262 261 261 263

table(clv_table$DECILE, useNA = 'ifany' )
#    1    2    3    4    5    6    7    8    9   10 
# 2612 2613 2613 2614 2613 2613 2614 2613 2613 2614

head(clv_table)
#   CUSTOMERID CLVSCORE INSERTEDDATE CURRENTSCORE DECILE PERCENTILE
# 1     169019  -786.63   2020-05-22            1      1          4
# 2     169175 16676.74   2020-05-22            1     10         92
# 3     169365 15321.79   2020-05-22            1     10         92
# 4     169368 12695.21   2020-05-22            1      9         90
# 5     169541 19307.71   2020-05-22            1     10         94
# 6     169574  3111.75   2020-05-22            1      7         65

str(clv_table)

clv_table[clv_table$CLVSCORE == min(clv_table$CLVSCORE),]
#       CUSTOMERID CLVSCORE INSERTEDDATE CURRENTSCORE DECILE PERCENTILE
# 23138    2387450 -7517340   2020-05-22            1      1          1

clv_table[clv_table$CLVSCORE == max(clv_table$CLVSCORE),]
#         CUSTOMERID  CLVSCORE INSERTEDDATE CURRENTSCORE DECILE PERCENTILE
# 6606    2203995 181211824   2020-05-22            1     10        100

# Calculate Percentage Profitability i.e. Revenue_Estimate as a % of CLV score on MPRN level
clv_table$PERCENTAGE_PROFITABILITY <- round(((clv_table$CLVSCORE/clv_table$REV_EST)*100), 2)

# clv_table_new <- sqldf("SELECT *
#              ,dense_rank() over (partition by AA.CUSTOMERID order by AA.MPRN desc) + dense_rank() over (partition by AA.CUSTOMERID order by AA.MPRN asc) - 1 as num_all_mprns
#              FROM clv_table AA
#              ")

setwd("S:/Retail/IRL/Supply/Marketing/MARKETING SUPPLY/Analytics & Products/Analytics/MM WIP/BE Value Model")
month_day = paste0(format(Sys.time(), "%b"), format(Sys.time(), "%d"))
### write.csv(clv_table, paste0("BE_CLV_",month_day,".csv"), row.names = F)
dat_value_model = clv_table

######################### summary output at meter level #########################################


sum_table3 = aggregate(list(dat2$AVG_YEARLY_CONSUMPTION, 
                            dat2$REV_EST, dat2$ENRG_CST, 
                            dat2$GR_PROF, 
                            dat2$CTA, 
                            dat2$CTS, 
                            dat2$NT_PROF), 
                       list(dat2$MPRN,
                            dat2$BE_SEGMENT,
                            dat2$CATGRY,
                            dat2$JURISDICTION),
                       FUN = sum) 
colnames(sum_table3) = c('MPRN', 'BE_SEGMENT', 'CATEGORY', 'JURISDICTION', 'ANNUAL_CONSUMPTION', 'REVENUE_ESTIMATE', 'ENERGY_COST', 'GROSS_PROFIT', 'CTA', 'CTS', 'NET_PROFIT')
table(sum_table3$CTA)

# initiate
sum_table3$prof_percentile = 0
sum_table3$percentile = 0
sum_table3$decile = 0

# ROI percentiles and deciles
indx <- sum_table3$JURISDICTION == 'ROI'
sum_table3$prof_percentile[indx] = round(ecdf(sum_table3$NET_PROFIT[indx])(sum_table3$NET_PROFIT[indx])*100,2)
sum_table3$percentile[indx] = as.integer(ecdf(sum_table3$NET_PROFIT[indx])(sum_table3$NET_PROFIT[indx])*100)
sum_table3$decile[indx] = (ecdf(sum_table3$NET_PROFIT[indx])(sum_table3$NET_PROFIT[indx])*1000)%/%100

# NI percentiles and deciles
indx <- sum_table3$JURISDICTION == 'NI'
sum_table3$prof_percentile[indx] = round(ecdf(sum_table3$NET_PROFIT[indx])(sum_table3$NET_PROFIT[indx])*100, 2)
sum_table3$percentile[indx] = as.integer(ecdf(sum_table3$NET_PROFIT[indx])(sum_table3$NET_PROFIT[indx])*100)
sum_table3$decile[indx] = (ecdf(sum_table3$NET_PROFIT[indx])(sum_table3$NET_PROFIT[indx])*1000)%/%100

sum_table3$percentile [sum_table3$percentile == 100] = 99
sum_table3$percentile = sum_table3$percentile + 1
sum_table3$decile [sum_table3$decile == 10] = 9
sum_table3$decile = sum_table3$decile + 1

indx = (!is.na(sum_table3$prof_percentile) &
          sum_table3$prof_percentile > 0.1 &
          sum_table3$prof_percentile < 99.9 )

table(sum_table3$percentile, sum_table3$JURISDICTION, useNA = 'ifany' )
table(sum_table3$decile, sum_table3$JURISDICTION, useNA = 'ifany' )

table(sum_table3$percentile[indx], sum_table3$JURISDICTION[indx], useNA = 'ifany' )
table(sum_table3$decile[indx], sum_table3$JURISDICTION[indx], useNA = 'ifany')

agg_fun4 <- function(x, ind = indx, df = sum_table3) {
  sum1 <- df [ind,] %>%
    group_by(JURISDICTION, CATEGORY) %>%
    summarise_at(x, funs(mean, median, sum, CNT), na.rm = T)
  return (sum1) }

con_agg3 <- agg_fun4('ANNUAL_CONSUMPTION')
rev_agg3 <- agg_fun4('REVENUE_ESTIMATE')
energy_cst_agg3 <- agg_fun4('ENERGY_COST')
gr_prof_agg3 <- agg_fun4('GROSS_PROFIT')
cta_agg3 <- agg_fun4('CTA')
cts_agg3 <- agg_fun4('CTS')
nt_prof_agg3 <- agg_fun4('NET_PROFIT')


tot_agg3 = rbind(con_agg3,
                 rev_agg3,
                 energy_cst_agg3,
                 gr_prof_agg3,
                 cta_agg3,
                 cts_agg3,
                 nt_prof_agg3)

tot_agg3 <- cbind(tot_agg3, 'type' = c(rep('Energy_Consumption', dim(con_agg3)[1]),
                                       rep('Revenue', dim(rev_agg3)[1]),
                                       rep('Energy_Cost', dim(energy_cst_agg3)[1]),
                                       rep('Gross_Profit', dim(gr_prof_agg3)[1]),
                                       rep('Cost_To_Acquire', dim(cta_agg3)[1]),
                                       rep('Cost_To_Serve', dim(cts_agg3)[1]),
                                       rep('Net_Profit', dim(nt_prof_agg3)[1]) ) )
print(tot_agg3, n = 108)



setwd("S:/Retail/IRL/Supply/Marketing/MARKETING SUPPLY/Analytics & Products/Analytics/MM WIP/BE Value Model")
month_day = paste0(format(Sys.time(), "%b"), format(Sys.time(), "%d"))
### write.csv(tot_agg3, paste0("BE_CLV_METER_SUMMARY_",month_day,".csv"), row.names = F)


agg_fun5 <- function(x, ind = indx, df = sum_table3) {
  sum1 <- df [ind,] %>%
    group_by(JURISDICTION, CATEGORY, decile) %>%
    summarise_at(x, funs(mean, median, sum, CNT), na.rm = T)
  return (sum1) }

con_agg4 <- agg_fun5('ANNUAL_CONSUMPTION')
rev_agg4 <- agg_fun5('REVENUE_ESTIMATE')
energy_cst_agg4 <- agg_fun5('ENERGY_COST')
gr_prof_agg4 <- agg_fun5('GROSS_PROFIT')
cta_agg4 <- agg_fun5('CTA')
cts_agg4 <- agg_fun5('CTS')
nt_prof_agg4 <- agg_fun5('NET_PROFIT')


tot_agg4 = rbind(con_agg4,
                 rev_agg4,
                 energy_cst_agg4,
                 gr_prof_agg4,
                 cta_agg4,
                 cts_agg4,
                 nt_prof_agg4)

tot_agg4 <- cbind(tot_agg4, 'type' = c(rep('Energy_Consumption', dim(con_agg4)[1]),
                                       rep('Revenue', dim(rev_agg4)[1]),
                                       rep('Energy_Cost', dim(energy_cst_agg4)[1]),
                                       rep('Gross_Profit', dim(gr_prof_agg4)[1]),
                                       rep('Cost_To_Acquire', dim(cta_agg4)[1]),
                                       rep('Cost_To_Serve', dim(cts_agg4)[1]),
                                       rep('Net_Profit', dim(nt_prof_agg4)[1]) ) )
print(tot_agg4, n = 600)

setwd("S:/Retail/IRL/Supply/Marketing/MARKETING SUPPLY/Analytics & Products/Analytics/MM WIP/BE Value Model")
month_day = paste0(format(Sys.time(), "%b"), format(Sys.time(), "%d"))
### write.csv(tot_agg4, paste0("BE_CLV_METER_DECILE_SUMMARY_",month_day,".csv"), row.names = F)

# details by decile

agg_fun6 <- function(x, ind = indx, df = sum_table3) {
  sum1 <- df [ind,] %>%
    group_by(JURISDICTION, BE_SEGMENT, CATEGORY, decile) %>%
    summarise_at(x, funs(mean, median, sum, CNT), na.rm = T)
  return (sum1) }

con_agg5 <- agg_fun6('ANNUAL_CONSUMPTION')
rev_agg5 <- agg_fun6('REVENUE_ESTIMATE')
energy_cst_agg5 <- agg_fun6('ENERGY_COST')
gr_prof_agg5 <- agg_fun6('GROSS_PROFIT')
cta_agg5 <- agg_fun6('CTA')
cts_agg5 <- agg_fun6('CTS')
nt_prof_agg5 <- agg_fun6('NET_PROFIT')


tot_agg5 = rbind(con_agg5,
                 rev_agg5,
                 energy_cst_agg5,
                 gr_prof_agg5,
                 cta_agg5,
                 cts_agg5,
                 nt_prof_agg5)

tot_agg5 <- cbind(tot_agg5, 'type' = c(rep('Energy_Consumption', dim(con_agg5)[1]),
                                       rep('Revenue', dim(rev_agg5)[1]),
                                       rep('Energy_Cost', dim(energy_cst_agg5)[1]),
                                       rep('Gross_Profit', dim(gr_prof_agg5)[1]),
                                       rep('Cost_To_Acquire', dim(cta_agg5)[1]),
                                       rep('Cost_To_Serve', dim(cts_agg5)[1]),
                                       rep('Net_Profit', dim(nt_prof_agg5)[1]) ) )
print(tot_agg5, n = 1100)

setwd("S:/Retail/IRL/Supply/Marketing/MARKETING SUPPLY/Analytics & Products/Analytics/MM WIP/BE Value Model")
month_day = paste0(format(Sys.time(), "%b"), format(Sys.time(), "%d"))
### write.csv(tot_agg5, paste0("BE_CLV_METER_DECILE_SUMMARY_V2_",month_day,".csv"), row.names = F)


plot(dat2$EAC, dat2$AVG_YEARLY_CONSUMPTION, 
     xlim = c(0,1e6), 
     ylim = c(0,1e6),
     xlab = 'EAC',
     ylab = 'ACTUAL',
     main = 'Actual Usage vs EAC')

plot(dat2$EAC, dat2$AVG_YEARLY_CONSUMPTION, 
     xlim = c(0,1e5), 
     ylim = c(0,1e5),
     xlab = 'EAC',
     ylab = 'ACTUAL',
     main = 'Actual Usage vs EAC')


# investigate data clustered around 2*EAC
dat3 <- dat2[is.finite(dat2$AVG_YEARLY_CONSUMPTION/dat2$EAC) & dat2$AVG_YEARLY_CONSUMPTION/dat2$EAC >= 1.8 & dat2$AVG_YEARLY_CONSUMPTION/dat2$EAC <= 2.2, ]

plot(dat3$EAC, dat3$AVG_YEARLY_CONSUMPTION, 
     xlim = c(0,1e5), 
     ylim = c(0,1e5),
     xlab = 'EAC',
     ylab = 'ACTUAL',
     main = 'Actual Usage vs EAC')

table(dat3$JURISDICTION, dat3$REGISTER_DESC)

#     24hr Day GAS_24HR Heating Low Night Normal Off Peak
# NI    52  45        0      16  16    48     25        2
# ROI    0   0       37       0   0     0      0        0


# dat2[dat2$Customer_ID == 2429186 & dat2$PREMISE_ID ==	1333788, ]
dat3 <- dat2[dat2$CATGRY =='GPNS',]

dat2[dat2$MPRN == '10000045424', ]

dat2$GR_PROF_PCT = dat2$GR_PROF/dat2$REV_EST

dat2$GR_PROF_PCT_BKT = (dat2$GR_PROF_PCT * 100) %/% 10 / 10

dat2$GR_PROF_PCT_BKT <- ifelse( dat2$GR_PROF_PCT_BKT < 0, 0, ifelse( dat2$GR_PROF_PCT_BKT > 1, 1, dat2$GR_PROF_PCT_BKT))

table(dat2$GR_PROF_PCT_BKT[dat2$CATGRY %in% c('CBC', 'GP', 'GPNS', 'Weekender', 'Nightsaver', 'Popular')], dat2$CATGRY [dat2$CATGRY %in% c('CBC', 'GP', 'GPNS', 'Weekender', 'Nightsaver', 'Popular')], useNA = 'ifany')

head(dat2[dat2$GR_PROF_PCT > 0.3 & dat2$AVG_YEARLY_CONSUMPTION > 10000 & !is.na(dat2$GR_PROF_PCT_BKT) & dat2$REGISTER_DESC == '24hr' & dat2$JURISDICTION == 'ROI',] )

t(as.data.frame(dat2[dat2$MPRN == '10303739373', ]))

head(dat2[dat2$GR_PROF_PCT < 0.2 & dat2$AVG_YEARLY_CONSUMPTION > 10000 & !is.na(dat2$GR_PROF_PCT_BKT) & dat2$REGISTER_DESC == '24hr' & dat2$JURISDICTION == 'ROI',] )

t(as.data.frame(dat2[dat2$MPRN == '10303739373', ]))


indx = !is.na(dat2$AVG_YEARLY_CONSUMPTION)
brks = c(-Inf,0,500,1000,2000,4000,8000,12000,20000,Inf)
labs = c('<=0 MWh','0-0.5MWh','0.5-1MWh','1-2MWh','2-4MWh','4-8MWh','8-12MWh','12-20MWh','>20MWh')
dat2$CONS_BKT <- cut(dat2$AVG_YEARLY_CONSUMPTION, breaks = brks, labels = labs)

indx = !is.na(dat2$GR_PROF_PCT_BKT) & !is.na(dat2$CONS_BKT) & !is.na(dat2$REGISTER_DESC) & dat2$CATGRY %in% c('CBC', 'GP', 'GPNS', 'Weekender', 'Nightsaver', 'Popular')
table(dat2$GR_PROF_PCT_BKT[indx & dat2$JURISDICTION == 'ROI' & dat2$REGISTER_DESC == '24hr'], dat2$CONS_BKT[indx & dat2$JURISDICTION == 'ROI' & dat2$REGISTER_DESC == '24hr'], useNA = 'ifany')
#     <=0 MWh 0-0.5MWh 0.5-1MWh 1-2MWh 2-4MWh 4-8MWh 8-12MWh 12-20MWh >20MWh
# 0.1       0        0        0      0      0      0       0        0      1
# 0.2       0        0        0      0      0      0       0       41   1470
# 0.3       0        0        0      0      1   3900    1488     1544    833
# 0.4       0        0        0      0    988   1869       0        0      0
# 0.5       0        0        0    422    677      0       0        0      0
# 0.6       0        0      122    599      0      0       0        0      0
# 0.7       0       29      512      0      0      0       0        0      0
# 0.8       0      471        0      0      0      0       0        0      0
# 0.9       0      516        0      0      0      0       0        0      0
# 1       465        0        0      0      0      0       0        0      0

table(dat2$GR_PROF_PCT_BKT[indx & dat2$JURISDICTION == 'ROI' & dat2$REGISTER_DESC == 'Day'], dat2$CONS_BKT[indx & dat2$JURISDICTION == 'ROI' & dat2$REGISTER_DESC == 'Day'], useNA = 'ifany')
#     <=0 MWh 0-0.5MWh 0.5-1MWh 1-2MWh 2-4MWh 4-8MWh 8-12MWh 12-20MWh >20MWh
# 0.2       0        0        0      0      0      1       2      105   1331
# 0.3       0        0        0      0      0    809    1573      433     98
# 0.4       0        0        0      0    312    277       3        0      0
# 0.5       0        0        0    120    161      0       0        0      0
# 0.6       0        0       37    152      0      0       0        0      0
# 0.7       0       11      138      0      0      0       0        0      0
# 0.8       0      138        0      0      0      0       0        0      0
# 0.9       0      104        0      0      0      0       0        0      0
# 1       107        0        0      0      0      0       0        0      0

table(dat2$GR_PROF_PCT_BKT[indx & dat2$JURISDICTION == 'ROI' & dat2$REGISTER_DESC == 'Night'], dat2$CONS_BKT[indx & dat2$JURISDICTION == 'ROI' & dat2$REGISTER_DESC == 'Night'], useNA = 'ifany')
#     <=0 MWh 0-0.5MWh 0.5-1MWh 1-2MWh 2-4MWh 4-8MWh 8-12MWh 12-20MWh >20MWh
# 0         0        0        0      0      0      1       0        0      0
# 0.1       0       39       35     37     68    138      48       59     72
# 0.2       0      407      218    356    538   2128     464      522    665

table(dat2$GR_PROF_PCT_BKT[indx & dat2$JURISDICTION == 'NI' & dat2$REGISTER_DESC == '24hr'], dat2$CONS_BKT[indx & dat2$JURISDICTION == 'NI' & dat2$REGISTER_DESC == '24hr'], useNA = 'ifany')
#     <=0 MWh 0-0.5MWh 0.5-1MWh 1-2MWh 2-4MWh 4-8MWh 8-12MWh 12-20MWh >20MWh
# 0         0        0        0      0      0      0       1        0      0
# 0.1       0        0        0      0      0      5      26       19     57
# 0.2       0        0        4     38    612   1553    1502     1205   1861
# 0.3       0        5       81    451    381     94       2        2      5
# 0.4       0       31      173      3      0      2       2        2     18
# 0.5       0      125        1      1      0      4       1        7     15
# 0.6       0       63        1      0      0      3       2        2     17
# 0.7       1       55        0      0      0      0       1        2      5
# 0.8       0       42        0      0      0      2       1        3     10
# 0.9       1       58        0      0      0      3       2        3     12
# 1        52        1        4      2      4     16      22       26     78

table(dat2$GR_PROF_PCT_BKT[indx & dat2$JURISDICTION == 'NI' & dat2$REGISTER_DESC == 'Day'], dat2$CONS_BKT[indx & dat2$JURISDICTION == 'NI' & dat2$REGISTER_DESC == 'Day'], useNA = 'ifany')
#     <=0 MWh 0-0.5MWh 0.5-1MWh 1-2MWh 2-4MWh 4-8MWh 8-12MWh 12-20MWh >20MWh
# 0.1       0        0        0      0      0      1       6        2     18
# 0.2       0        0        0      8    133    234     149      161    225
# 0.3       0        0       27     66     24      0       0        0      1
# 0.4       0        7       24      2      0      0       0        0      0
# 0.5       0       23        0      0      1      1       0        0      1
# 0.6       0       14        0      0      0      1       1        0      4
# 0.7       0       11        0      0      0      0       1        0      1
# 0.8       1       11        0      0      0      0       0        0      0
# 0.9       5       34        0      0      1      0       0        1      2
# 1        46        0        0      0      4      3       6        5     11

table(dat2$GR_PROF_PCT_BKT[indx & dat2$JURISDICTION == 'NI' & dat2$REGISTER_DESC == 'Night'], dat2$CONS_BKT[indx & dat2$JURISDICTION == 'NI' & dat2$REGISTER_DESC == 'Night'], useNA = 'ifany')

#     <=0 MWh 0-0.5MWh 0.5-1MWh 1-2MWh 2-4MWh 4-8MWh 8-12MWh 12-20MWh >20MWh
# 0.1       0      179      108    108    122    133      74       72     84
# 0.2       0       71       35     25     25     15      17       13      9
# 0.3       0       11        0      0      0      0       0        0      0
# 0.4       0       13        0      0      0      1       0        1      0
# 0.5       0        1        0      0      0      0       0        0      0
# 0.8       2        3        0      1      0      0       0        0      0
# 0.9       1        0        0      0      0      0       0        0      0
# 1         0        5        4      4      8      7       2        3      7

indx = dat2$JURISDICTION == 'ROI' & dat2$REGISTER_DESC == '24hr' & dat2$CATGRY %in% c('CBC', 'GP', 'GPNS', 'Weekender', 'Nightsaver', 'Popular')
plot(dat2$AVG_YEARLY_CONSUMPTION [indx], dat2$GR_PROF_PCT [indx], xlim = c(0,2e4), ylim = c(0,1), xlab = 'Usage', ylab = 'gross profit %', main = 'ROI, 24hr')

indx = dat2$JURISDICTION == 'ROI' & dat2$REGISTER_DESC == 'Day' & dat2$CATGRY %in% c('CBC', 'GP', 'GPNS', 'Weekender', 'Nightsaver', 'Popular')
plot(dat2$AVG_YEARLY_CONSUMPTION [indx], dat2$GR_PROF_PCT [indx], xlim = c(0,2e4), ylim = c(0,1), xlab = 'Usage', ylab = 'gross profit %', main = 'ROI, Day')

indx = dat2$JURISDICTION == 'ROI' & dat2$REGISTER_DESC == 'Night' & dat2$CATGRY %in% c('CBC', 'GP', 'GPNS', 'Weekender', 'Nightsaver', 'Popular')
plot(dat2$AVG_YEARLY_CONSUMPTION [indx], dat2$GR_PROF_PCT [indx], xlim = c(0,2e4), ylim = c(0,1), xlab = 'Usage', ylab = 'gross profit %', main = 'ROI, Night')

indx = dat2$JURISDICTION == 'NI' & dat2$REGISTER_DESC == '24hr' & dat2$CATGRY %in% c('CBC', 'GP', 'GPNS', 'Weekender', 'Nightsaver', 'Popular')
plot(dat2$AVG_YEARLY_CONSUMPTION [indx], dat2$GR_PROF_PCT [indx], xlim = c(0,2e4), ylim = c(0,1), xlab = 'Usage', ylab = 'gross profit %', main = 'NI, 24hr')

indx = dat2$JURISDICTION == 'NI' & dat2$REGISTER_DESC == 'Day' & dat2$CATGRY %in% c('CBC', 'GP', 'GPNS', 'Weekender', 'Nightsaver', 'Popular')
plot(dat2$AVG_YEARLY_CONSUMPTION [indx], dat2$GR_PROF_PCT [indx], xlim = c(0,2e4), ylim = c(0,1), xlab = 'Usage', ylab = 'gross profit %', main = 'NI, Day')

indx = dat2$JURISDICTION == 'NI' & dat2$REGISTER_DESC == 'Night' & dat2$CATGRY %in% c('CBC', 'GP', 'GPNS', 'Weekender', 'Nightsaver', 'Popular')
plot(dat2$AVG_YEARLY_CONSUMPTION [indx], dat2$GR_PROF_PCT [indx], xlim = c(0,2e4), ylim = c(0,1), xlab = 'Usage', ylab = 'gross profit %', main = 'NI, Night')


head(dat2[dat2$GR_PROF_PCT_BKT == 0.6 & !is.na(dat2$GR_PROF_PCT_BKT) & dat2$CATGRY == 'GPNS',] )

t(as.data.frame(dat2[dat2$MPRN == '10000670220', ]))


head(dat2[dat2$GR_PROF_PCT_BKT == 0.6 & !is.na(dat2$GR_PROF_PCT_BKT) & dat2$CATGRY == 'Nightsaver',] )


head(dat2[dat2$GR_PROF_PCT_BKT == 0.6 & !is.na(dat2$GR_PROF_PCT_BKT) & dat2$CATGRY == 'Weekender',] )


head(dat2[dat2$GR_PROF_PCT_BKT == 0.6 & !is.na(dat2$GR_PROF_PCT_BKT) & dat2$CATGRY == 'Popular',] )





sum1 <- sum_table2[, c('JURISDICTION', 'decile', 'NT_PROF')]%>%
  group_by(JURISDICTION, decile) %>%
  summarise_at("NT_PROF", funs(mean, min, max), na.rm=TRUE)
sum2 <- sum_table2[, c('JURISDICTION', 'decile', 'NT_PROF')] %>%
  group_by(JURISDICTION, decile) %>%
  summarise(n = n()) %>%
  full_join(sum1)

print(sum2, n=22)
# # A tibble: 22 x 6
# # Groups:   JURISDICTION [2]
# JURISDICTION decile     n      mean       min        max
# <chr>         <dbl> <int>     <dbl>     <dbl>      <dbl>
#  1 NI                1   882   -873.      -9050.      -373.
#  2 NI                2   883   -111.       -373.       124.
#  3 NI                3   883    352.        124.       596.
#  4 NI                4   883    850.        597.      1135.
#  5 NI                5   883   1456.       1136.      1753.
#  6 NI                6   883   2151.       1753.      2622.
#  7 NI                7   883   3251.       2623.      3993.
#  8 NI                8   883   5267.       3995.      6942.
#  9 NI                9   883   9646.       6951.     13541.
# 10 NI               10   883 342074.      13550. 181211824.
# 11 NI               NA   267    NaN         Inf       -Inf 
# 12 ROI               1  1730 -20662.   -7517340.      -412.
# 13 ROI               2  1730     -7.13     -411.       292.
# 14 ROI               3  1730    562.        293.       811.
# 15 ROI               4  1731   1097.        812.      1379.
# 16 ROI               5  1730   1611.       1379.      1910.
# 17 ROI               6  1730   2341.       1910.      2816.
# 18 ROI               7  1731   3453.       2816.      4233.
# 19 ROI               8  1730   5438.       4234.      6993.
# 20 ROI               9  1730   9689.       6995.     13451.
# 21 ROI              10  1731  61315.      13452.  32627208.
# 22 ROI              NA     1    NaN         Inf       -Inf 

indx = sum_table2$percentile <= 99 & sum_table2$percentile > 1
sum1 <- sum_table2[indx, c('JURISDICTION', 'decile', 'NT_PROF')]%>%
  group_by(JURISDICTION, decile) %>%
  summarise_at("NT_PROF", funs(mean, min, max), na.rm=TRUE)
sum2 <- sum_table2[indx, c('JURISDICTION', 'decile', 'NT_PROF')] %>%
  group_by(JURISDICTION, decile) %>%
  summarise(n = n()) %>%
  full_join(sum1)

print(sum2, n=22)

# # A tibble: 21 x 6
# # Groups:   JURISDICTION [3]
#    JURISDICTION decile     n     mean    min    max
#    <chr>         <dbl> <int>    <dbl>  <dbl>  <dbl>
#  1 NI                1   794  -681.   -1299.  -373.
#  2 NI                2   883  -111.    -373.   124.
#  3 NI                3   883   352.     124.   596.
#  4 NI                4   883   850.     597.  1135.
#  5 NI                5   883  1456.    1136.  1753.
#  6 NI                6   883  2151.    1753.  2622.
#  7 NI                7   883  3251.    2623.  3993.
#  8 NI                8   883  5267.    3995.  6942.
#  9 NI                9   883  9646.    6951. 13541.
# 10 NI               10   794 30016.   13550. 92813.
# 11 ROI               1  1557  -888.   -1995.  -412.
# 12 ROI               2  1730    -7.13  -411.   292.
# 13 ROI               3  1730   562.     293.   811.
# 14 ROI               4  1731  1097.     812.  1379.
# 15 ROI               5  1730  1611.    1379.  1910.
# 16 ROI               6  1730  2341.    1910.  2816.
# 17 ROI               7  1731  3453.    2816.  4233.
# 18 ROI               8  1730  5438.    4234.  6993.
# 19 ROI               9  1730  9689.    6995. 13451.
# 20 ROI              10  1557 23561.   13452. 56633.
# 21 NA               NA   268   NaN      Inf   -Inf 


sum1 <- sum_table2[, c('JURISDICTION', 'NT_PROF')]%>%
  group_by(JURISDICTION) %>%
  summarise_at("NT_PROF", funs(mean, min, max), na.rm=TRUE)
sum2 <- sum_table2[, c('JURISDICTION',  'NT_PROF')] %>%
  group_by(JURISDICTION) %>%
  summarise(n = n()) %>%
  full_join(sum1)

print(sum2, n=22)
# A tibble: 2 x 5
#   JURISDICTION     n   mean       min        max
#   <chr>        <int>  <dbl>     <dbl>      <dbl>
# 1 NI            9096 36410.    -9050. 181211824.
# 2 ROI          17304  6487. -7517340.  32627208.

indx = sum_table2$percentile <= 99 & sum_table2$percentile > 1
sum1 <- sum_table2[indx, c('JURISDICTION', 'NT_PROF')]%>%
  group_by(JURISDICTION) %>%
  summarise_at("NT_PROF", funs(mean, min, max), na.rm=TRUE)
sum2 <- sum_table2[indx, c('JURISDICTION',  'NT_PROF')] %>%
  group_by(JURISDICTION) %>%
  summarise(n = n()) %>%
  full_join(sum1)

print(sum2, n=22)

# A tibble: 3 x 5
#   JURISDICTION     n  mean    min    max
#   <chr>        <int> <dbl>  <dbl>  <dbl>
# 1 NI            8652 5025. -1299. 92813.
# 2 ROI          16956 4550. -1995. 56633.
# 3 NA             268  NaN    Inf   -Inf 












# troubleshoot differences between component files
# diff1 = setdiff( dat2$CUSTOMER_ID, churn_rte$CUSTOMER_ID ) # null
# diff2 = setdiff( churn_rte$CUSTOMER_ID, dat2$CUSTOMER_ID ) # 2453
# diff3 = setdiff( paste0(dat2$CUSTOMER_ID, 'P', dat2$PREMISE_ID), paste0(churn_rte$CUSTOMER_ID, 'P', churn_rte$PREMISE_ID)) # null
# diff4 = setdiff( paste0(churn_rte$CUSTOMER_ID, 'P', churn_rte$PREMISE_ID), paste0(dat2$CUSTOMER_ID, 'P', dat2$PREMISE_ID)) # 3472

ecdf(sum_table2$NT_PROF) (0)
# [1]  0.302714 # 30.3% have a loss
ecdf1 = t(data.frame('percentiles' = ecdf(sum_table2$NT_PROF) (seq(-500, 10000, by = 100))))
colnames(ecdf1) = seq(-500, 10000, by = 100)
ecdf1

hist(sum_table2$NT_PROF [ sum_table2$NT_PROF  > -500 & sum_table2$NT_PROF  < 1500], 
     xlab = 'Profitability Estimate (local currency)',
     main = '5 yr profitability',
     xlim = c(-500,1500),
     col = 3,
     breaks = 50) 

mean(sum_table2$NT_PROF, na.rm = T)/5
# [1] 1258.104
median(sum_table2$NT_PROF, na.rm = T)/5
# [1]  316.5909
mean(sum_table2$NT_PROF [sum_table2$JURISDICTION == 'ROI'], na.rm = T)/5
# [1] 1265.861
mean(sum_table2$NT_PROF [sum_table2$JURISDICTION == 'ROI' & sum_table2$DF_SF == 'DF' & is.finite(sum_table2$NT_PROF)], na.rm = T)/5
# [1] 49.42237
mean(sum_table2$NT_PROF [sum_table2$JURISDICTION == 'ROI' & sum_table2$DF_SF == 'SF' & is.finite(sum_table2$NT_PROF)], na.rm = T)/5
# [1] 60.63337
mean(sum_table2$NT_PROF [sum_table2$JURISDICTION == 'NI' & sum_table2$DF_SF == 'SF' & is.finite(sum_table2$NT_PROF)], na.rm = T)/5
# [1] 4.148493
median(sum_table2$NT_PROF [sum_table2$JURISDICTION == 'ROI'], na.rm = T)/5
# [1] 313.5044
mean(sum_table2$NT_PROF [sum_table2$JURISDICTION == 'NI'], na.rm = T)/5
# [1]  1238.395
median(sum_table2$NT_PROF [sum_table2$JURISDICTION == 'NI'], na.rm = T)/5
# [1] 323.5009

summary(sum_table2)


a <- ggplot(na.omit(sum_table2), aes(x = NT_PROF))
mu <- sum_table2 %>% 
  group_by(JURISDICTION) %>% 
  summarize_at ('NT_PROF', funs(grp.mean = mean, grp.median = median), na.rm = T)
mu
a2 <- a + 
  geom_density(aes(fill = JURISDICTION), alpha = 0.4) + 
  geom_vline(data = mu, aes(xintercept = grp.mean, color = JURISDICTION), linetype = "dashed") + 
  theme_minimal() +
  scale_color_discrete() + 
  coord_cartesian(xlim = c(-500, 2000), ylim = c(0,0.003)) 
a2

a <- ggplot(na.omit(sum_table2), aes(x = NT_PROF))
mu <- sum_table2 %>% 
  group_by(ACQ_CHANNEL) %>% 
  summarize_at ('NT_PROF', funs(grp.mean = mean, grp.median = median), na.rm = T)
mu
# # A tibble: 8 x 3
# ACQ_CHANNEL     grp.mean grp.median
# <fct>              <dbl>      <dbl>
# 1 Call Centre         221.      172. 
# 2 Door to Door        185.      142. 
# 3 Events              404.      356. 
# 4 Homemoves           129.      101. 
# 5 Online              197.      162. 
# 6 Online - ICS        127.       30.3
# 7 Other               276.      225. 
# 8 Property Button     107.       86.1

sum_table3 = sum_table2[sum_table2$JURISDICTION == 'ROI',]
mu <- sum_table3 %>% 
  group_by(ACQ_CHANNEL) %>% 
  summarize_at ('NT_PROF', funs(grp.mean = mean, grp.median = median), na.rm = T)
mu
# A tibble: 8 x 3
# ACQ_CHANNEL     grp.mean grp.median
# <fct>              <dbl>      <dbl>
# 1 Call Centre         296.      249. 
# 2 Door to Door        296.      274. 
# 3 Events              406.      358. 
# 4 Homemoves           180.      150. 
# 5 Online              276.      232. 
# 6 Online - ICS        127.       30.3
# 7 Other               321.      264. 
# 8 Property Button     107.       86.1

table(sum_table3$ACQ_CHANNEL, useNA = 'ifany')
# Call Centre    Door to Door          Events       Homemoves          Online 
# 45160          173701            6259           30463           25215 
# Online - ICS           Other Property Button 
# 8246            4542            8451 
a <- ggplot(na.omit(sum_table3), aes(x = NT_PROF))
# a2 <- a + 
#   geom_histogram(aes(fill = ACQ_CHANNEL), alpha = 0.8, binwidth = 20) + 
#   geom_vline(data = mu, aes(xintercept = grp.mean, color = ACQ_CHANNEL), linetype = "dashed") + 
#   theme_minimal() +
#   scale_color_discrete() + 
#   coord_cartesian(xlim = c(-500, 3000), ylim = c(0,6000))
# a2
a2 <- a + 
  geom_density(aes(fill = ACQ_CHANNEL), alpha = 0.4) + 
  geom_vline(data = mu, aes(xintercept = grp.mean, color = ACQ_CHANNEL), linetype = "dashed") + 
  theme_minimal() +
  scale_color_discrete() + 
  coord_cartesian(xlim = c(-500, 2000), ylim = c(0,0.003)) + 
  facet_wrap(.~ACQ_CHANNEL)
a2

mu <- sum_table3 %>% 
  group_by(UTILITY_TYPE_CODE) %>% 
  summarize_at ('NT_PROF', funs(grp.mean = mean, grp.median = median), na.rm = T)
mu
# A tibble: 2 x 3
# UTILITY_TYPE_CODE grp.mean grp.median
# <chr>                <dbl>      <dbl>
# 1 E                     275.       242.
# 2 G                     277.       241.

a2 <- a + 
  geom_density(aes(fill = UTILITY_TYPE_CODE), alpha = 0.4) + 
  geom_vline(data = mu, aes(xintercept = grp.mean, color = UTILITY_TYPE_CODE), linetype = "dashed") + 
  theme_minimal() +
  scale_color_discrete() + 
  coord_cartesian(xlim = c(-500, 2000), ylim = c(0,0.0025))
a2

a2 <- a + 
  geom_histogram(aes(fill = UTILITY_TYPE_CODE), alpha = 0.8, binwidth = 20) + 
  geom_vline(data = mu, aes(xintercept = grp.mean, color = UTILITY_TYPE_CODE), linetype = "dashed") + 
  theme_minimal()

a2 + 
  scale_color_manual(values = c("#999999", "#E69F00")) + 
  coord_cartesian(xlim = c(-500, 2000), ylim = c(0,10000))

sum_table3 = sum_table2[sum_table2$JURISDICTION == 'NI',]
mu <- sum_table3 %>% 
  group_by(ACQ_CHANNEL) %>% 
  summarize_at ('NT_PROF', funs(grp.mean = mean, grp.median = median), na.rm = T)  
mu
# A tibble: 8 x 3
# ACQ_CHANNEL     grp.mean grp.median
# <fct>              <dbl>      <dbl>
# 1 Call Centre         54.0      -12.3
# 2 Door to Door        20.0      -20.3
# 3 Events             170.       -21.6
# 4 Homemoves          -45.6      -65.1
# 5 Online              19.6      -29.6
# 6 Online - ICS        69.0       69.0
# 7 Other               44.3      -17.3
# 8 Property Button   -243.      -243. 

sum_table3 = sum_table3[sum_table3$ACQ_CHANNEL != 'Online - ICS' & sum_table3$ACQ_CHANNEL != 'Property Button',]
sum_table3$ACQ_CHANNEL = droplevels(sum_table3$ACQ_CHANNEL)
mu <- na.omit(sum_table3) %>% 
  group_by(ACQ_CHANNEL) %>% 
  summarize_at ('NT_PROF', funs(grp.mean = mean, grp.median = median), na.rm = T)  
mu
# A tibble: 6 x 3
# ACQ_CHANNEL  grp.mean grp.median
# <fct>           <dbl>      <dbl>
# 1 Call Centre      54.0      -12.3
# 2 Door to Door     20.0      -20.3
# 3 Events          170.       -21.6
# 4 Homemoves       -45.6      -65.1
# 5 Online           19.6      -29.6
# 6 Other            44.3      -17.3

table(sum_table3$ACQ_CHANNEL, useNA = 'ifany')
# Call Centre Door to Door       Events    Homemoves       Online        Other 
#       19991       116982           54         9029        11206          869 

a <- ggplot(na.omit(sum_table3), aes(x = NT_PROF))
# a2 <- a + 
#   geom_histogram(aes(fill = ACQ_CHANNEL), alpha = 0.8, binwidth = 20) + 
#   geom_vline(data = mu, aes(xintercept = grp.mean, color = ACQ_CHANNEL), linetype = "dashed") + 
#   theme_minimal() +
#   scale_color_discrete() + 
#   coord_cartesian(xlim = c(-500, 3000), ylim = c(0,6000))
# a2
a2 <- a + 
  geom_density(aes(fill = ACQ_CHANNEL), alpha = 0.4) + 
  geom_vline(data = mu, aes(xintercept = grp.mean, color = ACQ_CHANNEL), linetype = "dashed") + 
  theme_minimal() +
  scale_color_discrete() + 
  coord_cartesian(xlim = c(-500, 2000), ylim = c(0,0.003))

a2 + facet_wrap(.~ACQ_CHANNEL)

mu <- na.omit(sum_table3) %>% 
  group_by(UTILITY_TYPE_CODE) %>% 
  summarize_at ('NT_PROF', funs(grp.mean = mean, grp.median = median), na.rm = T)  
mu
# A tibble: 1 x 3
#     UTILITY_TYPE_CODE grp.mean grp.median
#     <chr>                <dbl>      <dbl>
#   1 E                     20.7      -23.7

a <- ggplot(na.omit(sum_table3), aes(x = NT_PROF))
# a2 <- a + 
#   geom_histogram(aes(fill = ACQ_CHANNEL), alpha = 0.8, binwidth = 20) + 
#   geom_vline(data = mu, aes(xintercept = grp.mean, color = ACQ_CHANNEL), linetype = "dashed") + 
#   theme_minimal() +
#   scale_color_discrete() + 
#   coord_cartesian(xlim = c(-500, 3000), ylim = c(0,6000))
# a2

# density plots
a2 <- a + 
  geom_density(aes(fill = UTILITY_TYPE_CODE), alpha = 0.4) + 
  geom_vline(data = mu, aes(xintercept = grp.mean, color = UTILITY_TYPE_CODE), linetype = "dashed") + 
  theme_minimal() +
  scale_color_discrete() + 
  coord_cartesian(xlim = c(-500, 2000), ylim = c(0,0.003))
a2

# stacked Histogram
a2 <- a + 
  geom_histogram(aes(fill = UTILITY_TYPE_CODE), alpha = 0.8, binwidth = 20) + 
  geom_vline(data = mu, aes(xintercept = grp.mean, color = UTILITY_TYPE_CODE), linetype = "dashed") + 
  theme_minimal()

a2 + 
  scale_color_manual(values = c("#999999", "#E69F00")) + 
  coord_cartesian(xlim = c(-500, 2000), ylim = c(0,8000))


#########################################################################################################
##################### BE MONTHLY RENEWALS PROCESS #######################################################
#########################################################################################################

library(DBI)
library(ROracle)
library(dplyr)
library(sqldf)

rep_month <- toupper(lubridate::month(lubridate::ymd(Sys.Date()) + months(3), label = TRUE,
                                      abbr = FALSE))
rep_year <- lubridate::year(lubridate::ymd(Sys.Date()) + months(3))
contract_end_month <- toupper(lubridate::month(lubridate::ymd(Sys.Date()) + months(2), label = TRUE,
                                               abbr = FALSE))
contract_end_year <- lubridate::year(lubridate::ymd(Sys.Date()) + months(2))

drv <- dbDriver("Oracle")
host <- "dubla055.airtricity.com"
port <- "1526"
sid <- "pceprdr"
connect.string <- paste(
  "(DESCRIPTION=",
  "(ADDRESS=(PROTOCOL=tcp)(HOST=", host, ")(PORT=", port, "))",
  "(CONNECT_DATA=(SID=", sid, ")))", sep = "")

conn_pce <- dbConnect(drv, username =  dbname = connect.string, prefetch = FALSE,
                      bulk_read = 1000L, stmt_cache = 0L, external_credentials = FALSE,
                      sysdba = FALSE)

peace_class <- dbGetQuery(conn_pce, "select A.debtornum,  A.classification_code
from (
select
CL.debtornum
, row_number() over (partition by CL.debtornum order by CL.DATE_EFFECTIVE desc) as row_id
, LTRIM(RTRIM(CL.CLASSIFICATN_CODE)) as classification_code
from energydb.AR_CUST_CLASS_HIST CL
inner join energydb.pm_all_consprem AB
on CL.debtornum = AB.debtornum
WHERE AB.status_cons = 'C'
AND to_char(AB.changedate,'YYYY-MM-DD') < to_char(sysdate,'YYYY-MM-DD')
) A
where A.row_id = 1
order by A.debtornum")

dbDisconnect(conn_pce)

drv <- dbDriver("Oracle")
host <- "Azula008"
port <- "1526"
sid <- "DMPRD1"
connect.string <- paste(
  "(DESCRIPTION=",
  "(ADDRESS=(PROTOCOL=tcp)(HOST=", host, ")(PORT=", port, "))",
  "(CONNECT_DATA=(SID=", sid, ")))", sep = "")

conn_dm2 <- dbConnect(drv, username =  dbname = connect.string,
                      prefetch = FALSE, bulk_read = 1000L, stmt_cache = 0L, external_credentials = FALSE, sysdba = FALSE)

ROI_E <- dbGetQuery(conn_dm2, "SELECT table_name
  from (select table_name from all_tab_cols 
  where table_name like '%TARIFFSETUP_ROI_E%' 
  and REGEXP_LIKE(substr(table_name, length(table_name)-6, 4), '^[0-9]+$')
  order by table_name desc) A
where rownum = 1")

ROI_E

ROI_G <- dbGetQuery(conn_dm2, "SELECT table_name
  from (select table_name from all_tab_cols 
  where table_name like '%TARIFFSETUP_ROI_G%' 
  and REGEXP_LIKE(substr(table_name, length(table_name)-6, 4), '^[0-9]+$')
  order by table_name desc) A
where rownum = 1")

ROI_G

NI_E <- dbGetQuery(conn_dm2, "SELECT table_name
  from (select table_name from all_tab_cols 
  where table_name like '%TARIFFSETUP_NI_E%' 
  and REGEXP_LIKE(substr(table_name, length(table_name)-6, 4), '^[0-9]+$')
  order by table_name desc) A
where rownum = 1")

NI_E

dm2_sql = paste0("SELECT DISTINCT AA.JURISDICTION 
,AA.BE_SEGMENT
,AA.Customer_ID
,AA.name
,AA.postal_address_name
,AA.postal_address_line1
,AA.postal_address_line2
,AA.postal_address_line3
,AA.Postcode
,AA.email
,AA.phone
,AA.Premise_ID
,AA.Utility_type_Code
,LTRIM(RTRIM(AA.MPRN)) AS MPRN
,dense_rank() over (partition by AA.customer_Id order by LTRIM(RTRIM(AA.MPRN)) desc) + dense_rank() over (partition by AA.customer_Id order by LTRIM(RTRIM(AA.MPRN)) asc) - 1 as num_mprns_this_month
,NVL (AA.DG, prfs.DG) as DG
,NVL (AA.PROFILE, prfs.PROFILE) as profile
,CASE WHEN NVL (AA.DG, prfs.DG) = 'DG5' AND NVL (AA.PROFILE, prfs.PROFILE) = '05' THEN 'P5'
WHEN NVL (AA.DG, prfs.DG) = 'DG5' AND NVL (AA.PROFILE, prfs.PROFILE) = '06' THEN 'P6'
WHEN NVL (AA.DG, prfs.DG) = 'DG6' THEN 'LVMD'
WHEN NVL (AA.DG, prfs.DG) = 'T031' THEN 'Popular'
WHEN NVL (AA.DG, prfs.DG) IN ('T032', 'T034') THEN 'Nightsaver'
WHEN NVL (AA.DG, prfs.DG) = 'T033' THEN 'Weekender'
WHEN NVL (AA.DG, prfs.DG) IN ('T041', 'T042', 'T043') THEN 'Off-Peak' 
ELSE NVL (AA.DG, prfs.DG) END as CATGRY
,CASE WHEN AA.Utility_type_Code = 'E' THEN NVL(R.AVG_YEARLY_CONSUMPTION, EUF.EAC) 
ELSE NVL(R.AVG_YEARLY_CONSUMPTION, AQ.EAC) END AS AVG_YEARLY_CONSUMPTION                                       
,CASE WHEN AA.Utility_type_Code = 'E' THEN EUF.EAC
ELSE AQ.EAC END AS EAC   

,trunc(AA.Contract_Start_Date) as Contract_Start_Date
,trunc(AA.Contract_End_Date) as Contract_End_Date
,AD.aged_debt_insert_date
,AD.account_group_name
,AD.tot_overdue_amt
,AD.max_overdue_duration
,AD.amt_max_duration
,AD.status_description
,AD.arrange_due_date
,AD.arrange_amount_due
,AD.arrange_amount_paid
,AD.active_arrangement

FROM (                   
  SELECT CM.Customer_ID
  ,CM.Premise_ID
  ,PRN.MPRN
  ,CM.Utility_type_Code
  ,CM.Acquisition_date_sid
  ,C.full_name as name
  ,C.postal_address_name
  ,C.postal_address_line1
  ,C.postal_address_line2
  ,C.postal_address_line3
  ,C.Postcode
  ,NVL(c.email, c.promotion_email) as email
  ,NVL(c.mobile_phone, NVL(c.work_phone, c.home_phone)) as phone
  ,LCC.Customer_Type_Desc as BE_segment
  ,AR.Contract_Start_Date
  ,AR.Contract_End_Date
  ,DENSE_RANK() over (PARTITION BY CM.Customer_ID,CM.Premise_ID,PRN.MPRN,CM.Utility_type_Code ORDER BY CM.Acquisition_Date_SID DESC) as row_id
  ,COM.company_ID AS Jurisdiction
  ,CASE WHEN (CM.Utility_type_Code = 'E' AND COM.company_ID = 'ROI') THEN TSROIE.DG
      WHEN (CM.Utility_type_Code = 'E' AND COM.company_ID = 'NI') THEN TSNIE.DG 
      WHEN CM.Utility_type_Code = 'G' THEN TSROIG.DG END AS DG
  ,CASE WHEN (CM.Utility_type_Code = 'E' AND COM.company_ID = 'ROI') THEN TSROIE.PROFILE
      WHEN (CM.Utility_type_Code = 'E' AND COM.company_ID = 'NI') THEN TSNIE.PROFILE 
      WHEN CM.Utility_type_Code = 'G' THEN TSROIG.PROFILE END AS PROFILE

  FROM DWH.FACT_CUSTOMER_MOVEMENT CM
  LEFT JOIN DWH.LKP_Tenancy_Change TC
  ON CM.Tenancy_change_SID = TC.Tenancy_change_SID
  LEFT JOIN DWH.DIM_PRN PRN
  ON PRN.PRN_SID = CM.PRN_SID
  AND CM.Utility_type_Code = PRN.UTILITY_TYPE_CODE
  AND CM.Premise_ID = PRN.Premise_ID
  LEFT JOIN DWH.Dim_Customer C
  ON C.Customer_SID = CM.Customer_SID
  LEFT JOIN DWH.LKP_COMPANY COM
  ON COM.Company_SID = C.Company_SID
  LEFT JOIN DWH.LKP_CUSTOMER_CLASSIFICATION LCC
  ON LCC.CUSTOMER_CLASSIFICATION_SID = CM.CUSTOMER_CLASSIFICATION_SID

LEFT JOIN (SELECT * FROM (SELECT DISTINCT TS3.Customer_no AS CUSTOMER_ID
,TS3.Premise AS PREMISE_ID
,TS3.MPRN_GPRN AS MPRN
,TS3.Utility_Type AS UTILITY_TYPE_CODE
,LTRIM(RTRIM(TS3.DUOS_CBC)) AS DG
,TS3.PROFILE_AC_BAND_CODE AS PROFILE
,'NI' AS JURISDICTION
,ROW_NUMBER () OVER (PARTITION BY TS3.Customer_No, TS3.Premise, TS3.MPRN_GPRN, TS3.Utility_Type ORDER BY TS3.Rate_Effective_From DESC) AS Row_Id

FROM BISHARED.", NI_E," TS3 
where TS3.Utility_Type = 'E'
AND TS3.Tariff_Description NOT IN ('PSO Levy', 'Carbon Tax', 'Climate Change Levy', 'lifestyle PAYG Service Charge')
AND TS3.Stepnum = 1
AND REGEXP_LIKE(trim(TS3.PREMISE), '^[0-9]+$')
)TSNIEE
WHERE TSNIEE.row_ID = 1
) TSNIE
ON TSNIE.Customer_ID = CM.Customer_ID
AND TSNIE.Premise_ID = CM.Premise_ID
AND TSNIE.MPRN = PRN.MPRN
AND COM.Company_ID = 'NI'

LEFT JOIN  (SELECT * FROM (SELECT DISTINCT TS2.Customer_no AS CUSTOMER_ID
,TS2.Premise AS PREMISE_ID
,TS2.MPRN_GPRN AS MPRN
,TS2.Utility_Type AS UTILITY_TYPE_CODE
,LTRIM(RTRIM(TS2.DUOS_CBC)) AS DG
,TS2.PROFILE_AC_BAND_CODE AS PROFILE
,'ROI' AS JURISDICTION
,ROW_NUMBER () OVER (PARTITION BY TS2.Customer_No, TS2.Premise, TS2.MPRN_GPRN, TS2.Utility_Type ORDER BY TS2.Rate_Effective_From DESC) AS Row_Id

FROM BISHARED.", ROI_G, " TS2
where TS2.Utility_Type = 'G'
AND LTRIM(RTRIM(TS2.DUOS_CBC)) ='CBC'
AND TS2.Tariff_Description NOT IN ('PSO Levy', 'Carbon Tax', 'Climate Change Levy', 'lifestyle PAYG Service Charge')
AND TS2.Stepnum = 1 
AND REGEXP_LIKE(trim(TS2.PREMISE), '^[0-9]+$')
) TSROIGG
WHERE TSROIGG.row_ID = 1
) TSROIG
ON TSROIG.Customer_ID = CM.Customer_ID
AND TSROIG.Premise_ID = CM.Premise_ID
AND TSROIG.MPRN = PRN.MPRN
AND COM.Company_ID = 'ROI'
AND CM.UTILITY_TYPE_CODE = 'G'

LEFT JOIN (SELECT * FROM (SELECT DISTINCT TS.Customer_no AS CUSTOMER_ID
,TS.Premise AS PREMISE_ID
,TS.MPRN_GPRN AS MPRN
,TS.Utility_Type AS UTILITY_TYPE_CODE
,LTRIM(RTRIM(TS.DUOS_CBC)) AS DG
,TS.PROFILE_AC_BAND_CODE AS PROFILE
,'ROI' AS JURISDICTION
,ROW_NUMBER () OVER (PARTITION BY TS.Customer_No, TS.Premise, TS.MPRN_GPRN, TS.Utility_Type ORDER BY TS.Rate_Effective_From DESC) AS Row_Id
FROM BISHARED.", ROI_E, " TS
where TS.Utility_Type = 'E'
AND LTRIM(RTRIM(TS.DUOS_CBC)) IN ('DG1', 'DG2', 'DG5', 'DG6')
AND TS.Tariff_Description NOT IN ('PSO Levy', 'Carbon Tax', 'Climate Change Levy', 'lifestyle PAYG Service Charge')
AND TS.Stepnum = 1
AND REGEXP_LIKE(trim(TS.PREMISE), '^[0-9]+$')
) TSROIEE
WHERE TSROIEE.row_ID = 1
) TSROIE

ON TSROIE.Customer_id = CM.Customer_ID
AND TSROIE.Premise_id = CM.Premise_ID
AND TSROIE.MPRN = PRN.MPRN
AND COM.Company_ID = 'ROI'
AND CM.UTILITY_TYPE_CODE = 'E'

  LEFT JOIN (
    SELECT REN.CUSTOMER_ID
    ,REN.PREMISE_ID
    ,REN.PRN_SID
    ,REN.MPRN
    ,REN.UTILITY_TYPE_CODE
    ,REN.Acquisition_Date_SID
    ,REN.Contract_Type_Desc as Contract_Type
    ,CASE WHEN REN.UTILITY_TYPE_CODE = 'E' THEN NVL(REN.ELEC_CONTRACT_END_DATE, REN.CONTRACT_END_DATE_SID)
    ELSE NVL(REN.GAS_CONTRACT_END_DATE, REN.CONTRACT_END_DATE_SID) END AS Contract_End_date
    ,CASE WHEN REN.UTILITY_TYPE_CODE = 'E' THEN NVL(REN.ELEC_CONTRACT_START_DATE, REN.CONTRACT_START_DATE_SID)
    ELSE NVL(REN.GAS_CONTRACT_START_DATE, REN.CONTRACT_START_DATE_SID) END AS Contract_START_date
    FROM ANALYTICS.ACTIVE_RENEWALS REN
  ) AR
  ON AR.Customer_ID = CM.Customer_ID
  AND AR.Premise_ID = CM.Premise_ID
  AND AR.PRN_SID = CM.PRN_SID
  AND AR.Utility_type_Code = CM.UTILITY_TYPE_CODE
  AND AR.MPRN = PRN.MPRN
  AND AR.Acquisition_Date_SID = CM.Acquisition_Date_SID
  
  WHERE LCC.CUSTOMER_TYPE_DESC IN ('SME', 'ENTERPRISE')
  AND CM.IS_ACTIVE_IND = 'Y'
  AND CM.CUST_MOVEMENT_EVENT_TYPE_SID = 21
  AND To_CHAR(CM.LOSS_DATE_SID, 'YYYY-MM-DD') = '1900-01-01'
)AA

LEFT JOIN (                   
  SELECT SER.CUSTOMER_ID
  ,SER.premise_ID
  ,SER.MPRN
  ,SER.Utility_Type_Code
  ,CASE WHEN ((MAX(SER.READING_DATE_SID) - MIN(SER.READING_DATE_SID)) < 250 or count(SER.ENERGY_CONSUMPTION_FCT) < 3) THEN NULL
  ELSE ROUND(SUM(SER.ENERGY_CONSUMPTION_FCT)/ ( MAX(SER.READING_DATE_SID) - MIN(SER.READING_DATE_SID) )* 365,0) 
  END AVG_YEARLY_CONSUMPTION
  ,SER.Acquisition_date_SID
  
  FROM (
    SELECT DISTINCT M.CUSTOMER_ID
    ,S.premise_ID
    ,S.MPRN
    ,M.Utility_Type_Code
    ,S.Consumption_type_Code
    ,M.READING_DATE_SID
    ,M.ENERGY_CONSUMPTION_FCT
    ,M.Service_SID
    ,M.Meter_register_SID AS Register_SID
    ,S.Service_ID as Servicenum -- numeric
    ,DMR.register_code as registernum -- varchar
    ,FCMM.Acquisition_date_SID
    
    FROM DWH.FACT_METER_READ M
    LEFT JOIN ANALYTICS.DIM_SERVICE_ML S
    ON S.service_SID = M.Service_SID
    AND S.Utility_type_Code = M.Utility_type_Code 
    LEFT JOIN DWH.Dim_Meter_Register DMR
    ON DMR.Register_SID = M.Meter_register_SID
    LEFT join dwh.lkp_meter_read_type mrt
    on mrt.meter_read_type_sid = m.meter_read_type_sid
    LEFT join dwh.lkp_yes_no_ind yn
    on yn.yes_no_ind_sid = m.reversed_ind_sid
    LEFT join dwh.lkp_yes_no_ind ynm 
    on ynm.yes_no_ind_sid = m.meter_read_cancel_ind_sid
    LEFT join dwh.lkp_meter_read_type mrt2
    on mrt2.meter_read_type_sid = m.prev_meter_read_type_sid
    INNER JOIN (
      select FCM.customer_ID, FCM.Premise_ID, FCM.Utility_type_Code, PRN.MPRN, FCM.Acquisition_Date_sid
      from DWH.Fact_Customer_Movement FCM
      inner JOIN DWH.DIM_PRN PRN 
      ON PRN.PRN_SID = FCM.PRN_SID
      where FCM.Is_active_ind = 'Y' 
      and FCM.Cust_Movement_Event_type_SID = 21 
      and to_char(FCM.Loss_Date_Sid, 'YYYY-MM-DD') = '1900-01-01'
    ) FCMM
    ON FCMM.customer_ID = M.CUSTOMER_ID
    AND FCMM.premise_ID = S.Premise_ID
    AND FCMM.Utility_Type_Code = S.Utility_Type_Code
    AND FCMM.MPRN = S.MPRN
    AND M.READING_DATE_SID >= FCMM.Acquisition_Date_sid
    
    where 1=1
    and yn.yes_no_ind_code != 'Y' -- not reversed
    and ynm.yes_no_ind_code != 'Y' -- not cancelled
    and mrt.meter_read_type_desc != 'Reconnect'
    and REGEXP_LIKE(M.ENERGY_CONSUMPTION_FCT, '^[0-9]+$')                  
    AND M.Customer_ID >= 0
  )SER
  
  GROUP BY SER.CUSTOMER_ID, SER.Premise_ID, SER.MPRN, SER.Utility_Type_Code, SER.Acquisition_Date_sid

) R

ON R.CUSTOMER_ID = AA.CUSTOMER_ID
AND R.Premise_ID = AA.Premise_ID
AND R.MPRN = AA.MPRN
AND R.Utility_Type_Code = AA.Utility_Type_Code 
AND R.Acquisition_Date_sid = AA.Acquisition_Date_sid

LEFT JOIN (
  SELECT DISTINCT BB.premise_ID
  ,BB.Utility_Type_Code
  ,SUM(BB.EUF) AS EAC
  
  FROM (SELECT  DISTINCT svc.premise_ID
        ,sdp.Utility_type AS Utility_Type_Code
        ,NVL(CAST(ltrim(rtrim(sdp.external_reference)) AS number),0) AS EUF
        ,row_number() OVER (PARTITION BY svc.premise_ID, sdp.Utility_type, sdp.role_id, svc.service_id, sdp.Role_Description ORDER BY sdp.start_date DESC) AS row_id
        ,sdp.Role_Description AS Register_Desc
        ,cast(ltrim(rtrim(svc.service_id)) as number) AS servicenum
        ,LTRIM(RTRIM(sdp.role_id)) AS registernum
        
        FROM dwh.rep_ref_external_reference_inc@ODI_SIT sdp
        INNER JOIN dwh.rep_svc_service_point_detail@ODI_SIT svc 
        ON svc.service_point_id = sdp.service_point_id
        INNER JOIN Analytics.pm_mkt_party_role mpr -- 270 mapped to Peace
        on sdp.role_id = mpr.participant_role
        
        WHERE sdp.participant_ID = 'EUF'
        AND sdp.Utility_Type = 'E'
        AND mpr.system_role = 'EUF'
        AND UPPER(sdp.Role_Description) NOT LIKE '%EXPORT%' -- export registers get payments not charges
        AND (sdp.external_reference IS NULL OR REGEXP_LIKE(ltrim(rtrim(sdp.external_reference)), '^[0-9]+$'))
        AND sdp.end_date IS NULL
        AND svc.TYPE = 'M'
  ) BB
  
  WHERE BB.row_id = 1
  GROUP BY BB.premise_ID, BB.Utility_type_code
) EUF
ON EUF.Premise_ID = AA.Premise_ID
AND AA.Utility_Type_Code = 'E'

LEFT JOIN (
  select AQQ.premise_ID
  ,AQQ.Utility_Type_Code 
  ,SUM(AQQ.AQ) AS EAC
  
  FROM (
    SELECT svc.premise_ID
    ,sdp.Utility_type as Utility_type_code
    ,svc.service_id
    ,row_number() OVER (PARTITION BY svc.premise_ID, sdp.Utility_Type, svc.service_id ORDER BY sdp.start_date DESC) AS row_id
    ,NVL(CAST(ltrim(rtrim(sdp.external_reference)) AS number),0) AS AQ
    
    FROM dwh.rep_ref_external_reference_inc@ODI_SIT sdp
    INNER JOIN dwh.rep_svc_service_point_detail@ODI_SIT svc 
    ON svc.service_point_id = sdp.service_point_id
    INNER JOIN Analytics.pm_mkt_party_role mpr -- 270 mapped to Peace
    on sdp.role_id = mpr.participant_role
    
    WHERE participant_ID = 'BG'
    AND sdp.role_ID = 'AQ'
    and mpr.system_role = 'EUF'
    AND sdp.Utility_type = 'G'
    AND (sdp.external_reference IS NULL OR REGEXP_LIKE(ltrim(rtrim(sdp.external_reference)), '^[0-9]+$'))
    AND REGEXP_LIKE(ltrim(rtrim(svc.service_id)), '^[0-9]+$')
    AND sdp.end_date IS NULL
    AND svc.TYPE = 'M'
  ) AQQ
  WHERE AQQ.row_id = 1
  GROUP BY AQQ.premise_ID, AQQ.Utility_type_code
) AQ
ON AQ.Premise_ID = AA.Premise_ID
AND AA.Utility_Type_Code = 'G'   

LEFT JOIN(
  select prof.PREMISE_ID
  ,prof.UTILITY_TYPE
  ,LTRIM(RTRIM(prof.ROLE_ID)) AS DG
  ,LTRIM(RTRIM(prof.EXTERNAL_REFERENCE)) AS PROFILE
  ,prof.start_date
  from
  (select pro.*
      , Row_Number() over (partition by pro.premise_id, pro.utility_type order by pro.insert_timestamp desc) row_id 
    from ( --------------------------------------------------------------------------------------------------------
             select sdp.participant_id,svc.service_point_id, svc.premise_id, 
           svc.type, sdp.utility_type, sdp.role_id, sdp.external_reference,
           sdp.start_date, sdp.end_date,
           sdp.insert_timestamp
           from DWH.rep_svc_service_point_detail@dwh_ro_dmstprd1 svc, -- Replica Table
           DWH.REP_REF_EXTERNAL_REFERENCE_INC@dwh_ro_dmstprd1 sdp, -- Replica Table
           Analytics.VEN_SYSDATES ens, -- Active Date
           Analytics.pm_mkt_party_role mpr -- 270 mapped to Peace
           where 1=1
           and svc.type = 'P'
           and svc.service_point_id = sdp.service_point_id
           and sdp.role_id = mpr.participant_role
           and ens.de_date between sdp.start_date and nvl(sdp.end_date, ens.de_date)
           and mpr.system_role = 'LPC' 
    ) pro 
  ) prof where prof.row_id = 1
) prfs
ON prfs.premise_ID = AA.Premise_ID
AND prfs.Utility_Type = AA.Utility_Type_Code

left join (select fad.customer_id
, fad.account_group_name
, fad.Utility_group_desc
, fad.insert_date as aged_debt_insert_date
, NVL(total_unpaid,0)  - NVL(notdue,0) as tot_overdue_amt
, case when NVL(due_180,0) > NVL(due_151_180,0) and NVL(due_180,0) > NVL(due_121_150,0) and NVL(due_180,0) > NVL(due_91_120,0) and NVL(due_180,0) > NVL(due_61_90,0) and NVL(due_180,0) > NVL(due_31_60,0) and NVL(due_180,0) > NVL(due_0_30,0) then '180+'
       when NVL(due_151_180,0) > NVL(due_121_150,0) and NVL(due_151_180,0) > NVL(due_91_120,0) and NVL(due_151_180,0) > NVL(due_61_90,0) and NVL(due_151_180,0) > NVL(due_31_60,0) and NVL(due_151_180,0) > NVL(due_0_30,0) then '151_180'
       when NVL(due_121_150,0) > NVL(due_91_120,0) and NVL(due_121_150,0) > NVL(due_61_90,0) and NVL(due_121_150,0) > NVL(due_31_60,0) and NVL(due_121_150,0) > NVL(due_0_30,0) then '121_150'
       when NVL(due_91_120,0) > NVL(due_61_90,0) and NVL(due_121_150,0) > NVL(due_31_60,0) and NVL(due_121_150,0) > NVL(due_0_30,0) then '91_120'
       when NVL(due_61_90,0) > NVL(due_31_60,0) and NVL(due_121_150,0) > NVL(due_0_30,0) then '61_90'
       when NVL(due_31_60,0) > NVL(due_0_30,0) then '31_60'
       when NVL(due_0_30,0) > 0 then '0_30'                                    
       END as max_overdue_duration 
, case when NVL(due_180,0) > NVL(due_151_180,0) and NVL(due_180,0) > NVL(due_121_150,0) and NVL(due_180,0) > NVL(due_91_120,0) and NVL(due_180,0) > NVL(due_61_90,0) and NVL(due_180,0) > NVL(due_31_60,0) and NVL(due_180,0) > NVL(due_0_30,0) then NVL(due_180,0)
       when NVL(due_151_180,0) > NVL(due_121_150,0) and NVL(due_151_180,0) > NVL(due_91_120,0) and NVL(due_151_180,0) > NVL(due_61_90,0) and NVL(due_151_180,0) > NVL(due_31_60,0) and NVL(due_151_180,0) > NVL(due_0_30,0) then NVL(due_151_180,0)
       when NVL(due_121_150,0) > NVL(due_91_120,0) and NVL(due_121_150,0) > NVL(due_61_90,0) and NVL(due_121_150,0) > NVL(due_31_60,0) and NVL(due_121_150,0) > NVL(due_0_30,0) then NVL(due_121_150,0)
       when NVL(due_91_120,0) > NVL(due_61_90,0) and NVL(due_91_120,0) > NVL(due_31_60,0) and NVL(due_91_120,0) > NVL(due_0_30,0) then NVL(due_91_120,0)
       when NVL(due_61_90,0) > NVL(due_31_60,0) and NVL(due_61_90,0) > NVL(due_0_30,0) then NVL(due_61_90,0)
       when NVL(due_31_60,0) > NVL(due_0_30,0) then NVL(due_31_60,0)
       when NVL(due_0_30,0) > 0 then NVL(due_0_30,0)                                    
       END as amt_max_duration
, status_description
, fad.arrange_due_date
, fad.arrange_amount_due
, fad.arrange_amount_paid
, fad.active_arrangement
from analytics.fact_aged_debt fad
where 1= 1
and fad.active = 'Active'
and fad.customer_type_desc in ('SME', 'ENTERPRISE')
)AD
on AD.customer_ID = AA.customer_ID

WHERE AA.row_id = 1
and to_char(AA.Contract_End_Date, 'fmMONTH') = '",contract_end_month,"'
and to_char(AA.Contract_End_Date, 'fmyyyy') = '",contract_end_year,"'
                              
ORDER BY AA.Customer_ID
,AA.Premise_ID
,MPRN
,AA.Utility_type_Code")

dm2_dat <- dbGetQuery(conn_dm2, dm2_sql)

dbDisconnect(conn_dm2)


dat <- sqldf("SELECT *
             FROM dm2_dat as l
             LEFT JOIN peace_class as r
             on l.CUSTOMER_ID = r.DEBTORNUM")

dat_renewals = dat[,-which(colnames(dat)=='DEBTORNUM')]


day_month = paste0(format(Sys.time(), "%d"),format(Sys.time(), "%b"))

setwd("S:/Retail/IRL/Supply/Marketing/MARKETING SUPPLY/Analytics & Products/Analytics/MM WIP/BE Value Model")


dat_renewals$ANALYTICS_CREDIT_OUTCOME <- ifelse(as.numeric(dat_renewals$TOT_OVERDUE_AMT)  %in% c(0) | is.na(dat_renewals$TOT_OVERDUE_AMT) , "OK", "DEBT_OS")
# write.csv(dat_renewals, paste0('BE_renewals_',rep_month,'_NEW_',day_month, '.csv'), row.names = F)
#
###############################################
## Combine the Churn Model data with BE_renewals data
###############################################

# setwd("S:/Retail/IRL/Supply/Marketing/MARKETING SUPPLY/Analytics & Products/Analytics/MM WIP/BE Churn")

cust_pred_curr71 = dat_churn_model

# setwd("S:/Retail/IRL/Supply/Marketing/MARKETING SUPPLY/Analytics & Products/Analytics/MM WIP/BE Value Model")

BE_ren = dat_renewals

cust_pred_curr91 <- sqldf("SELECT *
             FROM BE_ren as l
             LEFT JOIN cust_pred_curr71 as r
             on l.CUSTOMER_ID = r.Customer_ID
             and l.PREMISE_ID = r.Premise_ID
             and l.MPRN = r.MPRN
             and l.UTILITY_TYPE_CODE = r.Utility_Type")

cust_pred_curr101 <- cust_pred_curr91[,-c(which(colnames(cust_pred_curr91) == 'Customer_ID'),
                                          which(colnames(cust_pred_curr91) == 'Premise_ID'),
                                          max(which(colnames(cust_pred_curr91) == 'MPRN')),
                                          which(colnames(cust_pred_curr91) == 'Utility_Type'),
                                          which(colnames(cust_pred_curr91) == 'Segment'),
                                          which(colnames(cust_pred_curr91) == 'Contract_end_date'))]

# Added the below code to remove mulitple churn score entry for the combination of CUSTOMER_ID,PREMISE_ID, MPRN, UTILITY_TYPE_CODE
cust_pred_curr101 <- cust_pred_curr101 %>% distinct(CUSTOMER_ID,PREMISE_ID, MPRN, UTILITY_TYPE_CODE, .keep_all= TRUE)

######################################################
### Combine the Value Model data with BE_renewals data
######################################################


# setwd("S:/Retail/IRL/Supply/Marketing/MARKETING SUPPLY/Analytics & Products/Analytics/MM WIP/BE Value Model")

BE_value = dat_value_model

cust_pred_curr111 <- sqldf("SELECT *
             FROM cust_pred_curr101 as l
             LEFT JOIN BE_value as r
             on r.CUSTOMERID = l.Customer_ID
             and l.MPRN = r.MPRN")

cust_pred_curr121 <- cust_pred_curr111[,-c(which(colnames(cust_pred_curr111) == 'CUSTOMERID'),
                                           max(which(colnames(cust_pred_curr111) == 'MPRN')))]




################################################
### Rename Last Updated Date columns
################################################

cust_pred_curr121 <- cust_pred_curr121 %>% rename(Churn_Updated_Date = Last_Updated_Date)
cust_pred_curr121 <- cust_pred_curr121 %>% rename(Value_Updated_Date = INSERTEDDATE)

#####################################################
### Generate Anlytics Outcome (Office Tender, No Action, Renewal Top 6 (25%), Next 6 (25%) & Renewal Letter (50%))
#####################################################

# setwd("S:/Retail/IRL/Supply/Marketing/MARKETING SUPPLY/Analytics & Products/Analytics/MM WIP/BE Churn")
# 
# day_month = paste0(format(Sys.time(), "%d"),format(Sys.time(), "%b"))
# 
# write.csv(cust_pred_curr12, paste0('BE_renewals_',rep_month,'_',day_month, '.csv'), row.names = F )

BE_RENEWALS <- cust_pred_curr121


#####################################################################################
### Join with PEACE to get Cust_Full_Name and fix billing address problem using DM2
#####################################################################################

# vals = paste(BE_RENEWALS$CUSTOMER_ID, collapse = ", ")
# vals.paren = paste0("(", vals, ")")
# 
# drv <- dbDriver("Oracle")
# host <- "dubla055.airtricity.com"
# port <- "1526"
# sid <- "pceprdr"
# connect.string <- paste(
#   "(DESCRIPTION=",
#   "(ADDRESS=(PROTOCOL=tcp)(HOST=", host, ")(PORT=", port, "))",
#   "(CONNECT_DATA=(SID=", sid, ")))", sep = "")
# 
# conn_pce <- dbConnect(drv, username =  dbname = connect.string, prefetch = FALSE,
#                       bulk_read = 1000L, stmt_cache = 0L, external_credentials = FALSE,
#                       sysdba = FALSE)
# 
# sqldf(paste("select from",cust_pred_curr12,"", sep=" "))
# 
# qry = sprintf("select
#      debtornum,
#      trim(firstnames) || ' ' || trim(surname) full_name
#      ,addressee
#      from
#      energydb.pm_consumer where debtornum in (select CUSTOMER_ID from %s)", BE_RENEWALS)
# 
# cust_pred_curr13 <- dbGetQuery(conn_pce, qry)
# dbDisconnect(conn_pce)

# peace_customer_name <- paste0("select
#     debtornum,
#     trim(firstnames) || ' ' || trim(surname) full_name
#     ,addressee
#     from
#     energydb.pm_consumer where debtornum in")
# 
# cust_pred_curr13 <- sqldf("SELECT *
#              FROM cust_pred_curr12 as l
#              LEFT JOIN peace_customer_name as r
#              on l.CUSTOMER_ID = r.DEBTORNUM")


# 1GBP = 1.147613 EUR - Yearly Average Rate for Oct 2022
BE_RENEWALS <- BE_RENEWALS %>% mutate(REV_EST=ifelse(JURISDICTION=="NI",REV_EST,round((REV_EST*1.147613),2)))
BE_RENEWALS <- BE_RENEWALS %>% mutate(CLVSCORE=ifelse(JURISDICTION=="NI",CLVSCORE,round((CLVSCORE*1.147613),2)))
BE_RENEWALS$REVENUE_AT_RISK <- (BE_RENEWALS$vote * BE_RENEWALS$CLVSCORE)
BE_RENEWALS$REVENUE_AT_RISK <- round(BE_RENEWALS$REVENUE_AT_RISK, 2)
BE_RENEWALS$ANALYTICS_CREDIT_OUTCOME <- ifelse(as.numeric(BE_RENEWALS$TOT_OVERDUE_AMT)  %in% c(0) | is.na(BE_RENEWALS$TOT_OVERDUE_AMT) , "OK", "DEBT_OS")

BE_RENEWALS <- BE_RENEWALS %>% mutate(ANALYTICS_OT_OUTCOME = 
                                        ifelse(BE_RENEWALS$DG %in% c("DG6", "DG1", "DG2", "DG7"), "OT", 
                                               ifelse(BE_RENEWALS$DG == "DG5" & 
                                                        as.numeric(BE_RENEWALS$PROFILE) == 0, "OT", 
                                                      ifelse(BE_RENEWALS$UTILITY_TYPE_CODE == "G", "OT",
                                                             ifelse(BE_RENEWALS$DG %in% c("T011", "T021", "T035", "T101", 
                                                                                          "T503", "T502", "T507", "T202",
                                                                                          "T022", "T053"),"OT", 
                                                                    ifelse((BE_RENEWALS$DG == "T031" | BE_RENEWALS$DG == "T032" |
                                                                              BE_RENEWALS$DG == "T033" | BE_RENEWALS$DG == "T034" |
                                                                              BE_RENEWALS$DG == "T041" | BE_RENEWALS$DG == "T042" |
                                                                              BE_RENEWALS$DG == "T043") ,"LETTER", 
                                                                           ifelse(((BE_RENEWALS$JURISDICTION == "ROI") & (as.numeric(BE_RENEWALS$num_all_mprns) > 4) | (as.numeric(BE_RENEWALS$NUM_MPRNS_THIS_MONTH) > 4)) , "OT", "NO OUTCOME")))))))

# setwd("C:/Users/AW92211/OneDrive - SSE PLC/R_Code")


# BE_RENEWALS_NO_OT <- read.csv('NO_OUTCOME_JAN.csv')
# 
# # BE_RENEWALS_NO_OT <- filter(BE_RENEWALS, ANALYTICS_OT_OUTCOME %in% c("NA"))
# 
# # summary(BE_RENEWALS_NO_OT$REVENUE_AT_RISK)
# 
# 
# write.csv(BE_RENEWALS, paste0('BE_RENEWALS_RENEWAL_ACTION',rep_month,'_',day_month, '.csv'), row.names = F )
# 
# # summary(BE_RENEWALS_RENEWAL_ACTION$REVENUE_AT_RISK)

# setwd("C:/Users/AW92211/OneDrive - SSE PLC/R_Code")
# BE_RENEWALS_NO_OT <- read.csv('BE_RENEWALS_FEBRUARY_2021.csv')

# setwd("C:/Users/AW92211/OneDrive - SSE PLC/BE_Value_Model/Outputs")

# write.csv(BE_RENEWALS, paste0('BE_RENEWALS.csv'), row.names = F )

BE_RENEWALS_FIXED_ACTION <- filter(BE_RENEWALS, !ANALYTICS_OT_OUTCOME %in% c("NO OUTCOME"))

BE_ACTION <- filter(BE_RENEWALS, ANALYTICS_OT_OUTCOME %in% c("NO OUTCOME"))
BE_ACTION <- BE_ACTION %>% mutate(ANALYTICS_BIN_ACTION = ifelse(BE_ACTION$REVENUE_AT_RISK %in% c(NA), "NO ACTION",
                                                                ifelse(BE_ACTION$REVENUE_AT_RISK < 100, "NO ACTION", "ACTION")))

BE_RENEWALS_NO_ACTION <- filter(BE_ACTION, ANALYTICS_BIN_ACTION %in% c("NO ACTION"))
BE_RENEWALS_RENEWAL_ACTION <- filter(BE_ACTION, ANALYTICS_BIN_ACTION %in% c("ACTION"))

# write.csv(BE_RENEWALS_FIXED_ACTION, paste0('BE_RENEWALS_FIXED_ACTION.csv'), row.names = F )
# write.csv(BE_RENEWALS_NO_ACTION, paste0('BE_RENEWALS_NO_ACTION.csv'), row.names = F )

MIN <- round(min(BE_RENEWALS_RENEWAL_ACTION$REVENUE_AT_RISK) - 1,2)
FIRST_QR <-  round(quantile(BE_RENEWALS_RENEWAL_ACTION$REVENUE_AT_RISK, 0.25, names = FALSE),2)
MEDIAN <-  round(quantile(BE_RENEWALS_RENEWAL_ACTION$REVENUE_AT_RISK, 0.50, names = FALSE),2)
THIRD_QR <-  round(quantile(BE_RENEWALS_RENEWAL_ACTION$REVENUE_AT_RISK, 0.75, names = FALSE),2)
MAX <- round(max(BE_RENEWALS_RENEWAL_ACTION$REVENUE_AT_RISK) + 1,2)
# MAX <- round(8498.89+1,2) - QA CHECK POINT - MANUALLY CHANGE MAX TO SIGNIFICANT VALUE
#                             IF MAX IS NOT SIGNIFICANT/TOO LARGE

print(MIN)
print(FIRST_QR)
print(MEDIAN)
print(THIRD_QR)
print(MAX)

TOP_6 <- sprintf("[%1.2f, %1.2f]", THIRD_QR, MAX)
NEXT_6 <- sprintf("[%1.2f, %1.2f)", MEDIAN, THIRD_QR)
LETTER <- sprintf("[%1.2f, %1.2f)", MIN, MEDIAN)

library(mltools)
BE_RENEWALS_RENEWAL_ACTION$WEIGHT_OUTCOME <- bin_data(BE_RENEWALS_RENEWAL_ACTION$REVENUE_AT_RISK,
                                                      bins=c(MIN, MEDIAN, THIRD_QR, MAX), binType = "explicit")
# BE_Comparison$WEIGHT <- bin_data(BE_Comparison$REVENUE_AT_RISK, bins=100, binType = "quantile")
# CLV_WEIGHT_TABLE <- BE_RENEWALS_RENEWAL_ACTION %>%
#   count(WEIGHT_OUTCOME) %>%
#   rename(COUNT_MPRN = n)

index <- c(LETTER, NEXT_6, TOP_6)
values <- c("RENEWAL LETTER", "RENEWAL LETTER (NEXT 6)", "RENEWAL LETTER (TOP 6)")
BE_RENEWALS_RENEWAL_ACTION$ANALYTICS_BIN_OUTCOME <- values[match(BE_RENEWALS_RENEWAL_ACTION$WEIGHT_OUTCOME, index)]

# write.csv(BE_RENEWALS_RENEWAL_ACTION, paste0('BE_RENEWALS_RENEWAL_ACTION.csv'), row.names = F )

# Combine DataFrames to produce single output file
library(gtools)
BE_RENEWALS_FINAL <- smartbind(BE_RENEWALS_FIXED_ACTION, BE_RENEWALS_NO_ACTION, BE_RENEWALS_RENEWAL_ACTION)
BE_RENEWALS_FINAL %>%
  mutate(NAME = coalesce(NAME,POSTAL_ADDRESS_NAME)) #works
BE_RENEWALS_FINAL %>%
  mutate(POSTAL_ADDRESS_NAME = coalesce(POSTAL_ADDRESS_NAME,NAME)) #works

##### save BE Renewals RAW file
write.csv(BE_RENEWALS_FINAL, paste0("BE_RENEWALS_RAW_",rep_month,"_",rep_year,".csv"), row.names = F )

#################################################################################################
########### Automatically calculate the ANALYTICS_RENEWAL_OUTCOME ###############################
#################################################################################################

for( i in rownames(BE_RENEWALS_FINAL))
  BE_RENEWALS_FINAL[i, "ANALYTICS_RENEWAL_OUTCOME"] <- ifelse(BE_RENEWALS_FINAL[i, "ANALYTICS_OT_OUTCOME"] !='NO OUTCOME', BE_RENEWALS_FINAL[i, "ANALYTICS_OT_OUTCOME"],
                                                              ifelse((BE_RENEWALS_FINAL[i, "ANALYTICS_OT_OUTCOME"] =='NO OUTCOME' & BE_RENEWALS_FINAL[i, "ANALYTICS_BIN_ACTION"] == 'NO ACTION'),BE_RENEWALS_FINAL[i, "ANALYTICS_BIN_ACTION"],
                                                                     ifelse((BE_RENEWALS_FINAL[i, "ANALYTICS_OT_OUTCOME"]=='NO OUTCOME' & BE_RENEWALS_FINAL[i, "ANALYTICS_BIN_ACTION"] == 'ACTION'), BE_RENEWALS_FINAL[i, "ANALYTICS_BIN_OUTCOME"], "NA")))

#################################################################################################
########### Fix Customer Names using PEACE data #################################################
#################################################################################################


# Split customer IDs into batches of 1000
id_batches <- split(unique(BE_RENEWALS_FINAL$CUSTOMER_ID), ceiling(seq_along(unique(BE_RENEWALS_FINAL$CUSTOMER_ID))/1000))

# Create an empty dataframe to store the results
names_data <- data.frame()

# Loop through each batch of IDs and retrieve the data
for (batch in id_batches) {
  # Create a string of comma-separated customer IDs for the current batch
  customer_ids <- paste0("'", batch, "'", collapse = ",")
  
  # Modify the SQL query to use the customer IDs for the current batch
  sql_text <- paste0("SELECT debtornum,
                      TRIM(firstnames) || ' ' || TRIM(surname) AS full_name,
                      addressee
                      FROM energydb.pm_consumer
                      WHERE debtornum IN (", customer_ids, ")")
  
  # Execute the query and append the results to the names_data dataframe
  batch_data <- dbGetQuery(conn_pce, sql_text)
  names_data <- rbind(names_data, batch_data)
}



sql_text

drv <- dbDriver("Oracle")
host <- "dubla055.airtricity.com"
port <- "1526"
sid <- "pceprdr"
connect.string <- paste(
  "(DESCRIPTION=",
  "(ADDRESS=(PROTOCOL=tcp)(HOST=", host, ")(PORT=", port, "))",
  "(CONNECT_DATA=(SID=", sid, ")))", sep = "")

conn_pce <- dbConnect(drv, username =  dbname = connect.string, prefetch = FALSE,
                      bulk_read = 1000L, stmt_cache = 0L, external_credentials = FALSE,
                      sysdba = FALSE)

names_data <- dbGetQuery(conn_pce, sql_text)

dat_renewals_join <- sqldf("SELECT *
             FROM BE_RENEWALS_FINAL as l
             LEFT JOIN names_data as r
             on l.CUSTOMER_ID = r.DEBTORNUM")

dat_renewals = dat_renewals_join[,-c(which(colnames(dat_renewals_join)=='DEBTORNUM'),
                                     which(colnames(dat_renewals_join)=='NAME'),
                                     which(colnames(dat_renewals_join)=='ADDRESSEE'))]
dat_renewals$FULL_NAME <- trimws(dat_renewals$FULL_NAME)

dat_renewals_weight_outcome <- select(dat_renewals, JURISDICTION,	BE_SEGMENT,	CUSTOMER_ID,	FULL_NAME,	POSTAL_ADDRESS_NAME,	POSTAL_ADDRESS_LINE1,	POSTAL_ADDRESS_LINE2,	POSTAL_ADDRESS_LINE3,	
                                      POSTCODE,	EMAIL,	PHONE,	PREMISE_ID,	UTILITY_TYPE_CODE,	MPRN,	NUM_MPRNS_THIS_MONTH,	DG,	PROFILE,	CATGRY,	AVG_YEARLY_CONSUMPTION,	EAC,	
                                      CONTRACT_START_DATE,	CONTRACT_END_DATE,	AGED_DEBT_INSERT_DATE,	ACCOUNT_GROUP_NAME,	TOT_OVERDUE_AMT,	MAX_OVERDUE_DURATION,	AMT_MAX_DURATION,	
                                      STATUS_DESCRIPTION,	ARRANGE_DUE_DATE,	ARRANGE_AMOUNT_DUE,	ARRANGE_AMOUNT_PAID,	ACTIVE_ARRANGEMENT,	CLASSIFICATION_CODE,	Contract_end_date_exists,	
                                      Contract_end_Month_Year,	Consumption,	boosting,	lasso,	random_forest,	svm,	vote,	vote_by_consumption,	vote_consupmtion_above_threshold,	
                                      Churn_Updated_Date,	num_all_mprns, CLVSCORE,	REV_EST,	Value_Updated_Date,	DECILE,	PERCENTILE,	PERCENTAGE_PROFITABILITY,	REVENUE_AT_RISK,	
                                      ANALYTICS_CREDIT_OUTCOME, WEIGHT_OUTCOME, ANALYTICS_RENEWAL_OUTCOME)

##### save BE Renewals file with WEIGHT OUTCOME
write.csv(dat_renewals_weight_outcome, paste0("BE_RENEWALS_QA_",rep_month,"_",rep_year,".csv"), row.names = F )

dat_renewals_final <- select(dat_renewals, JURISDICTION,	BE_SEGMENT,	CUSTOMER_ID, FULL_NAME,	POSTAL_ADDRESS_NAME,	POSTAL_ADDRESS_LINE1,	POSTAL_ADDRESS_LINE2,	POSTAL_ADDRESS_LINE3,	
                             POSTCODE,	EMAIL,	PHONE,	PREMISE_ID,	UTILITY_TYPE_CODE,	MPRN,	NUM_MPRNS_THIS_MONTH,	DG,	PROFILE,	CATGRY,	AVG_YEARLY_CONSUMPTION,	EAC,	
                             CONTRACT_START_DATE,	CONTRACT_END_DATE,	AGED_DEBT_INSERT_DATE,	ACCOUNT_GROUP_NAME,	TOT_OVERDUE_AMT,	MAX_OVERDUE_DURATION,	AMT_MAX_DURATION,	
                             STATUS_DESCRIPTION,	ARRANGE_DUE_DATE,	ARRANGE_AMOUNT_DUE,	ARRANGE_AMOUNT_PAID,	ACTIVE_ARRANGEMENT,	CLASSIFICATION_CODE,	Contract_end_date_exists,	
                             Contract_end_Month_Year,	Consumption,	boosting,	lasso,	random_forest,	svm,	vote,	vote_by_consumption,	vote_consupmtion_above_threshold,	
                             Churn_Updated_Date,	num_all_mprns, CLVSCORE,	REV_EST,	Value_Updated_Date,	DECILE,	PERCENTILE,	PERCENTAGE_PROFITABILITY,	REVENUE_AT_RISK,	
                             ANALYTICS_CREDIT_OUTCOME,	ANALYTICS_RENEWAL_OUTCOME)

##### save BE Renewals FINAL file
write.csv(dat_renewals_final, paste0("BE_RENEWALS_FINAL_",rep_month,"_",rep_year,".csv"), row.names = F )



################################################################################################
############### END ############################################################################
################################################################################################
