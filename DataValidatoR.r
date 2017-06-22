library(data.table)
library(plyr)
library(futile.logger)



#create a log file with that particular date of run as filename
logFile <-paste(Sys.Date(),".log")

flog.appender(appender.tee(logFile))

flog.info("*******************************************")
flog.info("   Started Running the Validation Script")
flog.info("*******************************************")

readCSV <- function(MillCSVFile, VerticaCSVFile)
{
  flog.info("Started Reading Millennium CSVs")
  claims.millennium <<- fread(MillCSVFile, colClasses ="character")
  claims.vertica <<- fread(VerticaCSVFile, colClasses ="character")
  flog.info("Finished Reading Millennium CSVs")
}

writeIntoCSV <- function(DF, fileName)
{
  flog.info("Started writing ", DF, " into ", fileName)
  write.csv(DF, file = fileName)
  flog.info("Finished writing ", DF, " into ", fileName)
}

duplicateCheck <- function(millData, verticaData)
{
  flog.info("Checking for Duplicated Records in Millennium")
  millDuplicatedRecord <<- millData[duplicated(millData),]
  flog.info(paste("", nrow(millDuplicatedRecord) , " Duplicate records were found in Millennium"))
  
  flog.info("Checking for Duplicated Records in Vertica")
  verticaDuplicatedRecord <<- verticaData[duplicated(verticaData),]
  flog.info(paste("", nrow(verticaDuplicatedRecord), " Duplicate records were found in Vertica"))
}



normalizeMillennium <- function()
{
  flog.info("Started cleansing Millennium Data")
  #list of columns to be dropped in millennium
  millennium_primaryDisplay_drops <- c("first_submitted_claim_indicator","financial_class_primary_display","status_primary_display","status_reason_primary_display","medical_service_primary_display","media_type_primary_display","encounter_type_primary_display","encounter_type_class_primary_display")
  millennium_additional_drops <- c("patient_mrn","47","encounter_id")
  
  claims.millennium <- as.data.frame(claims.millennium)
  #rename columns to remove extra characters in the beginning 
  names(claims.millennium) <- substring(names(claims.millennium), 2)
  
  flog.info("Renaming and Dropping Unnecessary columns")
  #drop the columns 
  claims.millennium <- claims.millennium[ , !(names(claims.millennium) %in% millennium_primaryDisplay_drops)]
  claims.millennium <- claims.millennium[ , !(names(claims.millennium) %in% millennium_additional_drops)]
  
  
  #Standardization of data in millennium
  claims.millennium$claim_uid <- gsub("-",",",claims.millennium$claim_uid)
  claims.millennium$attending_physician <- gsub("-",",",claims.millennium$attending_physician)
  claims.millennium$admitting_physician <- gsub("-",",",claims.millennium$admitting_physician)
  #claims.millennium$encounter_balance_amount <- gsub("-",",",claims.millennium$encounter_balance_amount)
  claims.millennium$nurse_unit_location <- gsub("-",",",claims.millennium$nurse_unit_location)
  claims.millennium$patient_full_name <- gsub("-",",",claims.millennium$patient_full_name)
  claims.millennium$interim_bill_flag[is.na(claims.millennium$interim_bill_flag)] <- 0
  #claims.millennium$claim_balance <- as.integer(claims.millennium$claim_balance)
  claims.millennium$claim_balance[is.na(claims.millennium$claim_balance)] <- 0
  claims.millennium$encounter_balance_amount[is.na(claims.millennium$encounter_balance_amount)] <- 0
  claims.millennium$encounter_balance_amount <- abs(as.integer(round(as.numeric(claims.millennium$encounter_balance_amount))))
  
  
  #order records based on claim_uid
  flog.info("Order the Millennium data based on Claim UIDs")
  claims.millennium <- as.data.table(claims.millennium[order(claims.millennium$claim_uid),])
  
  setnames(claims.millennium,"earliest_svc_dt","earliest_service_date")
  setnames(claims.millennium,"latest_svc_dt","latest_service_date")
  setnames(claims.millennium,"earliest_posted_dt","earliest_posted_date")
  setnames(claims.millennium,"latest_posted_dt","latest_posted_date")
  
  flog.info("Finished cleansing Millennium Data")
  return(claims.millennium)
}

normalizeVertica <- function()
{
  flog.info("Started Normalizing Vertica Data")
  claims.vertica <- as.data.frame(claims.vertica)
  
  vertica_primaryDisplay_drops <- c("first_submitted_claim_indicator","financial_class_primary_display","status_primary_display","status_reason_primary_display", "medical_service_primary_display", "media_type_primary_display", "encounter_type_primary_display", "encounter_type_class_primary_display")
  
  vertica_additional_drops <- c("patient_mrn","facility","facility_id","encounter_type_class_display")
  
  #modify data.frame to have only proper columns
  claims.vertica <- claims.vertica[ , !(names(claims.vertica) %in% vertica_primaryDisplay_drops)]
  claims.vertica <- claims.vertica[ , !(names(claims.vertica) %in% vertica_additional_drops)]
  
  #Destandardize data in vertica
  flog.info("Removing the appended Partition IDs ")
  claims.vertica$claim_uid <- gsub("_[0-9]+","",gsub("/partition:17e9b39b-4bfa-4cab-b116-d53a8e414c76/clmbll:", "",claims.vertica$claim_uid))
  claims.vertica$health_plan_balance_id <- gsub("17e9b39b-4bfa-4cab-b116-d53a8e414c76:", "",claims.vertica$health_plan_balance_id)
  claims.vertica$financial_balance_id <- gsub("17e9b39b-4bfa-4cab-b116-d53a8e414c76:", "",claims.vertica$financial_balance_id)
  claims.vertica$admitting_physician_id <- gsub("17e9b39b-4bfa-4cab-b116-d53a8e414c76:", "",claims.vertica$admitting_physician_id)
  claims.vertica$attending_physician_id <- gsub("17e9b39b-4bfa-4cab-b116-d53a8e414c76:", "",claims.vertica$attending_physician_id)
  #claims.vertica$interim_bill_flag <- gsub("", "0",claims.vertica$interim_bill_flag)
  claims.vertica$interim_bill_flag <- gsub("", "0",claims.vertica$interim_bill_flag)
  claims.vertica$interim_bill_flag[is.na(claims.vertica$interim_bill_flag)] <- 0
  claims.vertica[is.na(claims.vertica)] <- "NULL"
  
  #add interim flag one
  claims.vertica$nurse_unit_location <- gsub("-",",",claims.vertica$nurse_unit_location)
  claims.vertica$patient_full_name <- gsub("-",",",claims.vertica$patient_full_name)
  claims.vertica$attending_physician <- gsub("-",",",claims.vertica$attending_physician)
  #claims.vertica$claim_balance = claims.vertica$claim_balance
  claims.vertica$encounter_balance_amount <- abs(as.integer(round(as.numeric(claims.vertica$encounter_balance_amount))))
  
  #order the dataset in ascending order of claim_uid
  claims.vertica <- as.data.table(claims.vertica[order(claims.vertica$claim_uid),])
  flog.info("Finished cleansing Vertica Data")
  return(claims.vertica)
}

removeUnmatchingIDs <- function()
{
  #list of claim_uid's in Millennium but not in Vertica 
  flog.info("Started removing unmatching IDs from both datasets")
  
  mill_extra_claim_uids <- as.list(setdiff(claims.millennium$claim_uid,claims.vertica$claim_uid))
  claims.millennium <<- claims.millennium[!(claims.millennium$claim_uid %in% mill_extra_claim_uids),]
  
  flog.info(paste("Millennium has " , length(mill_extra_claim_uids), " Extra Claim IDs which are not there in Vertica") )
  flog.info("Missing Claim IDs from Vertica is written into 'vertica_missing_claim_uids.csv'")
  writeIntoCSV(mill_extra_claim_uids, "vertica_missing_claim_uids.csv")
  
  #list of claim_uids in Vertica but not in millennium
  vertica_extra_claim_uids <- as.list(setdiff(claims.vertica$claim_uid,claims.millennium$claim_uid))
  claims.vertica <<- claims.vertica[!(claims.vertica$claim_uid %in% vertica_extra_claim_uids),]
  
  flog.info(paste("Vertica has " , length(vertica_extra_claim_uids), " Extra Claim IDs which are not there in Millennium") )
  flog.info("Missing Claim IDs from Millennium is written into 'millennium_missing_claim_uids.csv'")
  writeIntoCSV(vertica_extra_claim_uids, "millennium_missing_claim_uids.csv")
  
  
  flog.info("Finished removing unmatching IDs from both datasets")
}


#catches differences between the two datasources --deprecated
differenceCatcher <- function(MillDF,VerticaDF)
{
  if (!isTRUE(all.equal(MillDF,VerticaDF)))
  {
    mismatches <- as.character(paste(which(MillDF != VerticaDF), collapse = "-"))
    mismatchesDF <- as.data.frame(as.integer(do.call(rbind, strsplit(mismatches, "-", fixed=TRUE))))
    mismatches.mods <- as.data.frame(mismatchesDF %% nrow(MillDF) + 1)
    mismatches.quotient <- as.data.frame(mismatchesDF %/% nrow(MillDF) )
    mismatchesDF <- as.data.frame(append(mismatches.mods,mismatches.quotient))
    #mismatchesDF <- as.data.frame(append(mismatchesDF,mismatches.mods))
    #mismatchesDF <- as.data.frame(append(mismatchesDF,mismatches.quotient))
    colnames(mismatchesDF) <- c("Row","Column")
    
    mismatchesDF <- mismatchesDF[order(mismatchesDF$Row),]
    
    claims.millennium <- as.data.frame(claims.millennium)
    claims.vertica <- as.data.frame(claims.vertica)
    
    return(mismatchesDF)
    #cat("Error! The Millennium and Vertica data does not match at the following columns: ", mismatches )
  } 
  else 
  {
    print("Data Matches Successfully!!")
  }
}

#shows differences between millennium and Vertica
displayDifferences <- function(millDF, verticaDF)
{
  flog.info("Starting comparing the dataset")
  
  millDF.mat <- as.matrix(millDF)
  verticaDF.mat <- as.matrix(verticaDF)
  
  #holds the truthness of the match
  logicalMapTable <- (millDF.mat == verticaDF.mat)
  
  
  
  #to display all the claim_IDs
  logicalMapTable[,1] <- FALSE
  
  #fill all matched values with -
  verticaDF.mat[logicalMapTable] <- "--"
  
  #add a merge ere
  flog.info("Finished comparing the dataset")
  
  flog.info("Writing result of comparison to results.csv")
  
  writeIntoCSV(as.data.frame(verticaDF.mat), "results.csv")
  
  return(as.data.frame(verticaDF.mat))
}

#analyzes the difference matrix
analyze_output <- function(diffDF)
{
  flog.info("Analyzing the Differences")
  
  count.non.matchings.per.column <- ldply(diffDF, function(c) sum(c != "--"))
  
  count.non.matchings.per.column <<- count.non.matchings.per.column[order(count.non.matchings.per.column$V1),]
  
  return(count.non.matchings.per.column)
}


#function calls
#*******************************************************************************************************
print("Reading CSV")
system.time(readCSV("LoganRegional_Claims_Mill.csv","LoganRegional_Claims_Vertica.csv"))

duplicateCheck(claims.millennium, claims.vertica)

print("Standardizing Millennium Data")
system.time(claims.millennium <<- normalizeMillennium())

print("Standardizing Vertica Data")
system.time(claims.vertica <<- normalizeVertica())

print("Removing Mismatching Exisiting ID's")
system.time(removeUnmatchingIDs())

print("Show Difference Between two Datasources")
system.time(mismatchedVertica <<- displayDifferences(claims.millennium,claims.vertica))

print("Analyzing Results")
system.time(analysis <<- analyze_output(mismatchedVertica))

flog.info("Finished Running the Script")