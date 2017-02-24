###############################################
## Prepare historical FARS data for Analysis ##
###############################################

## Load libraries
install.packages('data.table')
install.packages('RPostgreSQL')
install.packages('dplyr')
library(data.table)
library(RPostgreSQL)
library(dplyr)

## Connect to PostgreSQL database on Compose for 2014 FARS data
## Note:  I downloaded the 2014 FARS data, created tables in PostgreSQL, and am 
## accessing them here.  You can also simply download the vehicle and accident
## files for 2014 in their native format and work from there.
drv <- dbDriver("PostgreSQL")
conn <- dbConnect(drv, 
                  dbname = "yourdb", 
                  host = "yourhost", 
                  port = "portnumber", 
                  user = "user", 
                  password = password)

## View tables in PostgreSQL database
dbListTables(conn)

## Load PostgreSQL data into R DataFrame
accident.df <- dbGetQuery(conn, "SELECT * FROM accident")
vehicle.df <- dbGetQuery(conn, "SELECT * FROM vehicle")

###############################################################################
## After downloading all years of accident and vehicle data from FARS,       ##
## I am going to merge them into one table.  The trouble here is that column ##
## names do not always match, so we'll have to work around that by matching  ##
## column names to those found in the 2014 dataset.                          ##
###############################################################################

## Create list of dataframes, drop first column of row names, bind all
## rows to make one giant dataframe, then convert column names to lower case.

## Accidents
accdata <- list()
acclist <- list.files(pattern="*.csv")
for (i in 1:length(acclist)) 
  accdata[[i]] <- read.csv(acclist[i], stringsAsFactors = F, header = T)[,-1]
accident.all <- rbindlist(accdata, fill = T, use.names = T)
accident.all <- select(accident.all, -c(50,51)) ## Remove duplicate columns
names(accident.all) <- tolower(names(accident.all))

## Subset columns found in 2014 FARS data
accnames <- names(accident.all) %in% names(accident.df)
accnames <- colnames(accident.all)[accnames]
accnames <- which(colnames(accident.all) %in% accnames)
accident.all <- select(accident.all, accnames)

## Write to disk
write.csv(accident.all, file = 'accident_all.csv', row.names = F)

## Vehicles
vehdata <- list()
vehlist <- list.files(pattern="*.csv")
for (i in 1:length(vehlist)) 
  vehdata[[i]] <- read.csv(vehlist[i], stringsAsFactors = F, header = T)[,-1]
vehicle.all <- rbindlist(vehdata, fill = T, use.names = T)
names(vehicle.all) <- tolower(names(vehicle.all))

## Subset columns found in 2014 FARS data
vehnames <- names(vehicle.all) %in% names(vehicle.df)
vehnames <- colnames(vehicle.all)[vehnames]
vehnames <- which(colnames(vehicle.all) %in% vehnames)
vehicle.all <- select(vehicle.all, vehnames)

## Write to disk
write.csv(vehicle.all, file = 'vehicle_all.csv', row.names = F)

## Persons
perdata <- list()
perlist <- list.files(pattern="*.csv")
for (i in 1:length(perlist)) 
  perdata[[i]] <- read.csv(perlist[i], stringsAsFactors = F, header = T)[,-1]
person.all <- rbindlist(perdata, fill = T)
names(person.all) <- tolower(names(person.all))
write.csv(person.all, file = 'person_all.csv', row.names = F)

## Load each unique file as DF
dframes <- list.files(pattern="*.csv")
for (i in 1:length(dframes)){ 
  assign(dframes[i], read.csv(dframes[i], stringsAsFactors = F, header = T)[,-1])
  names(dframes[i]) <- tolower(names(dframes[i]))
}

## List of df's in environment
dfs <- Filter(function(x) is(x, "data.frame"), mget(ls()))

## Names to lowercase
for(i in 1:length(dfs)){
  colnames(dfs[[i]]) <- tolower(colnames(dfs[[i]]))
}

## Unlist to global environment
list2env(dfs, .GlobalEnv)

## Modeling dataframes for each year
mod_dat75 <- left_join(accident_75.csv,
                       vehicle_75.csv, by = "st_case")

mod_dat76 <- left_join(accident_76.csv,
                       vehicle_76.csv, by = "st_case")

mod_dat77 <- left_join(accident_77.csv,
                       vehicle_77.csv, by = "st_case")

mod_dat78 <- left_join(accident_78.csv,
                       vehicle_78.csv, by = "st_case")

mod_dat79 <- left_join(accident_79.csv,
                       vehicle_79.csv, by = "st_case")

mod_dat80 <- left_join(accident_80.csv,
                       vehicle_80.csv, by = "st_case")

mod_dat81 <- left_join(accident_81.csv,
                       vehicle_81.csv, by = "st_case")

mod_dat82 <- left_join(accident_82.csv,
                       vehicle_82.csv, by = "st_case")

mod_dat83 <- left_join(accident_83.csv,
                       vehicle_83.csv, by = "st_case")

mod_dat84 <- left_join(accident_84.csv,
                       vehicle_84.csv, by = "st_case")

mod_dat85 <- left_join(accident_85.csv,
                       vehicle_85.csv, by = "st_case")

mod_dat86 <- left_join(accident_86.csv,
                       vehicle_86.csv, by = "st_case")

mod_dat87 <- left_join(accident_87.csv,
                       vehicle_87.csv, by = "st_case")

mod_dat88 <- left_join(accident_88.csv,
                       vehicle_88.csv, by = "st_case")

mod_dat89 <- left_join(accident_89.csv,
                       vehicle_89.csv, by = "st_case")

mod_dat90 <- left_join(accident_90.csv,
                       vehicle_90.csv, by = "st_case")

mod_dat91 <- left_join(accident_91.csv,
                       vehicle_91.csv, by = "st_case")

mod_dat92 <- left_join(accident_92.csv,
                       vehicle_92.csv, by = "st_case")

mod_dat93 <- left_join(accident_93.csv,
                       vehicle_93.csv, by = "st_case")

mod_dat94 <- left_join(accident_94.csv,
                       vehicle_94.csv, by = "st_case")

mod_dat95 <- left_join(accident_95.csv,
                       vehicle_95.csv, by = "st_case")

mod_dat96 <- left_join(accident_96.csv,
                       vehicle_96.csv, by = "st_case")

mod_dat97 <- left_join(accident_97.csv,
                       vehicle_97.csv, by = "st_case")

mod_dat98 <- left_join(accident_98.csv,
                       vehicle_98.csv, by = "st_case")

mod_dat99 <- left_join(accident_99.csv,
                       vehicle_99.csv, by = "st_case")

mod_dat00 <- left_join(accident_00.csv,
                       vehicle_00.csv, by = "st_case")

mod_dat01 <- left_join(accident_01.csv,
                       vehicle_01.csv, by = "st_case")

mod_dat02 <- left_join(accident_02.csv,
                       vehicle_02.csv, by = "st_case")

mod_dat03 <- left_join(accident_03.csv,
                       vehicle_03.csv, by = "st_case")

mod_dat04 <- left_join(accident_04.csv,
                       vehicle_04.csv, by = "st_case")

mod_dat05 <- left_join(accident_05.csv,
                       vehicle_05.csv, by = "st_case")

mod_dat06 <- left_join(accident_06.csv,
                       vehicle_06.csv, by = "st_case")

mod_dat07 <- left_join(accident_07.csv,
                       vehicle_07.csv, by = "st_case")

mod_dat08 <- left_join(accident_08.csv,
                       vehicle_08.csv, by = "st_case")

mod_dat09 <- left_join(accident_09.csv,
                       vehicle_09.csv, by = "st_case")

mod_dat10 <- left_join(accident_10.csv,
                       vehicle_10.csv, by = "st_case")

mod_dat11 <- left_join(accident_11.csv,
                       vehicle_11.csv, by = "st_case")

mod_dat12 <- left_join(accident_12.csv,
                       vehicle_12.csv, by = "st_case")

mod_dat13 <- left_join(accident_13.csv,
                       vehicle_13.csv, by = "st_case")

## As list
moddatlist <- list(mod_dat75, mod_dat76, mod_dat77, mod_dat78, mod_dat79,
                   mod_dat80, mod_dat81, mod_dat82, mod_dat83, mod_dat84
                   , mod_dat85, mod_dat86, mod_dat87, mod_dat88, mod_dat89
                   , mod_dat90, mod_dat91, mod_dat92, mod_dat93, mod_dat94
                   , mod_dat95, mod_dat96, mod_dat97, mod_dat98, mod_dat99
                   , mod_dat00, mod_dat01, mod_dat02, mod_dat03, mod_dat04
                   , mod_dat05, mod_dat06, mod_dat07, mod_dat08, mod_dat09
                   , mod_dat10, mod_dat11, mod_dat12, mod_dat13)

## Merge dataframes in list
mod_dat_all <- rbindlist(moddatlist, fill = T)

## Create key for aggregations
mod_dat_all$key <- paste0(mod_dat_all$st_case, mod_dat_all$year)

## Filter columns
mod_dat_all <- select(mod_dat_all, state.x, st_case, dr_drink, make, deaths, speedrel, 
                      trav_sp, body_typ, mod_year, prev_acc, prev_sus, prev_dwi,
                      prev_spd, dr_hgt, dr_wgt, permvit, county, city, day_week,
                      day.x, month.x, hour.x, year, road_fnc, latitude, longitud, reljct1,
                      typ_int, lgt_cond, weather, fatals, drunk_dr, key)

## mod_dat_all should be the file that is worked with in the Jupyter Notebook


