#! /bin/bash

# create a datestamped filename for the raw wttr data:
today=$(date +%Y%m%d)
weather_report=raw_data_$today

# download today's weather report from wttr.in:
city=Casablanca
curl wttr.in/$city --output $weather_report

# use command substitution to store the current day, month, and year in corresponding shell variables:
hour=$(TZ='Morocco/Casablanca' date -u +%H) 
day=$(TZ='Morocco/Casablanca' date -u +%d) 
month=$(TZ='Morocco/Casablanca' date +%m)
year=$(TZ='Morocco/Casablanca' date +%Y)

# extract all lines containing temperatures from the weather report and write to file
grep °C $weather_report > temperatures.txt

# extract the current temperature 
obs_tmp=$(head -1 temperatures.txt | tr -s " " | xargs | rev | cut -d " " -f2 | rev)

# extract the forecast for noon tomorrow
fc_temp=$(head -3 temperatures.txt | tail -1 | tr -s " " | xargs | cut -d "C" -f2 | rev | cut -d " " -f2 |rev)

# create a tab-delimited record
# recall the header was created as follows:
# header=$(echo -e "year\tmonth\tday\thour_UTC\tobs_tmp\tfc_temp")
# echo $header>rx_poc.log

record=$(echo -e "$year\t$month\t$day\t$obs_tmp\t$fc_temp")
# append the record to rx_poc.log
echo $record>>rx_poc.log
