echo "Creating the database"

createdb -h postgres -U postgres -p 5432 billingDW

echo "Downloading the data files"
wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0260EN-SkillsNetwork/labs/Setting%20up%20a%20staging%20area/billing-datawarehouse.tgz

echo "Extracting files"
tar -xvzf billing-datawarehouse.tgz

echo "Creating schema"

psql  -h postgres -U postgres -p 5432 billingDW < star-schema.sql

echo "Loading data"

psql  -h postgres -U postgres -p 5432 billingDW < DimCustomer.sql

psql  -h postgres -U postgres -p 5432 billingDW < DimMonth.sql

psql  -h postgres -U postgres -p 5432 billingDW < FactBilling.sql

echo "Finished loading data"

echo "Verifying data"

psql  -h postgres -U postgres -p 5432 billingDW < verify.sql

echo "Successfully setup the staging area"