\echo "Checking row in DimMonth Table"
select count(*) from "DimMonth";
\echo "Checking row in DimCustomer Table"
select count(*) from "DimCustomer";
\echo "Checking row in FactBilling Table"
select count(*) from "FactBilling";