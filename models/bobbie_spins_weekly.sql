  WITH bobbie_data AS (
	select "Reporting_Level" PRODUCT_LEVEL,"Subcategory" SUBCATEGORY,"Brand" BRAND, "Product_Universe" PRODUCT_UNIVERSE , "UPC" UPC, "Description" PRODUCT_DESCRIPTION 
			,"TIME_PERIOD_END_DATE" TIME_PERIOD_END_DATE, "Geography" GEOGRAPHY, "POSITIONING_GROUP" POSITIONING_GROUP , "Category" CATEGORY, "COMPANY" COMPANY
			, "PRODUCT_TYPE" PRODUCT_TYPE,    "LABELED_NON_GMO" LABELED_NON_GMO 
			, "Department" DEPARTMENT, "STORAGE" STORAGE, "UNIT_OF_MEASURE" UNIT_OF_MEASURE
			, "LABELED_ORGANIC" LABELED_ORGANIC, "PACK_COUNT" PACK_COUNT,  "SIZE" "SIZE"
			, case when "TIME_PERIOD"='4 Weeks' then '04 Weeks' else "TIME_PERIOD" end TIME_PERIOD
			, CASE when "TIME_PERIOD"='4 Weeks' then '04W' 
		 	   when "TIME_PERIOD"='12 Weeks' then '12W' 
			   when "TIME_PERIOD"='24 Weeks' then '24W' 
			   when "TIME_PERIOD"='52 Weeks' then '52W' 
			   else "TIME_PERIOD" end as TIME_PERIOD_ABR
			, sum("UNITS"						) as UNITS_RF
			, sum("YAGO_UNITS"					) as UNITS_YA_RF
			, sum(cast(case when "DOLLARS" is null then 0 else "DOLLARS" end  as float)) as DOLLARS_RF
			, sum(cast(case when "YAGO_DOLLARS" is null then 0 else "YAGO_DOLLARS" end as float)) as DOLLARS_YA_RF
			, sum("DOLLARS_PROMO"						) as DOLLARS_PROMO_RF
			, sum("YAGO_DOLLARS_PROMO"					) as DOLLARS_PROMO_YA_RF
			, sum("UNITS_PROMO"							) as UNITS_PROMO_RF
			, sum("YAGO_UNITS_PROMO"					) as UNITS_PROMO_YA_RF
			, sum("BASE_DOLLARS"						) as BASE_DOLLARS_RF
			, sum("YAGO_BASE_DOLLARS"					) as BASE_DOLLARS_YA_RF
			, sum("DOLLARS_SPM"						) as DOLLARS_SPM_RF
			, sum("YAGO_DOLLARS_SPM"					) as DOLLARS_SPM_YA_RF
			, sum("UNITS_SPM"						) as UNITS_SPM_RF
			, sum("YAGO_UNITS_SPM"					) as UNITS_SPM_YA_RF
			, sum(cast("DOLLARS" 					as float))-		sum(cast("BASE_DOLLARS" 			as float)) as INCREMENTAL_SALES_RF
			, sum(cast("YAGO_DOLLARS" 				 as float))-	sum(cast("YAGO_BASE_DOLLARS" 	as float)) as INCREMENTAL_SALES_YA_RF
			, sum("BASE_UNITS"								) as BASE_UNITS_RF
			, sum("YAGO_BASE_UNITS"						) as BASE_UNITS_YA_RF
			, sum("PROMO_BASE_UNITS"					) as BASE_UNITS_PROMO_RF
			, sum("YAGO_PROMO_BASE_UNITS"				) as BASE_UNITS_PROMO_YA_RF
			, sum("PROMO_BASE_DOLLARS"					) as BASE_DOLLARS_PROMO_RF
			, sum("YAGO_PROMO_BASE_DOLLARS"					) as BASE_DOLLARS_PROMO_YA_RF
			, sum("TDP_ANY_MERCH"									) as TDP_ANY_PROMO_RF
			, sum("YAGO_TDP_ANY_MERCH"							) as TDP_ANY_PROMO_YA_RF
			, sum("TDP"									) as TDP_RF
			, sum("YAGO_TDP"							) as TDP_YA_RF
			, sum("EQ_UNITS")					as EQ_UNITS_RF
			, sum("YAGO_EQ_UNITS")					as EQ_UNITS_YA_RF
			, sum("STORE_CNT")					as STORE_CNT_RF
			, sum("YAGO_STORE_CNT")					as STORE_CNT_YA_RF
			, sum("DOLLARS_TPR")					as DOLLARS_TPR_RF
			, sum("YAGO_DOLLARS_TPR")					as DOLLARS_TPR_YA_RF
			, sum("DOLLARS_FEATURE_ONLY")					as DOLLARS_FEATURE_ONLY_RF
			, sum("YAGO_DOLLARS_FEATURE_ONLY")					as DOLLARS_FEATURE_ONLY_YA_RF
			, sum("DOLLARS_DISPLAY_ONLY")					as DOLLARS_DISPLAY_ONLY_RF
			, sum("YAGO_DOLLARS_DISPLAY_ONLY")				as DOLLARS_DISPLAY_ONLY_YA_RF
			, sum("DOLLARS_FEATURE_AND_DISPLAY")			as DOLLARS_FEATURE_AND_DISPLAY_RF
			, sum("YAGO_DOLLARS_FEATURE_AND_DISPLAY")		as DOLLARS_FEATURE_AND_DISPLAY_YA_RF			
			, max("MAX_PCT_ACV"						)     as MAX_PCT_ACV_RF 
			, max("YAGO_MAX_PCT_ACV"				) as MAX_PCT_ACV_YA_RF
			, max("AVG_PCT_ACV"						)     as AVG_PCT_ACV_RF 
			, max("YAGO_AVG_PCT_ACV"				) as YAGO_AVG_PCT_ACV_RF
			, max("MAX_PCT_ACV"	) - max("YAGO_MAX_PCT_ACV"	) as MAX_PCT_ACV_CHG_RF
			, max("TIME_PERIOD_END_DATE"				) as LAST_UPDATE_DATE_RF
			, max("NO_OF_SS"				) as NO_OF_STORES_SELLING_RF
			, max("YAGO_NO_OF_SS"			) as NO_OF_STORES_SELLING_YA_RF
			, max("MAX_PCT_ACV_ANY_MERCH") AS MAX_PCT_ACV_ANY_PROMO_RF
			, max("YAGO_MAX_PCT_ACV_ANY_MERCH") AS MAX_PCT_ACV_ANY_PROMO_YA_RF
			, max("WEIGHT_WEEKS_PROMO") AS WEIGHT_WEEKS_ANY_PROMO_RF
			, max("YAGO_WEIGHT_WEEKS_PROMO") AS WEIGHT_WEEKS_ANY_PROMO_YA_RF
			, case when max("WEEKS_SELLING") is null then 0 else max("WEEKS_SELLING") end  as NUMBER_OF_WEEKS_SELLING_RF
			, case when max("YAGO_WEEKS_SELLING") is null then 0 else max("YAGO_WEEKS_SELLING") end as NUMBER_OF_WEEKS_SELLING_YA_RF
	from {{source('BOBBIE','BOBBIE_WEEKLY')}}--"PUBLIC".BOBBIE_WEEKLY 
	GROUP BY 	"Reporting_Level" ,"Subcategory" ,"Brand" , "Product_Universe"  , "UPC" , "Description"  
				,"TIME_PERIOD_END_DATE" , "Geography" , "POSITIONING_GROUP"  , "Category" , "COMPANY" 
				, "PRODUCT_TYPE" ,    "LABELED_NON_GMO"  
				, "Department" , "STORAGE" , "UNIT_OF_MEASURE" 
				, "LABELED_ORGANIC" , "PACK_COUNT" ,  "SIZE" 
				, case when "TIME_PERIOD"='4 Weeks' then '04 Weeks' else "TIME_PERIOD" end 
				, CASE when "TIME_PERIOD"='4 Weeks' then '04W' 
			 	   when "TIME_PERIOD"='12 Weeks' then '12W' 
				   when "TIME_PERIOD"='24 Weeks' then '24W' 
				   when "TIME_PERIOD"='52 Weeks' then '52W' 
				   else "TIME_PERIOD" end 
)
SELECT * FROM bobbie_data
