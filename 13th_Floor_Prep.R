### Madison Hardesty and Jonathan Auerbach
### Preparing Data for 13th Floor Analysis
### 02/20/2024

### Data Sources:
### Property Assessment Data: bit.ly/492AQYi (query filters applied: YEAR == 2023, PERIOD == 3, CONDO_Number not null)
### Property Rolling Sales: https://bit.ly/48oppd1

# merge property assesssment data with sales data
Property_Assessment_Data <-
  read_csv("Property_Valuation_and_Assessment_Data_Tax_Classes_1_2_3_4_20240211.csv", 
           col_types = cols(EASEMENT = col_character(),
                            NOAV  = col_character(),
                            VALREF  = col_character(),
                            REUC_REF  = col_character()))

rolling_sales <- 
  read_csv("NYC_Citywide_Rolling_Calendar_Sales_20240210.csv")

url <- "https://www.nyc.gov/assets/finance/downloads/pdf/rolling_sales/annualized-sales/2021/2021_bronx.xlsx"
download.file(url, destfile = "local.xlsx")
rolling_sales_prev <- read_excel("local.xlsx", skip = 6) %>%
  mutate(BOROUGH = as.numeric(BOROUGH),
         `EASE-MENT` = as.character(`EASE-MENT`)) %>%
  filter(BOROUGH == 0)
colnames(rolling_sales_prev) <- str_replace(colnames(rolling_sales_prev), "\r\n|-", "")

for(year in 2003:2022) {
  for(boro in c("manhattan", "bronx", "brooklyn", "queens", "staten_island")) {
    start <- 4
    ext <- ".xlsx"
    if(year <= 2017) ext = ".xls"
    if(year <= 2019 & boro == "staten_island") {
      boro <- "statenisland"
    }
    if(year == 2009) {
      url <- paste0("https://www.nyc.gov/assets/finance/downloads/pdf/rolling_sales/annualized-sales/",year,"_",boro, ext)
    } else if(year == 2008) {
      url <- paste0("https://www.nyc.gov/assets/finance/downloads/pdf/",
                    paste0("0",as.numeric(str_sub(year, 3, 4)) + 1),
                    "pdf/rolling_sales/sales_",year,"_",boro, ext)
      start <- 4
    } else if(year == 2007) {
      url <- paste0("https://www.nyc.gov/assets/finance/downloads/excel/rolling_sales/sales_",year,"_",boro, ext)
      start <- 4
    } else if(year <= 2006) {
      if(boro == "statenisland") boro <- "si"
      url <- paste0("https://www.nyc.gov/assets/finance/downloads/sales_",boro,"_",str_sub(year, 3, 4), ext)
      start <- 4
    }
    else {
      url <- paste0("https://www.nyc.gov/assets/finance/downloads/pdf/rolling_sales/annualized-sales/", year, "/",year,"_",boro, ext)
      if(year <= 2019) {
        start <- 5} else {
          start <- 7
        }
    }
    download.file(url, destfile = paste0("local",ext))
    rolling_sales_prev_temp <- 
      read_excel(paste0("local",ext), skip = start - 1, guess_max = 1e6) 
    rolling_sales_prev_temp <- rolling_sales_prev_temp[,1:21]
    colnames(rolling_sales_prev_temp) <- colnames(rolling_sales_prev) 
    rolling_sales_prev_temp <- rolling_sales_prev_temp  %>% 
      mutate(BOROUGH = as.numeric(BOROUGH),
             `TAX CLASS AT TIME OF SALE` = as.character(`TAX CLASS AT TIME OF SALE`))
    rolling_sales_prev <- bind_rows(rolling_sales_prev, rolling_sales_prev_temp)
  }
}

sales_by_unit <-
  bind_rows(rolling_sales %>% mutate(`SALE DATE` = as.Date(`SALE DATE`, format = "%m/%d/%Y")), 
            rolling_sales_prev %>% 
              mutate(`SALE DATE` = as.Date(`SALE DATE`),
                     `TAX CLASS AT TIME OF SALE` = as.numeric(`TAX CLASS AT TIME OF SALE`))) %>%
  filter(`SALE PRICE` != 0) %>%
  group_by(BORO = BOROUGH, BLOCK, LOT) %>%
  arrange(`SALE DATE`) %>%
  summarize(sale_price = last(`SALE PRICE`))

Property_Assessment_Data <-
  Property_Assessment_Data %>%
  left_join(sales_by_unit, by = c("BORO", "BLOCK", "LOT")) %>%
  mutate(sale_price = ifelse(sale_price == "NA", NA, sale_price))


# prepare data 
dataNYC <- Property_Assessment_Data %>%
  select(CONDO_Number, BLD_STORY, COOP_APTS, APTNO, BORO, BLOCK, BLD_FRT, BLD_DEP, YRBUILT, GROSS_SQFT, PYMKTTOT, sale_price) %>%
  rename(CONDO_NUMBER = CONDO_Number, MARKET_VALUE = PYMKTTOT, SALE_PRICE = sale_price) %>%
  filter(BLD_STORY > 13, # buildings need to include a 13th story
         BLD_STORY < 110, # remove potential data entry errors
         COOP_APTS == 1, # we are assessing single residential condo units
         !is.na(APTNO)) %>% # we can't identify the floor level without APTNO
  mutate(APTNO = ifelse(grepl("PH", APTNO), as.character(BLD_STORY), APTNO), # identify penthouses
         APTNO = as.numeric(sub("^0+", "", str_extract(APTNO, "\\d+"))), # extract digits from apartment number
         APT_FLOOR = case_when(
           # we assume the first digit is the floor level for three digit apartment numbers
           APTNO >= 100 & APTNO < 1000 ~ as.numeric(substr(as.character(APTNO), 1, 1)),
           # we assume the two digits are the floor level for four+ digit apartment numbers
           APTNO >= 1000 ~ as.numeric(substr(as.character(APTNO), 1, 2)), TRUE ~ APTNO), 
         BLD_ID = paste(BORO, BLOCK, CONDO_NUMBER, BLD_FRT, BLD_DEP, sep = "_"), # create building ID
         TRECE = BLD_ID %in% unique(BLD_ID[APT_FLOOR == 13]), # create 13th floor ID
         YRBUILT = ifelse(YRBUILT < 1000, NA, YRBUILT), # remove potential data entry errors
         GROSS_SQFT = ifelse(GROSS_SQFT == 0, NA, GROSS_SQFT),
         LOG_MARKET_VALUE = ifelse(MARKET_VALUE > 1000, log(MARKET_VALUE), NA), # needs to be positive to be defined on the log scale
         LOG_SALE_PRICE = ifelse(SALE_PRICE > 1000, log(SALE_PRICE), NA), # needs to be positive & filters potential data entry errors (units sold for less than $100)
         APT_FLOOR_OLD = APT_FLOOR,
         APT_FLOOR = ifelse(APT_FLOOR > 13 & TRECE == FALSE, APT_FLOOR - 1, APT_FLOOR)) %>% # find the unit's true floor level
  group_by(BLD_ID) %>%
  mutate(AVG_12TH_SQFT = mean(GROSS_SQFT[which(APT_FLOOR == 12)], na.rm = TRUE), # average 12th floor unit size per building
         AVG_13TH_SQFT = mean(GROSS_SQFT[which(APT_FLOOR == 13)], na.rm = TRUE), # average 13th floor unit size per building
         AVG_13TH_YRBUILT = mean(YRBUILT[which(APT_FLOOR == 13)], na.rm = TRUE)) %>% # average construction year per building
  ungroup() %>%
  group_by(BLD_ID, APT_FLOOR) %>%
  mutate(AVG_LOG_MARKET_VALUE_PER_FLOOR = mean(LOG_MARKET_VALUE, na.rm = TRUE), # geometric average of market value
         AVG_LOG_SALE_PRICE_PER_FLOOR = mean(LOG_SALE_PRICE, na.rm = TRUE)) %>% # geometric average of sales price
  ungroup() 


# export data
write.csv(dataNYC, file = "dataNYC.csv", row.names = FALSE)