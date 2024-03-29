# json_to_relational_assignment
## Data Modeling:

<img width="1467" alt="image" src="https://github.com/prashulk/json_to_relational_assignment/assets/67316162/2f513f08-d1fd-44cf-ac96-f54bd29e969d">


**Note**: If above image unclear then refer Data_Model_upd.pdf file for details.



Since, time values are sparse in this data, as observed through date values, as of now, I plan to keep the date values in the same table itself. If it would have been continuous, then I propose a ```time dimension``` as well where we can have different fields such as year, week, quarter, etc. based on the business needs and whatever granularity required and join with the date_id fields in the existing tables

## Queries:
Refer ```Queries.pdf``` for this

## Data Quality Handling:
The initial sequence I have followed for data cleaning - 

<img width="926" alt="image" src="https://github.com/prashulk/json_to_relational_assignment/assets/67316162/306799a4-119a-4575-81a6-f98056e36b70">

  - Data quality checks and cleaning done with assumptions stated in file.


### Data Quality - Users: (Refer - read_users.ipynb & Users_dq_sql.pdf)

1. **Null Percentage and Duplicate UserIDs**:
   - Around 10% of data is missing for `signupSource`, `state`, and `lastLoginDate`.
   - Out of 495 entries, only 212 unique userIDs were found.
   - **Further steps**:
     - Based on business questions asked, decision taken to create a separate ```User dim``` table which will have 1 record per UserID to enforce uniqueness.
     - Duplicate Userid's removed and then set as Primary key. Potential scope for SCD if business wants to keep historical details for users.

2. **Handling Blank Strings**:
   - Blank strings were converted to nulls for consistency.
   - Column names were renamed for clarity and consistency.
   - **Reasoning**: 
     - Standardization ensures uniformity across fields and facilitates easier analysis.

3. **Timestamp Conversion**:
   - Epoch timestamps were converted to `dd-mm-yyyy` format for better analysis.
   - **Reasoning**:
     - Aggregation based on months requested as one of the features.Enables flexible time-based analysis, meeting future business demands for different time granularities - year, day, week.

4. **Postgres Data Quality Review**:
   - Reasoning in the .pdf file

### Data Quality - Brands (Refer - read_brands.ipynb & Brands_dq_sql.pdf)

1. **Null Percentage Analysis**:
   - Approximately 50% of data is missing for `categoryCode` and `topBrand`.
   - **Steps**:
     - These missing values might require further investigation to determine their impact on analysis and decision-making processes.
     - Lack of `categoryCode` and `topBrand` data could affect brand classification and identification of top brands.
     - Potential for further Normalization - (category dim) could be solved if data present thus keeping only the id's in the main dimension table

2. **Handling Null Values**:
   - Blanks were replaced with `None`.
   - Column names were standardized for consistency.

3. **Database Cleaning and Assumptions**:
   - Cleaning steps and assumptions were documented in the provided PDF.

**Important Note:**
- Nulls are present in the `BrandCode` field, but there are no nulls in the `Barcode` field.
- This distinction is crucial for deciding how to join this table with other tables, especially considering the plan to create a dimension table for brands.
- Additionally, there are no duplicates or nulls in the `uuid` field, making it a suitable candidate for the primary key aspect.



### Data Quality - Receipts (Refer - read_receipts.ipynb & Receipts_dq_sql.pdf)

1. **Null Percentage Analysis**:
   - Approximately 50% of data is missing for 'bonusPoints', 'pointsAwardedDate', followed by `rewardsReceiptItemList`, `totalSpent`, etc.
   - **Steps**:
     - Further analysis in the sql.pdf file to understand relationships.

2. **Handling Null Values**:
   - Blanks replaced with `None` for consistency.
  
3. **Data Handling**:
  - The input JSON is further split into 2:

<img width="762" alt="image" src="https://github.com/prashulk/json_to_relational_assignment/assets/67316162/ae82a427-21e5-4f01-9f1c-7f77b91548d6">

4. **Data Conversion & Column Renaming, Insertion in Database**:
   - String objects converted to float for quantitative data, dates converted appropriately column sequencing followed by insertion of two tables in Postgres

5. **Refer the Receipts_dq.sql**: Refer this file for further analysis on inconsistencies found in some of the fields. There were more, but the main ones which are of primary cause as of now have been mentioned.

## Communicate with Stakeholders:

Refer ```Email.pdf``` for this.


## Production scaling Pipeline: (Refer Production Code Folder for this)
<img width="1212" alt="image" src="https://github.com/prashulk/json_to_relational_assignment/assets/67316162/4cd59d2b-a583-4d14-a8fd-bd2be77aa27c">

The input files are fed into unit test and if the tests are passed then the ingestion scripts followed by loading the relational tables



