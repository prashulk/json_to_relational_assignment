# json_to_relational_assignment

### Introduction:
The structure or the sequence used here is:
  - Initially, the ***json*** files are read using Jupyter Notebook, to read the data, do first round of data quality checks and cleaning (with assumptions stated).
  - Post initial cleaning of JSON, database connection is setup to Postgres and keeping relational design in mind, the data is inserted into the relational tables.
  - Second set of data quality inspection is done in Postgres, with the data model in mind. Screenshots/SQL code is attached as proof for the same.

### Data Quality - users:
From the Nulls percentage we observe that there around 10% of data is missing for *signupSource*, *state* and *lastloginDate*. Moreover out of 495 entries, there were only 212 unique userid's. Based on the business questions asked (query 5 and 6), the design was to create a separate User dimension table. This will be helpful wherein we can just isnert new User_ids in the Users dimension table with just 1 user per record thereby making the User_id as ```Primary Key```. Hence, Primary Key's cannot be duplicate and it was observed in the input Json, that entire rows were a duplicate, hence the decision was taken to remove duplicate user_id's here.  

Moreover, it was also observed that there were Blank strings in certain fields along with Nulls. Hence to maintain consistency, another transformation is used to convert the blank strings as Nulls only for consistency purpose. Followed by, renaming column names to meaningful or consistent naming schemes.

Finally, the timestamp was converted from epoch to proper format as an aggregation query was requested for specific month. Moreover, there can be other business demands, for drill down analysis based on specific time - i.e. Quarter wise, Month wise, Year wise. Keeping flexibility in mind, the epoch time was converted to ```dd-mm-yyyy``` format.
