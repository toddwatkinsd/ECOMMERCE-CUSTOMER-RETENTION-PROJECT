# ECOMMERCE-CUSTOMER-RETENTION-PROJECT
import pandas as pd 
import seaborn as sb 
import matplotlib as mp 
import os
import sqlite3
import numpy as np 
os.chdir('/Users/toddwatkins/desktop/Econometrics1/Chap1/archive1')

df_customers = pd.read_csv('olist_customers_dataset.csv')
df_orders = pd.read_csv('olist_orders_dataset.csv')
df_items = pd.read_csv('olist_order_items_dataset.csv')
df_reviews = pd.read_csv('olist_order_reviews_dataset.csv')
df_products = pd.read_csv('olist_products_dataset.csv')
df_category_translation = pd.read_csv('product_category_name_translation.csv')

#Merge data in order to understand the english translation of the category names
df_products_translated = pd.merge(
    df_products,
    df_category_translation,
    on='product_category_name',  
    how='left'
)
#Dropped the column with the non-english translation 
df_products_translated.drop(
    columns=['product_category_name'],
    inplace=True
)
#Created a dataframe (A library) in order to access the data easier without using snake
dataframes = {
    'Customers': df_customers,
    'Orders': df_orders,
    'Order Items': df_items,
    'Reviews': df_reviews,
    'Products': df_products,
    'Categories': df_category_translation
}

#Checked for duplicates in the data to essure quality data when moving to the explore faze, I used a loop
for Orders, df in dataframes.items():
    duplicate_count = df.duplicated().sum()
    status = "⚠️ Duplicates Found" if duplicate_count > 0 else "✅ Clean"
    print(f"| {Orders:15} | Rows: {len(df):10,} | Duplicates: {duplicate_count:8} | Status: {status}")
    print("-" * 100)
#the values printed below are those that can also be considered null, so when checking for null values, after stripping white space, all empty spaces will be identified
CUSTOM_NA_STRINGS = ['NA', 'N/A', 'n/a', 'NULL', 'None', 'Missing', 'Unknown', '?', '-']
CUSTOM_NA_NUMBERS = [-999, -1, 99999] 
#creating the loop
for Orders, df in dataframes.items():
    #Placing the extensive null values as missing place holders, (np.nan)
    df = df.replace(to_replace=CUSTOM_NA_NUMBERS, value=np.nan)
    df = df.replace(to_replace=CUSTOM_NA_STRINGS, value=np.nan)
   
    true_nulls = df.isnull()
  #UP, running the null check again with those missing values now identified in .isnull()
    empty_string_mask = df.astype(str).apply(lambda x: x.str.strip()).eq("").fillna(False)
    #.astype is the main function, we need python to recognize spaces with no values as characters so you can identify them and remove them
    combined_mask = true_nulls | empty_string_mask
    #this is a mask that allows us to now combine all null values present 
    missing_data_summary = combined_mask.sum()
    missing_columns = missing_data_summary[missing_data_summary > 0]
    df_reviews['review_comment_message'] = df_reviews['review_comment_message'].fillna('NO_COMMENT_PROVIDED')
    df_reviews['review_comment_title'] = df_reviews['review_comment_title'].fillna('NO_TITLE_PROVIDED')
    #Here we segment the actual missing columns 
    print("-" * 75)  
    print(f"🔎 Comprehensive Missing Value Report for: **{Orders}**")
    print("-" * 75)
    #this is for organization
    if missing_columns.empty:
        print("✅ No missing values (Nulls, Placeholders, or Empty Strings) found.")
        #Will prompt us and tell us if there are no missing values
    else:
        total_rows = len(df)
        #Gives the total amount of orders, or rows, in each data set in the loop
        print(f"Total Rows: {total_rows:,}\n")
        #Prints the total number of rows for the loop 
        print("Column   | Missing Count   | Percentage (%)    | Sample Indices (First 3)")
        #This is the header row to make the data more easy to read, for the assessment table
        print("-------  |---------------  |----------------   |--------------------------")
        #Prints a separator bwteen the header and the assessment table
        for column, count in missing_columns.items():
            #this starts the loop, it commands python to find each column "for column", count each column with whatever proceeds in "count", in each missing columns variable created.
            #.items(): allows us to unpack the .sum() method used when creating the missing_columns variable 
            percentage = (count / total_rows) * 100
            #Percentage: A variable created to calculate the percentage of total missing cells in each column
            col_mask = combined_mask[column] 
            #col_mask: variable created to revert to to each column and define each precise missing value under each column
            missing_indices = df[col_mask].index.tolist()
            #missing_indices: variable created to specify each column by index and create a list of those indices
            sample_indices = ', '.join(map(str, missing_indices[:3])) 
            #sample_indices: ', ': how to separate each index. .join: tells what is being mapped to separate each index with ', '.
            #map: turns each text into a string so that .join can work.
            #[:3]: tell python to to ist only the first three missing_indices
            if len(missing_indices) > 3:
                sample_indices += ', ...'
            #for formatting just adding a ... at the end to exemplify more missing values.
            print(f"{column:<5} | {count:5,} | {percentage:5.2f} | {sample_indices}")
            #function that shows the complete outcome, enabled by all of the work that wa just done.
            print("\n")
            #prints a new separating line character

#focusing on 'delivered' as the target status
TARGET_STATUS = 'delivered'
for Orders, df in dataframes.items():
    
     
    if 'order_status' not in df.columns:
        continue  

     
    true_nulls = df.isnull()
    empty_string_mask = df.astype(str).apply(lambda x: x.str.strip()).eq("").fillna(False)
    combined_mask = true_nulls | empty_string_mask
    #repeating the same process as before for missing values just executing a preferred target status
    status_filter_mask = df['order_status'] == TARGET_STATUS
    #creating the mask for 'order status', this is where python will now check for the target status == 'delivered'
    conditional_combined_mask = combined_mask[status_filter_mask]
    #adding a third additional mask for this specific target, now placing all three masks together. 
    df_delivered = df[status_filter_mask]
    #Creating the delivered variable
    missing_data_summary = conditional_combined_mask.sum()
    #making sure the repeat code has the new target status to specify what to look for
    missing_columns = missing_data_summary[missing_data_summary > 0]
    #repeat code after uploading the target status to 'missing_data_summary
    total_rows = len(df_delivered) 
    #these three lines are just readjusting the previous missing data code, and calibrating it for the target status == 'delivered' 
    print("-" * 85)
    print(f"🔎 Missing Value Report for: **{Orders}** (FILTERED by order_status = '{TARGET_STATUS}')")
    print("-" * 85)
    #Formatting and clarity
    if missing_columns.empty:
        print(f"✅ No missing values (Nulls, Placeholders, or Empty Strings) found among {total_rows:,} delivered orders.")
        #formatting for clarity
    else:
        print(f"Total '{TARGET_STATUS}' Orders: {total_rows:,}\n")
        #Header for missing 
        print(f"{'Column':<15} | {'Missing Count':<15} | {'Percentage (%)':<15} | Sample Indices (First 3)")
        print(f"{'-'*15} | {'-'*15} | {'-'*15} | {'-'*25}")
        #Formatting and clarity, these two lines are the header
        for column, count in missing_columns.items():
            #this is the actual loop that is being created for the function placed under the header directing the missing values for the target "delivered"
            percentage = (count / total_rows) * 100
            col_mask = conditional_combined_mask[column] 
            missing_indices = df_delivered[col_mask].index.tolist()
            sample_indices = ', '.join(map(str, missing_indices[:3])) 
            if len(missing_indices) > 3:
                sample_indices += ', ...'
            print(f"{column:<15} | {count:<15,} | {percentage:<15.2f} | {sample_indices}")
    print("\n" * 2) 
    #139-148 are all repeat code used from the previous block that finds missing values now that the target status has been identified in this for statement 


 
 
TARGET_STATUS = 'delivered' 
STATUSES_TO_KEEP = ['delivered', 'canceled']
#understanding the target status

df_orders['order_status'] = (
    df_orders['order_status']
    .astype(str)
    .str.strip()   
    .str.lower()   
)
#This is how all of the data is recognizable, eliminating all false values and inconsistent representations of data. 


date_cols = [
    'order_purchase_timestamp', 
    'order_approved_at', 
    'order_delivered_carrier_date', 
    'order_delivered_customer_date'
]
for col in date_cols:
    df_orders[col] = pd.to_datetime(df_orders[col], errors='coerce')
    #pd.to_datetime is the reference to create a str that uses the same format with a dates, i.e("03/25/2017 10:05 PM"	2017-03-25 22:05:00)
delivered_mask = df_orders['order_status'] == TARGET_STATUS
#making sure the target status is in line with what we are specifying in this specific for loop.
df_orders.loc[delivered_mask & df_orders['order_approved_at'].isna(),
              'order_approved_at'] = df_orders['order_purchase_timestamp']
#this is to set the value of the missing approval date to the value of the purchase date for those corresponding rows. This is a common imputation technique, assuming that if the approval date is missing for a delivered order, the approval must have occurred immediately at the time of purchase.
df_orders['Time_to_Approval_hrs'] = (
    df_orders['order_approved_at'] - df_orders['order_purchase_timestamp']
).dt.total_seconds() / 3600
#Time_to_approval_hours: converting each .isna value to 0 hrs, and actionably converting the time to approval to hrs
df_orders['Shipping_Time_hrs'] = (
    df_orders['order_delivered_customer_date'] - df_orders['order_delivered_carrier_date']
).dt.total_seconds() / 3600
#Shipping_Time_hrs: feature engineering to take two timestamp columns and convert them into a useable number

#Both of the variables that are created above are fabricated to turn raw data into insights for the ltv model
median_shipping_time = df_orders[delivered_mask]['Shipping_Time_hrs'].median()
#Median_shipping_time: variable that will be used to fill in the median for isna cells 
shipping_nan_mask = delivered_mask & df_orders['Shipping_Time_hrs'].isna()
#shipping_nan_mask: location of the .isna cells
df_orders.loc[shipping_nan_mask, 'Shipping_Time_hrs'] = median_shipping_time
#this is how to locate those isna cells and fill in the median shipping_time_hours
df_ltv_training = df_orders[df_orders['order_status'].isin(STATUSES_TO_KEEP)].copy()
#creating a copy of the new cleaned data, remember STATUSES_TO_KEEP = ['delivered', 'canceled']
print("--- Duration Handling Complete ---")
print(f"Median Shipping Time used for imputation (hrs): {median_shipping_time:.2f}")
print(f"Total delivered orders with imputed shipping time: {shipping_nan_mask.sum():,}")
print(f"Rows retained for LTV training (Delivered & Canceled): {len(df_ltv_training):,}")
print("\nVerification Check:")
print(df_ltv_training[shipping_nan_mask].head(10))
#this is to check that that the .isna correctly was imputed, applying the median value for shipping_time_hours 


df = df_orders   
df['order_status'] = df['order_status'].str.strip().str.title()
#ensuring the titles on each status are recognizable and clean for proper absorption 
all_statuses = df['order_status'].unique()
#ensuring each title/column name is retruned in the output with non-repeating values present in that column
print("✅ List of All Unique Order Statuses:")
print(all_statuses)
print("-" * 40)
status_counts = df['order_status'].value_counts()
print("📊 Statuses and Their Frequencies:")
print(status_counts)



print("Merging customer data to link unique IDs...")
df_ltv_training = df_ltv_training.merge(
    df_customers[['customer_id', 'customer_unique_id']],
    on='customer_id',
    how='left'
)
print("Customer unique ID added.")




 
 
 

 
print("Calculating total monetary value per order...")

 
 
order_value_df = df_items.groupby('order_id').agg(
    order_total_price=('price', 'sum'),
    order_total_freight=('freight_value', 'sum')
).reset_index()

 
order_value_df['order_monetary_value'] = (
    order_value_df['order_total_price'] + order_value_df['order_total_freight']
)

 
order_value_df = order_value_df[['order_id', 'order_monetary_value']]
df_ltv_training = df_ltv_training.merge(
    order_value_df,
    on='order_id',
    how='left'
)


canceled_mask = df_ltv_training['order_status'] == 'canceled'

 
df_ltv_training.loc[canceled_mask, 'order_monetary_value'] = 0.0

 
print("Monetary calculation and merge complete.")
print(f"Total rows in LTV training set after merge: {len(df_ltv_training):,}")
print(f"Sanity Check (Sample of Canceled Orders):")
print(df_ltv_training[canceled_mask][['order_status', 'order_monetary_value']].head())

 
 
first_purchase_date_df = df_ltv_training.groupby('customer_unique_id')[
    'order_purchase_timestamp'
].min().reset_index()

 
first_purchase_date_df.columns = [
    'customer_unique_id', 
    'first_purchase_timestamp'
]

print("First purchase dates calculated.")

 
df_ltv_training = df_ltv_training.merge(
    first_purchase_date_df, 
    on='customer_unique_id', 
    how='left'
)

 
 
df_ltv_training['is_first_transaction'] = (
    df_ltv_training['order_purchase_timestamp'] == df_ltv_training['first_purchase_timestamp']
)

print("Boolean flag 'is_first_transaction' created.")



 
 
 
 
observation_date = df_ltv_training['order_purchase_timestamp'].max() 
print(f"Observation Date set to: {observation_date}")


 

 
delivered_orders = df_ltv_training[df_ltv_training['order_status'] == 'delivered'].copy()


 
customer_df = df_ltv_training.groupby('customer_unique_id').agg(
    
     
    Last_Purchase_Date=('order_purchase_timestamp', 'max'),
    
     
    LTV_Target_Historical=('order_monetary_value', 'sum'),
    
     
    Total_Transactions=('order_id', 'count')
    
).reset_index()


 

 
frequency_monetary_df = delivered_orders.groupby('customer_unique_id').agg(
    Frequency=('order_id', 'count'),
    Monetary_Avg=('order_monetary_value', 'mean')
).reset_index()

 
customer_df = customer_df.merge(
    frequency_monetary_df, 
    on='customer_unique_id', 
    how='left'
)


 
customer_df['Recency_Days'] = (
    observation_date - customer_df['Last_Purchase_Date']
).dt.days

 
 
 
customer_df[['Frequency', 'Monetary_Avg']] = customer_df[['Frequency', 'Monetary_Avg']].fillna(0)


print("RFM and LTV features calculated at the customer level.")
print(f"Final customer DataFrame size: {len(customer_df):,} unique customers.")

 
 
 
 
observation_date = df_ltv_training['order_purchase_timestamp'].max() 
print(f"Observation Date set to: {observation_date}")


 

 
delivered_orders = df_ltv_training[df_ltv_training['order_status'] == 'delivered'].copy()


 
customer_df = df_ltv_training.groupby('customer_unique_id').agg(
    
     
    Last_Purchase_Date=('order_purchase_timestamp', 'max'),
    
     
    LTV_Target_Historical=('order_monetary_value', 'sum'),
    
     
    Total_Transactions=('order_id', 'count')
    
).reset_index()


 

 
frequency_monetary_df = delivered_orders.groupby('customer_unique_id').agg(
    Frequency=('order_id', 'count'),
    Monetary_Avg=('order_monetary_value', 'mean')
).reset_index()

 
customer_df = customer_df.merge(
    frequency_monetary_df, 
    on='customer_unique_id', 
    how='left'
)


 
customer_df['Recency_Days'] = (
    observation_date - customer_df['Last_Purchase_Date']
).dt.days

 
 
 
customer_df[['Frequency', 'Monetary_Avg']] = customer_df[['Frequency', 'Monetary_Avg']].fillna(0)


print("RFM and LTV features calculated at the customer level.")
print(f"Final customer DataFrame size: {len(customer_df):,} unique customers.")


 
 
 

 
print("Merging static customer geography data...")

 
customer_geo_df = df_customers[['customer_unique_id', 'customer_city', 'customer_state']].drop_duplicates()

 
customer_df = customer_df.merge(
    customer_geo_df,
    on='customer_unique_id',
    how='left'
)

 
print("Standardizing geographic text...")
customer_df['customer_city'] = customer_df['customer_city'].astype(str).str.strip().str.lower()
customer_df['customer_state'] = customer_df['customer_state'].astype(str).str.strip().str.lower()


 
print("Applying One-Hot Encoding to State...")
 
state_dummies = pd.get_dummies(customer_df['customer_state'], prefix='state', drop_first=False)

 
customer_df = pd.concat([customer_df, state_dummies], axis=1)

 
customer_df = customer_df.drop('customer_state', axis=1)


 
 
 

print("Applying Target Encoding to City (using historical LTV)...")

 
city_avg_ltv = customer_df.groupby('customer_city')['LTV_Target_Historical'].mean().reset_index()
city_avg_ltv.columns = ['customer_city', 'city_ltv_encoded']

 
customer_df = customer_df.merge(
    city_avg_ltv,
    on='customer_city',
    how='left'
)

 
 
global_mean_ltv = customer_df['LTV_Target_Historical'].mean()
customer_df['city_ltv_encoded'] = customer_df['city_ltv_encoded'].fillna(global_mean_ltv)

 
customer_df = customer_df.drop('customer_city', axis=1)


print("Geographic feature encoding complete.")
print(f"Final features include {len(customer_df.columns) - 1} predictors.")
print(customer_df[['state_sp', 'state_rj', 'city_ltv_encoded', 'LTV_Target_Historical']].head())



print(df_orders.loc[3002, 'Shipping_Time_hrs'])

Median_Value = df_orders['Shipping_Time_hrs'].median()
print(Median_Value)
rows_replaced_median = shipping_nan_mask.sum()
print(df_orders[shipping_nan_mask].head(9))

print(df_orders.loc[29263, ['order_status', 
                            'order_delivered_carrier_date', 
                            'order_delivered_customer_date']])

excluded_mask = ~df_orders['order_status'].isin(STATUSES_TO_KEEP)

# 2. Filter the DataFrame using the mask
df_excluded = df_orders[excluded_mask].copy()

# 3. Aggregate and display the Shipping_Time_hrs for these excluded statuses
# We expect all (or nearly all) values here to be NaN because they haven't been delivered
excluded_summary = df_excluded.groupby('order_status').agg(
    Total_Count=('order_id', 'count'),
    Shipping_Time_Sample=('Shipping_Time_hrs', lambda x: x.head(2).tolist())
).reset_index()

# Rename the sample column for clarity
excluded_summary.columns = [
    'Order_Status', 
    'Total_Count', 
    'Shipping_Time_Sample_Hours'
]

print("Orders Excluded from LTV Training Set:\n")
print(excluded_summary.to_markdown(index=False))

print(f"\nTotal Excluded Orders: {len(df_excluded):,}")
print("\nConclusion: The Shipping_Time_hrs column for these statuses should be NaN,")
print("as they never completed the delivery process.")

print("\n" + "=" * 50)
print("🎯 FINAL VERIFICATION: EXCLUSION CHECKS (CLEANED) 🎯")
print("=" * 50)

# 1. AGGRESSIVELY CLEAN THE COLUMN AGAIN TO ELIMINATE ANY HIDDEN CHARACTERS
# This ensures consistency, stripping all whitespace and forcing lowercase.
df_orders['order_status_cleaned'] = (
    df_orders['order_status']
    .astype(str)  # Ensure it's a string type
    .str.strip()  # Remove leading/trailing whitespace
    .str.lower()
)

# 2. Define the filter list using the cleaned status column name
STATUSES_TO_EXCLUDE_FROM_REPORT = ['delivered', 'canceled']

# 3. Create the exclusion mask using the new CLEANED column
excluded_mask = ~df_orders['order_status_cleaned'].isin(STATUSES_TO_EXCLUDE_FROM_REPORT)

# 4. Filter the DataFrame using the mask
df_excluded = df_orders[excluded_mask].copy()

# 5. Aggregate and display the Shipping_Time_hrs for these excluded statuses
# Group by the original (cleaned) order_status for the report
excluded_summary_verified = df_excluded.groupby('order_status_cleaned').agg(
    Total_Count=('order_id', 'count'),
    # Use the formatting lambda
    Shipping_Time_Sample=('Shipping_Time_hrs', 
                          lambda x: [f"{val:.2f}" if not pd.isna(val) else 'NaN' for val in x.head(2).tolist()])
).reset_index()

excluded_summary_verified.columns = [
    'Order_Status', 
    'Total_Count', 
    'Shipping_Time_Sample_Hours'
]

print("Orders **Correctly Excluded** from LTV Training Set:\n")
# The output table will now use the CLEANED status names for grouping
print(excluded_summary_verified.to_markdown(index=False))

print(f"\nTotal Excluded Orders: {len(df_excluded):,}")
print("Conclusion: Only incomplete statuses remain, and their Shipping_Time_hrs are correctly 'NaN'.")
