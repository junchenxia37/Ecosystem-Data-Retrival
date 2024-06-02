import pandas as pd

# Read the CSV file
df = pd.read_csv('aggregation_to_csv.csv')

# Pivot the table to have each activity type as a column
pivot_df = df.pivot_table(index=['orgs', 'repo', 'date'], columns='activities', values='count', aggfunc='sum')

# Reset index to flatten the DataFrame
pivot_df.reset_index(inplace=True)

# Fill NaN values with 0 or another placeholder if no activity was recorded
pivot_df.fillna(0, inplace=True)

# Save the transformed DataFrame to a new CSV file
pivot_df.to_csv('transformed_data.csv', index=False)