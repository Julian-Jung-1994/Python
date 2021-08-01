# packages
import pandas as pd  # pandas
import os  # directory
from datetime import datetime  # date

# ======================================================================================================================
# 1. import data and lookup file
# ======================================================================================================================

cwd = os.getcwd()
df = pd.read_csv(cwd + r'\Pandas_Cleaning_RawData.csv', sep=';')
df_lookup = pd.read_csv(cwd + r'\Pandas_Lookup.csv', sep=';')
print(df.head(), df.shape)

# ======================================================================================================================
# 2. transform data
# ======================================================================================================================

# drop na rows
df = df.dropna(subset=['Classification', 'Email'], axis=0)

# drop duplicate users
df = (df.sort_values(by='Date', ascending=True)
      .drop_duplicates(subset=['Classification', 'Email', 'Computer'], keep="first"))


# functions for data transformation
def email_to_name(x):
    name_mail = x.split("@")[0]
    try:
        name = name_mail.split(".")
        return name[1].capitalize() + ", " + name[0].capitalize()
    except:
        name = name_mail.capitalize()
        return name


def country_helper(x):
    name_mail = x.split("@")
    if name_mail[1] == 'generic.com':
        return name_mail[0]
    else:
        return name_mail[1].casefold()

# transforming data
df['Category'] = df['Classification'].apply(lambda x: x.split(" ", 1)[0])
df['Type'] = df['Classification'].apply(lambda x: ' '.join(x.split(" ")[1:]))
df['Date'] = df['Date'].apply(lambda x: datetime.strptime(x[:10], '%Y-%m-%d'))
df['Name'] = df['Email'].apply(email_to_name)
df['Lookup'] = df['Email'].apply(country_helper)

# drop unnecessary rows
df = df[df['Category'] != "MetaUser"]

# correct data problems
df['Computer'] = df['Computer'].replace('Aker', 'Acer')
df['Category'] = df['Category'].str.title()
df['Type'] = df['Type'].str.title()
df['Computer'] = df['Computer'].str.title()

# ======================================================================================================================
# 3. merge with countries
# ======================================================================================================================

# merge with countries from lookup file
df = df.merge(right=df_lookup, how='left', on='Lookup')

# drop unused columns and sort the dataframe
df = df[['Name', 'Country', 'Category', 'Type', 'Computer', 'Date']]
df = df.sort_values(by=['Name', 'Category', 'Type', 'Computer', 'Date'], ascending=[True, True, True, True, False])
print(df.head(), df.shape)

# ======================================================================================================================
# 4. export data
# ======================================================================================================================

# export to csv
df.to_csv(cwd + r'\Pandas_Cleaning_TransformedData.csv', index=False, sep=";")
