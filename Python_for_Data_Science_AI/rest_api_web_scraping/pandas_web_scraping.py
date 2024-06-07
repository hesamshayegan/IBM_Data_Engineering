import pandas as pd

URL = 'https://en.wikipedia.org/wiki/List_of_largest_banks'
tables = pd.read_html(URL)

df = tables[0]
# print(df.head())

# drop rows with any missing values
df = df.dropna()

# generate descriptive statistics
# summarize the central tendency, dispersion and shape of the dataset distribution
# print('type', type(df.iloc[0, 2]))
summary_statistics = df.describe()

print(summary_statistics)