import pandas as pd 

df =pd.read_json(r'C:\Users\FTECH\Downloads\Project\Tweets_Crawler\articles.json')

duplicates = df[df.duplicated(subset='id', keep=False)]
df_unique = df.drop_duplicates(subset='id', keep='first')
# Display the duplicates
print(duplicates)