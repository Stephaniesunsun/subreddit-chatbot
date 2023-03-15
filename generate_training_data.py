import sqlite3 
import pandas as pd 

db_name = "reddit"
connection = sqlite3.connect('{}.db'.format(db_name))
c = connection.cursor()
limit = 4000
last_unix = 0
cur_length= limit
counter =0
test_done = False

while cur_length == limit:
    df = pd.read_sql("""SELECT * FROM programming WHERE unix>{} 
    AND parent_text!=0 AND score>0 ORDER BY unix ASC LIMIT {}""".format(last_unix, limit), connection)
    last_unix = df.tail(1)['unix'].values[0]
    cur_length = len(df)
    if not test_done:
        with open("test_from",'a',encoding='utf8') as f:
            for content in df['parent_text'].values:
                f.write(content+'\n')
        with open("test_to", 'a', encoding='utf8') as f:
            for content in df['child_text'].values:
                f.write(content+'\n')
        test_done = True
    else:
        with open("train_from",'a',encoding='utf8') as f:
            for content in df['parent_text'].values:
                f.write(content+'\n')
        with open("train_to", 'a', encoding='utf8') as f:
            for content in df['child_text'].values:
                f.write(content+'\n')
    counter+=1
    if counter%2==0:
        print(counter*limit, 'rows completed so far')
