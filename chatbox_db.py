import sqlite3
import json
import os 
from datetime import datetime

sql_transation = []
db_name = "reddit"
connection = sqlite3.connect('{}.db'.format(db_name))
c = connection.cursor()
db_created = False

def create_table(name):
    c.execute("""
        CREATE TABLE IF NOT EXISTS {}('parent_id' TEXT PAIMARY KEY,
         'child_id' TEXT, 'parent_text' TEXT, 'child_text' TEXT, 'subreddit' TEXT, 'score' INT, 'unix' INT
    );""".format(name))

def format_data(data):
    data = data.replace('"', "'").replace('\n', '')
    return data
#
#def transaction_builder(sql):
#    global sql_transation
#    sql_transation.append(sql)
#    if len(sql_transation) > 500:
#        print('begin insert')
#        c.execute('BEGIN TRANSACTION')
#        for s in sql_transation:
#            try:
#                c.execute(s)
#                print("inserted")
#            except:
#                pass
#        connection.commit()
#        sql_transation=[]
    
def find_score(pid):
    sql="SELECT score FROM programming WHERE parent_id=?"
    c.execute(sql,(pid,))
    result = c.fetchone()
    if result!=None:
            return result[0]
    else:
        return None

def find_parent_text(child_id):
    try:
        sql = "SELECT child_text FROM programming WHERE child_id = ?"
        c.execute(sql, (child_id,))
        result = c.fetchone()
        if result!=None:
            return result[0]
        else:
            return False
    except Exception as e:
        print('no child col exists', e)
    
def acceptable(comment):
    if(len(comment)<1 or len(comment)>200):
        return False
    else:
        return True

def sql_insert_replacement(parent_id, child_id, child_text, score, parent_text,unix):
    try:
        sql = """UPDATE programming SET child_id = ?, child_text = ?, score = ?, parent_text=?, unix=? WHERE parent_id = ?"""
        c.execute(sql, (child_id, child_text, score, parent_text, parent_id,unix))
        connection.commit()

    except Exception as e:
        print('replace comment', e)

def sql_insert(parent_id, child_id, child_text, score, parent_text, subreddit,unix):
    try:
        sql = """INSERT INTO programming (parent_id, child_id, parent_text, child_text, subreddit, score, unix)
          VALUES (?,?,?,?,?,?,?)"""
        c.execute(sql, (parent_id, child_id, parent_text, child_text, subreddit,score,unix,))
        connection.commit()

    except Exception as e:
        print('insert has parent', e)

if __name__ =='__main__':
    row_counter = 0
    paired_rows  = 0
    subreddits = set()
    create_table('programming')

    with open('/Users/stephie/Desktop/datasets/reddit-corpus/utterances.jsonl') as f:
        for row in f:
            row = json.loads(row)
            subreddit = row['meta']['subreddit']
            subreddits.add(subreddit)
            child_id = row['id']
            child_text = format_data(row['text'])
            score = row['meta']['score']
            parent_id = row['reply-to']
            parent_text = find_parent_text(parent_id)
            unix = row['timestamp']

            if(child_text and not acceptable(child_text)):
                continue
            
            if(score > 2):
                row_counter+=1
                existing_score = find_score(parent_id)

                if(existing_score!=None and score > existing_score):
                    sql_insert_replacement(parent_id, child_id, child_text, score, parent_text,unix)
                elif existing_score==None:
                    sql_insert(parent_id, child_id, child_text, score, parent_text, subreddit,unix)
                    if parent_text:
                        paired_rows+=1
            
            if row_counter%1000 == 0:
                print('total rows read: {}, time: '.format(row_counter, str(datetime.now)))
        f.close()
        connection.close()
    print('subreddits:')
    print(subreddits)

                

