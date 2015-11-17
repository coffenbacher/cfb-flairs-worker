import rethinkdb
import delorean
from delorean import stops, parse, Delorean, epoch
from datetime import timedelta
from retrying import retry
import time
import datetime
import sys
import os
import praw
import logging; logger=logging.getLogger(); logger.setLevel('INFO')

# Global helpers
c = rethinkdb.connect(os.getenv('RETHINKDB_HOST'), os.getenv('RETHINKDB_PORT'))
r = praw.Reddit(user_agent='/r/CFB flair analysis by /u/coffenbacher')

# Setup db
def get_db():
    return rethinkdb.db('cfb')

def setup_tables():
    db = get_db()
    try:
        db.table_create('post').run(c)
        db.table_create('comment').run(c)
    except rethinkdb.RqlRuntimeError:
        pass

def get_posts_between_dates(start, end):
    for stop in stops(freq=delorean.DAILY, start=start.naive(), stop=end.naive()):
        next_stop = stop + timedelta(days=1)
        
        s_epoch = int(stop.epoch())
        n_epoch = int(next_stop.epoch()) - 1 # Offset to avoid overlap
        logging.info('Running search for range %s->%s' % (stop.datetime.strftime('%x'), next_stop.datetime.strftime('%x')))
        query = 'timestamp:%d..%d' % (s_epoch, n_epoch)
        submissions = r.search(query, subreddit='cfb', sort='new', limit=1000, syntax='cloudsearch')
        yield submissions

@retry
def extract_data():
    logging.info("Extracting posts")
    db = get_db()
    current_progress = db.table('progress').get('current').run(c)
    start = epoch(current_progress.get('epoch'))
    
    # Only get posts for one day at a time
    for group in get_posts_between_dates(start, Delorean()):
        for post in group:
            logging.info('Inserting post %s' % post.id)
            results = db.table('post').insert({
                'id': post.id,
                'author': post.author.name,
                'created': post.created_utc,
                'flair': post.link_flair_text,
                'num_comments': post.num_comments,
                'score': post.score,
                'title': post.title
                }, conflict="replace").run(c)
            logging.info(results)
            logger.info('Getting comments for post %s' % post.id)
            
            comments = []
            post.replace_more_comments(limit=None)
            for comment in praw.helpers.flatten_tree(post.comments):
                if isinstance(comment, praw.objects.Comment) and comment.author:
                    ftext = comment.author_flair_text
                    f = ftext.split('/') if ftext else []
                    comments.append({
                        'id': comment.id,
                        'post': post.id,
                        'author': comment.author.name,
                        'created': comment.created_utc,
                        'flair1': f[0].strip() if len(f) > 0 else None,
                        'flair2': f[1].strip() if len(f) > 1 else None,
                        'score': comment.score
                    })
            logging.info('Inserting %s comments' % len(comments))
            results = db.table('comment').insert(comments, conflict="replace").run(c)
            logging.info(results)
 
            progress = {'id': 'current', 'epoch': post.created_utc}
            db.table('progress').insert(progress, conflict="replace").run(c)
    
    
if __name__ == "__main__":
    setup_tables()
    extract_data()
    time.sleep(int(os.getenv('DELAY', 0)))
