import rethinkdb as r
import os
import logging; logger=logging.getLogger(); logger.setLevel('INFO')

# Global helpers
c = r.connect(os.getenv('RETHINKDB_HOST'), os.getenv('RETHINKDB_PORT'))
db = r.db('cfb')
    
def setup_tables():
    try:
        db.table_create('user').run(c)
        db.table_create('flair').run(c)
    except r.RqlRuntimeError:
        pass
    
def sync_flair():
    logging.info("Syncing flair options")
    flair = []
    flair.extend(db.table('comment').distinct(index='flair1').run(c))
    flair.extend(db.table('comment').distinct(index='flair2').run(c))
    flair = [{"id": f} for f in flair]
    logging.info("Inserting flair: %s" % len(flair))
    results = db.table('flair').insert(flair, conflict="replace").run(c)
    logging.info(results)
    logging.info("Flair available: %s" % db.table('flair').count().run(c)) 
    
def sync_users():
    logging.info("Syncing users")
    users = db.table('comment').distinct(index="author").run(c)
    for u in users:
        p = db.table('comment').get_all(u, index="author").order_by(r.desc('created')).run(c)[0]
        d = {
            "id": u,
            "flair1": p['flair1'],
            "flair2": p['flair2']
            }
        db.table('user').insert(d, conflict="replace").run(c)
    logging.info("User count: %s" % db.table('user').count().run(c))
    
if __name__ == "__main__":
    setup_tables()
    #sync_flair()
    sync_users()