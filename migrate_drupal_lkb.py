"""
This is a starter script to investigate the Drupal lkb database.
It should evolve into a migration script.
"""

from lib import my_env
from lib import mysqlstore as drupal
from lib import lkb_store
from lib.lkb_store import *

cfg = my_env.init_env("lkb_migrate", __file__)
db = cfg['Main']['db']
logging.info("Start application")
ds = drupal.DirectConn(cfg)
lkb = lkb_store.init_session(db=db)
parents = ds.get_parent_for_node()
rev_cnt = ds.get_rev_cnt()
node_info = ds.get_node_info()
pg_info = my_env.LoopInfo("Handle Node", 500)
for rec in node_info:
    pg_info.info_loop()
    if rec['nid'] in parents:
        parent_id = parents[rec['nid']]
    else:
        parent_id = -1
    node = Node(
        nid=rec['nid'],
        parent_id=parent_id,
        title=rec['title'],
        body=rec['body_value'],
        created=rec['created'],
        modified=rec['changed'],
        revcnt=rev_cnt[rec['nid']]
    )
    lkb.add(node)
pg_info.end_loop()
lkb.commit()
ds.close()
logging.info("End application")
