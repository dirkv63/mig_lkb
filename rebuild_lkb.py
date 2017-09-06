"""
This procedure will rebuild the sqlite lkb database
"""

import logging
from lib import my_env
from lib import lkb_store

cfg = my_env.init_env("lkb_migrate", __file__)
logging.info("Start application")
lkb = lkb_store.DirectConn(cfg)
lkb.rebuild()
logging.info("sqlite lkb rebuild")
logging.info("End application")
