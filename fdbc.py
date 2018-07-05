import fdb
from os.path import normpath

class fdbc:
  def __init__(self, srv = None, usr = None, psw = None, rol = None, enc = None):
    self.connect(srv, usr, psw, rol, enc)
    
    self.execute = self.cursor.execute
    self.fetchall = self.cursor.fetchall
    self.fetchallmap = self.cursor.fetchallmap
    self.commit = self.con.commit
    self.rollback = self.con.rollback
  
  def close(self):  
    self.rollback()
    self.cursor.close()
    self.con.close()
  
  def connect(self, srv = None, usr = None, psw = None, rol = None, enc = None):
    self.srv = normpath(srv) if srv else 'test'
    self.usr = usr or 'USER'
    self.psw = psw or 'PASS'
    self.rol = rol or 'ROLE'
    self.enc = enc or 'WIN1251'
    
    if self.srv == 'test':
      self.srv = 'test:/server'
    elif self.srv == 'prod':
      self.srv = 'prod:/server'
    
    self.con = fdb.connect(
      dsn = self.srv,
      sql_dialect = 3,
      user = self.usr,
      password = self.psw,
      role = self.rol,
      charset = self.enc
    )
    
    self.cursor = self.con.cursor()

def get_role(tbl, mode = 'R'):
  tbl = tbl.upper()
  mode = mode.upper()
  privileges = []
  
  if 'R' in mode:
    privileges.extend('S')
  
  if 'W' in mode:
    privileges.extend('UID')
  
  if '+' in mode:
    privileges.extend('R')

  db = fdbc(srv='real')
  db.execute('''
    select rdb$user from rdb$user_privileges
    where exists (select * from rdb$roles where rdb$role_name = rdb$user)
          and rdb$relation_name = '{}' and rdb$privilege in ({})
    group by rdb$user
    having count(rdb$privilege) >= {}
    '''.format(tbl, str(privileges)[1:-1], len(privileges)))

  roles = [row[0] for row in db.fetchall()]
  db.close()
  
  readable = False
  writeable = False
  
  for r in roles:
    f = fdbc(rol=r)
    try:
      f.execute('select count(*) from {} rows 1'.format(tbl))
      readable = True
    except fdb.fbcore.DatabaseError:
      pass
    
    if 'W' in mode:
      try:
        f.execute('insert into {} default values'.format(tbl))
        writeable = True
      except fdb.fbcore.DatabaseError:
        pass
      
      if not writeable:
        try:
          f.execute('insert into {t} select * from {t} rows 1'.format(t = tbl))
          writeable = True
        except fdb.fbcore.DatabaseError:
          pass
    
    f.close()
    
    if readable and not ('W' in mode and not writeable):
      return r.strip()
  
  return 'ROLE NOT FOUND'

def get_fields(tbl):
  tbl = tbl.upper()
  db = fdbc(srv='prod')
  db.execute('''
    select rdb$field_name
    from rdb$relation_fields
    where rdb$relation_name = '{}'
    order by rdb$field_position
    '''.format(tbl))

  fields = [row[0].strip() for row in db.fetchall()]
  db.close()
  
  return fields

def get_tables():
  db = fdbc(srv='prod')
  db.execute('''
    select trim(rdb$relation_name) from rdb$relations
    where rdb$system_flag = 0
    order by rdb$relation_name
    ''')

  tables = [row[0] for row in db.fetchall()]
  db.close()

  return tables
