#!/usr/bin/python
# -*- coding: utf-8 -*-
import os, cookielib, urllib2, time, sys, re, pickle, struct, readline, StringIO
import Image, glob, math, md5, cPickle, mx.DateTime, locale, urllib
datadir = os.path.dirname(sys.argv[0])
datafile = os.path.join(datadir, "pickle")

"""
Documentation sommaire :

Concepts de la DB : il y a des tables "définition", pour les
- villes
- provinces
- empires
- bâtiments (="classes" de bâtiments)
- objets (=tout ce qui peut se produire ou se ramasser)
- production (=règle qui dit les éléments et les UT nécessaires pour produire)

Et des tables "variables" pour les
- constructions (=bâtiments sur la map)
- stocks (=chaque objet vendable/productible dans un bâtiment)
- citoyens

Pour chaque table "variable", il y a une vue correspondante, qui permet de
s'y retrouver plus facilement. Cette vue réalise une jointure de toutes
les données disponibles, et ne conserve que l'entrée la plus récente.
Ainsi, la table kraland_stocks contient un historique, alors que la
table kraland_view_stocks contient l'état actuel des stocks.
"""

locale.setlocale(locale.LC_ALL, '')

class objify:
    def __init__(self, **kv):
        for k,v in kv.items(): setattr(self,k,v)
    def __call__(self, h={}, **kv):
        for k,v in h.items(): setattr(self,k,v)
        for k,v in kv.items(): setattr(self,k,v)
        return self.__dict__

def exceptpass(f):
    def ff(*l,**kv):
        try: return f(*l,**kv)
        except KeyboardInterrupt: raise
        except Exception,e:
            print "(Caught exception %s/%s)"%(e.__class__,str(e))
    return ff

def munge(something):
    if type(something)==type(u""): return something.encode("iso-8859-15")
    if type(something)==type([]): return [munge(x) for x in something]
    if type(something)==type(()): return tuple(munge(list(something)))
    return something

class SQL:
    phs={"qmark":"?", "pyformat":"%s"}
    typeDateTime = type(mx.DateTime.now())
    def __init__(self, dbtype):
        if dbtype=="pgsql":
            import psycopg
            self.dbco=psycopg.connect("")
            self.dbco.set_isolation_level(1)
            self.dbcu=self.dbco.cursor()
            self.dbph=SQL.phs[psycopg.paramstyle]
            self.dbnow="NOW()"
            return
        if dbtype=="sqlite":
            import pysqlite2.dbapi2
            self.dbco=pysqlite2.dbapi2.connect("kratool.db")
            self.dbcu=self.dbco.cursor()
            self.dbph=SQL.phs[pysqlite2.dbapi2.paramstyle]
            self.dbnow="datetime('now','localtime')"
            def adapt_str(s):
                # if you have declared this encoding at begin of the module
                return s.decode("iso-8859-15")
            pysqlite2.dbapi2.register_adapter(str, adapt_str)
            return
        self.dbco = None
        self.dbcu = None
    def __call__(self,query, *t):
        if not self.dbco: return []
        #print (query, t)
        t = [type(c)==typeDateTime and str(c) or c for c in t]
        self.dbcu.execute(query.replace("%s",self.dbph).replace("NOW()",self.dbnow), t)
        if not dbcu.description: return
        colnames = [c[0] for c in self.dbcu.description]
        return [dict(zip(colnames,munge(row))) for row in self.dbcu.fetchall()]
    def sqli(query, *t):
        cursor = self.dbco.cursor()
        cursor.execute(query.replace("%s",self.dbph).replace("NOW()",self.dbnow), t)
        if not cursor.description: return
        colnames = [c[0] for c in cursor.description]
        while 1:
            rows = cursor.fetchmany()
            if not rows: break
            for row in rows: yield dict(zip(colnames,munge(row)))

    def fetchall(self, *args):
        try: self.dbcu.fetchall(*args)
        except: return []
    @exceptpass
    def execute(self, *args):
        self.dbcu.execute(*args)
    @exceptpass
    def commit(self):
        self.dbco.commit()
    @exceptpass
    def rollback(self):
        self.dbco.rollback()
            

try: sql = SQL("pgsql")
except:
    print "Running without DB support"
    sql = SQL("")
            
def squash(rows, *keys):
    ht = {}
    for row in rows:
        ht[ tuple([row[k] for k in keys]) ] = row
    del rows[:] ; rows.extend(ht.values())
def order(rows, column, *columns):
    def mycmp(h1, h2, col, *cols):
        c = cmp(h1[col], h2[col])
        if cols: return c or mycmp(h1, h2, *cols)
        else: return c
    rows.sort(lambda h1, h2: mycmp(h1, h2, column, *columns))

def removesmileys(s):
    if s:
        s = re.sub("<[^>]+>", " ", s)
        #s = s.replace("*"," ")
        s = re.sub(" +", " ", s).strip()
    return s

statements = [
        """CREATE TABLE kraland_empires (
        empire_id smallint PRIMARY KEY,
        empire_nom varchar(64),
        empire_abbrev char(2),
        empire_impot_or smallint,
        empire_impot_salaire smallint,
        empire_impot_vente smallint,
        empire_timestamp timestamp without time zone
        );""",
        """CREATE TABLE kraland_provinces (
        province_id smallint PRIMARY KEY,
        province_nom varchar(64),
        province_impot smallint,
        province_autonome boolean,
        empire_id smallint REFERENCES kraland_empires,
        province_timestamp timestamp without time zone
        );""",
        """CREATE TABLE kraland_villes (
        ville_id smallint PRIMARY KEY,
        province_id smallint REFERENCES kraland_provinces,
        ville_nom varchar(64),
        ville_timestamp timestamp without time zone
        );""",
        """CREATE TABLE kraland_maps (
        province_id smallint NOT NULL REFERENCES kraland_provinces,
        map_pos varchar(4),
        map_type smallint,
        ville_id smallint UNIQUE REFERENCES kraland_villes,
        CHECK (map_type!=1 OR ville_id IS NOT NULL));""",
        """CREATE TABLE kraland_fonctions (
        PRIMARY KEY (empire_id, fonction_id),
        empire_id smallint NOT NULL REFERENCES kraland_empires,
        fonction_id smallint NOT NULL,
        fonction_masculin varchar(64),
        fonction_feminin varchar(64)
        );""",
        """CREATE TABLE kraland_citoyens (
        empire_id smallint NOT NULL REFERENCES kraland_empires,
        citoyen_id integer PRIMARY KEY,
        citoyen_nom varchar(250),
        citoyen_sex smallint NOT NULL,
        citoyen_level smallint NOT NULL,
        citoyen_fonction smallint NOT NULL,
        citoyen_area smallint NOT NULL,
        citoyen_money smallint NOT NULL,
        citoyen_link varchar(250),
        citoyen_timestamp timestamp without time zone
        );""",
        """CREATE TABLE kraland_adrs (
        citoyen_id integer NOT NULL REFERENCES kraland_citoyens,
        empire_id smallint NOT NULL REFERENCES kraland_empires,
        PRIMARY KEY (empire_id, citoyen_id),
        adr_jours smallint,
        adr_ep boolean,
        adr_prime smallint,
        adr_esclave boolean,
        adr_timestamp timestamp without time zone
        );""",
        """CREATE TABLE kraland_organisations (
        organisation_id integer PRIMARY KEY,
        organisation_nom varchar(250),
        organisation_type smallint NOT NULL,
        empire_id smallint NOT NULL REFERENCES kraland_empires,
        organisation_timestamp timestamp without time zone
        );""",
        """CREATE TABLE kraland_actions (
        organisation_id integer NOT NULL REFERENCES kraland_organisations,
        action_total integer NOT NULL,
        action_cours integer NOT NULL,
        action_dispo integer NOT NULL,
        action_timestamp timestamp without time zone
        );""",
        """CREATE TABLE kraland_pollutions (
        province_id integer NOT NULL REFERENCES kraland_provinces,
        pollution_indice integer NOT NULL,
        pollution_timestamp timestamp without time zone
        );""",
        """CREATE TABLE kraland_indices (
        PRIMARY KEY (empire_id),
        empire_id smallint NOT NULL REFERENCES kraland_empires,
        indice_economique smallint NOT NULL,
        indice_militaire smallint NOT NULL,
        indice_ideologique smallint NOT NULL,
        indice_scientifique smallint NOT NULL,
        indice_timestamp timestamp without time zone
        );""",
        """CREATE TABLE kraland_ecoles (
        ecole_id smallint PRIMARY KEY,
        ecole_nom varchar(32)
        );""",
        """CREATE TABLE kraland_nexus (
        ecole_id smallint REFERENCES kraland_ecoles,
        nexus_bonus smallint NOT NULL,
        nexus_timestamp timestamp without time zone
        );""",
        """CREATE TABLE kraland_batiments (
        batiment_id smallint PRIMARY KEY,
        batiment_nom varchar(64)
        );""",
        """CREATE TABLE kraland_objets (
        objet_id smallint PRIMARY KEY,
        objet_nom varchar(64),
        objet_millicharge smallint,
        production_par smallint,
        production_batiment smallint,
        production_niveau smallint,
        production_province smallint
        );""",
        """CREATE TABLE kraland_ressources (
        PRIMARY KEY (province_id, objet_id),
        province_id integer NOT NULL REFERENCES kraland_provinces,
        objet_id integer NOT NULL REFERENCES kraland_objets,
        ressource_cur smallint NOT NULL,
        ressource_max smallint NOT NULL,
        ressource_timestamp timestamp without time zone
        );""",
        """CREATE TABLE kraland_plans (
        PRIMARY KEY (objet_id),
        objet_id smallint NOT NULL REFERENCES kraland_objets,
        plan_prixindicatif smallint NOT NULL,
        plan_prixmax smallint NOT NULL,
        plan_cur smallint,
        plan_max smallint,
        plan_timestamp timestamp without time zone
        );""",
        """CREATE TABLE kraland_production (
        objet_id smallint NOT NULL REFERENCES kraland_objets,
        production_avec smallint,
        production_combien smallint
        );""",
        """CREATE TABLE kraland_constructions (
        PRIMARY KEY (construction_id),
        construction_id integer NOT NULL,
        ville_id smallint REFERENCES kraland_villes,
        batiment_id smallint REFERENCES kraland_batiments,
        construction_nom varchar(64),
        construction_pos varchar(4),
        citoyen_id integer REFERENCES kraland_citoyens,
        organisation_id integer REFERENCES kraland_organisations,
        construction_loyer integer,
        construction_prixvente integer,
        construction_prixindicatif integer,
        construction_salaire smallint,
        construction_caisse integer,
        construction_approx smallint,
        construction_pdb smallint,
        construction_timestamp timestamp without time zone
        );""",
        """CREATE TABLE kraland_stocks (
        construction_id integer REFERENCES kraland_constructions,
        objet_id smallint REFERENCES kraland_objets,
        PRIMARY KEY (construction_id, objet_id),
        stock_courant smallint,
        stock_max smallint,
        stock_prixhaut smallint,
        stock_prixbas smallint,
        stock_timestamp timestamp without time zone
        );""",
        """CREATE VIEW kraland_view_constructions AS SELECT
        construction_id, construction_timestamp,
        ville_id, ville_nom, ville_timestamp,
        province_id, province_nom, province_impot, province_autonome, province_timestamp,
        kraland_empires.empire_id, empire_nom, empire_abbrev, empire_impot_or, empire_impot_salaire, empire_impot_vente, empire_timestamp,
        batiment_id, batiment_nom, construction_nom, construction_pos,
        construction_loyer, construction_prixvente, construction_prixindicatif,
        construction_salaire, construction_caisse, construction_approx, construction_pdb,
        citoyen_id, citoyen_nom, organisation_id, organisation_nom,
        COALESCE(organisation_nom,citoyen_nom) AS construction_proprio,
        citoyen_nom AS construction_gerant,
        COALESCE(construction_caisse,construction_salaire*construction_approx,
                 0) AS construction_mincaisse,
        construction_salaire*(100-(province_impot+
            CASE WHEN province_autonome THEN 0 ELSE empire_impot_salaire END))
            /float4(100) AS construction_salairenet
        FROM kraland_constructions
        NATURAL JOIN kraland_batiments
        NATURAL JOIN kraland_villes
        NATURAL JOIN kraland_provinces
        NATURAL JOIN kraland_empires
        LEFT JOIN kraland_citoyens USING (citoyen_id)
        LEFT JOIN kraland_organisations USING (organisation_id)
        ;""",
        """CREATE VIEW kraland_view_stocks AS SELECT
        objet_id, objet_nom, objet_millicharge,
        production_par, production_niveau, production_batiment, 
        stock_courant, stock_max, stock_prixhaut, stock_prixbas, stock_timestamp,
        construction_id, construction_timestamp,
        ville_id, ville_nom, ville_timestamp,
        province_id, province_nom, province_impot, province_autonome, province_timestamp,
        kraland_empires.empire_id, empire_nom, empire_abbrev, empire_impot_or, empire_impot_salaire, empire_impot_vente, empire_timestamp,
        batiment_id, batiment_nom, construction_nom, construction_pos,
        construction_loyer, construction_prixvente, construction_prixindicatif,
        construction_salaire, construction_caisse, construction_approx, construction_pdb,
        citoyen_id, citoyen_nom, organisation_id, organisation_nom,
        construction_id AS stock_id,
        COALESCE(organisation_nom,citoyen_nom) AS construction_proprio,
        citoyen_nom AS construction_gerant,
        stock_max-stock_courant AS stock_libre,
        COALESCE(
            construction_caisse,construction_salaire*construction_approx,0)
            AS construction_mincaisse,
        construction_salaire*(100-(province_impot+
            CASE WHEN province_autonome THEN 0 ELSE empire_impot_salaire END))
            /float4(100) AS construction_salairenet
        FROM kraland_stocks
        NATURAL JOIN kraland_constructions
        NATURAL JOIN kraland_batiments
        NATURAL JOIN kraland_objets
        NATURAL JOIN kraland_villes
        NATURAL JOIN kraland_provinces
        NATURAL JOIN kraland_empires
        LEFT JOIN kraland_citoyens USING (citoyen_id)
        LEFT JOIN kraland_organisations USING (organisation_id)
        ;""",
        """CREATE VIEW kraland_view_citoyens AS SELECT
            empire_nom,citoyen_id,citoyen_nom,citoyen_level,fonction_id,
            CASE WHEN citoyen_sex=2
                 THEN fonction_feminin
                 ELSE fonction_masculin
                 END AS fonction,
            CASE WHEN fonction_id>50 AND fonction_id<100
                 THEN province_nom
                 WHEN fonction_id>100
                 THEN ville_nom
                 END as endroit
            FROM kraland_citoyens
            LEFT JOIN kraland_fonctions ON
              (kraland_citoyens.citoyen_fonction=kraland_fonctions.fonction_id
               AND kraland_citoyens.empire_id=kraland_fonctions.empire_id)
            JOIN kraland_empires ON
              (kraland_citoyens.empire_id=kraland_empires.empire_id)
            LEFT JOIN kraland_villes ON
              (citoyen_area=ville_id)
            LEFT JOIN kraland_provinces ON
              (citoyen_area=kraland_provinces.province_id)
        ;""",
        """CREATE VIEW kraland_view_villes AS SELECT * FROM kraland_villes
            NATURAL JOIN kraland_provinces
            NATURAL JOIN kraland_empires
        ;""",
        """CREATE VIEW kraland_view_maps AS SELECT
            int4(substr(map_pos, 2)) AS map_relx,
            ascii(substr(map_pos, 1, 1))-64 AS map_rely,
            (province_id-1)%%15 AS map_prvx,
            (province_id-1)/15 AS map_prvy,
            * FROM kraland_maps ;""",
        """CREATE VIEW kraland_view_map AS SELECT
            map_relx + map_prvx*20 + 10*(map_prvy%%2) AS map_absx,
            map_rely + map_prvy*13 AS map_absy,
            * FROM kraland_view_maps
            ;""",
        """CREATE TABLE kraland_biz_membres (
            nom varchar(64))
            ;""",
        """CREATE TABLE kraland_biz_prix (
            objet_id smallint REFERENCES kraland_objets,
            prix smallint)
            ;""",
        """CREATE TABLE kraland_forums (
            forum_id smallint PRIMARY KEY,
            forum_nom varchar(64) NOT NULL,
            forum_desc text NOT NULL,
            forum_crawl boolean NOT NULL DEFAULT 'f',
            forum_daysprivate smallint NOT NULL DEFAULT 0)
            ;""",
        """CREATE TABLE kraland_topics (
           forum_id smallint NOT NULL REFERENCES kraland_forums,
           topic_id integer PRIMARY KEY,
           topic_nom text NOT NULL,
           topic_locked boolean,
           topic_sticky boolean)
           ;""",
        """CREATE TABLE kraland_messages (
            topic_id integer NOT NULL REFERENCES kraland_topics,
            message_id integer PRIMARY KEY,
            citoyen_id integer,
            message_timestamp timestamp with time zone NOT NULL,
            message_text text NOT NULL)
            ;""",
        """CREATE TABLE kraland_membres (
            membre_id integer PRIMARY KEY,
            membre_html text NOT NULL,
            membre_timestamp timestamp with time zone NOT NULL,
            membre_pseudo_html text,
            membre_pseudo_smileys text,
            membre_pseudo_kramail text)
            ;""",
        """CREATE TABLE kraland_reports (
           membre_id integer NOT NULL REFERENCES kraland_membres,
           report_timestamp timestamp with time zone NOT NULL,
           report_order integer NOT NULL,
           report_text text NOT NULL)
           ;""",
        ]

def historize(table):
    s = [s for s in statements if "CREATE TABLE kraland_%s"%table in s]
    assert len(s)==1
    s = s[0]
    s = re.sub("CREATE TABLE kraland_", "CREATE TABLE kraland_history_", s)
    s = re.sub("PRIMARY KEY \([^)]+\),", "", s)
    s = re.sub("PRIMARY KEY","NOT NULL", s)
    s = re.sub("REFERENCES [^,]+,",",", s)
    sql(s)
    
try: sql("SELECT 1 FROM kraland_objets")
except:
    print "Je ne trouve pas la table kraland_objets."
    print "Je vais essayer de créer la structure SQL."
    sql.rollback()
    for statement in statements:
        sql(statement)
    for histo in ("empire", "province", "ville", "citoyen", "adr",
                  "action", "ressource", "indice", "nexus",
                  "construction", "stock", "plan", "pollution"):
        historize(histo)
    sql.commit()
    print "Et voilà le travail! Maintenant, essayez de lancer 'def'."

name2id = {} ; id2name = {}
def refreshmappings():
    for table in ("ville", "province", "objet", "batiment", "empire",
                  "organisation", "citoyen"):
        table_id = {} ; table_nom = {}
        sql.execute("SELECT %s_id, %s_nom FROM kraland_%ss"%
                     (table,table,table))
        for def_id, def_nom in sql.fetchall():
            def_nom = munge(def_nom)
            table_id[def_nom]=def_id
            table_id[def_nom.lower()]=def_id
            table_id["-"+def_nom]=-def_id
            table_id["-"+def_nom.lower()]=-def_id
            table_nom[-def_id]='-'+def_nom
            table_nom[def_id]=def_nom
        name2id[table]=table_id ; id2name[table]=table_nom
    class inthash:
        def __getitem__(self, x): return int(x)
    class strhash:
        def __getitem__(self, x): return str(x)
    name2id["construction"] = inthash()
    id2name["construction"] = strhash()
refreshmappings()

obj2bat = {}
def add_obj2bat(obj_id,bat_id,levels):
    bat_id=bat_id/10*10
    if obj_id not in obj2bat: obj2bat[obj_id]=[]
    for lvl in levels:
        if bat_id+lvl not in obj2bat[obj_id]:
            obj2bat[obj_id].append(bat_id+lvl)
def deal_is_applicable(row):
    if not obj2bat: # initialize this !
        for prod in sql("SELECT * FROM kraland_production "
                         "NATURAL JOIN kraland_objets"):
            if prod["production_batiment"]:
                # ajouter le lieu de production
                add_obj2bat(prod["objet_id"],
                            prod["production_batiment"],
                            range(10))
                # ajouter les lieux qui utilisent l'objet
                add_obj2bat(prod["production_avec"],
                            prod["production_batiment"],
                            range(10))
    # matériaux de construction
    if row["objet_nom"] in ["Métal","Planche","Brique","Verre"]:
        # si le bâtiment est de niveau <4: OK
        if row["a_batiment_id"]%10<4: return True
        # si le bâtiment les utilise/produit: OK
        if row["a_batiment_nom"] in obj2bat[row["objet_id"]]: return True
        # sinon, pas OK
        return False
    # autres objets
    # pas de pièce commerce: pas OK
    if row["a_batiment_nom"] in [
        "Poste de Police", "Commissariat", "Poste de Garde", "Caserne"]:
        return False
    # le reste: OK
    return True

def migrate_1_cit():
    createtable = [s for s in statements if "CREATE TABLE kraland_citoyens" in s][0]
    sql("ALTER TABLE kraland_citoyens RENAME TO kraland_history_citoyens")
    sql(createtable)
    total = sql("SELECT COUNT(*) AS c FROM kraland_history_citoyens")[0]["c"]
    count = 0
    for cit in sql("SELECT * FROM kraland_history_citoyens"):
        cit["citoyen_timestamp"]=str(cit["citoyen_timestamp"])
        cit_id = cit["citoyen_id"]
        del cit["citoyen_id"]
        update_row("citoyen", cit_id, timestamp=False, unique=True, alsohistory=True, **cit)
        count += 1
        if count%(total/count)==0: sys.stdout.write("%d/%d"%(count,total))
    assert 0 # WARNING WARNING
    sql.commit()

def migrate_2_cons():
    ok=True
    print "Loading cits+orgas..."
    cits = dict([(row["citoyen_nom"].lower(), row["citoyen_id"])
                 for row in sql("SELECT citoyen_nom, citoyen_id "
                                "FROM kraland_citoyens")])
    orgas = dict([(row["organisation_nom"].lower(), row["organisation_id"])
                  for row in sql ("SELECT organisation_nom, organisation_id "
                                  "FROM kraland_organisations")])
    both = cits.copy()
    both.update(orgas)
    print "Checking cons..."
    for cons in sql("SELECT * FROM kraland_view_constructions"):
        if cons["construction_proprio"]=="(sans)":
            #print "(sans)\t", dispcons(cons)
            pass
        elif cons["construction_proprio"]==None:
            print "None\t", dispcons(cons)
        elif cons["construction_proprio"].lower() not in both:
            print "?PROPRIO?\t", dispcons(cons)
            #update_construction(cons["construction_id"])
        if cons["construction_gerant"] and cons["construction_gerant"].lower() not in cits:
            print "?GERANT?\t", dispcons(cons)
            #update_construction(cons["construction_id"])
            
    #return    

    print "Creating new table..."
    createtable = [s for s in statements if "CREATE TABLE kraland_constructions" in s][0]
    sql("ALTER TABLE kraland_constructions RENAME TO kraland_constructions_old")
    sql(createtable)
    print "Migrating..."
    total = sql("SELECT COUNT(*) AS c FROM kraland_constructions_old")[0]["c"]
    count = 0
    for c in sql("SELECT * FROM kraland_constructions_old"):
        c["citoyen_id"], c["organisation_id"] = None, None
        if c["construction_proprio"]:
            if c["construction_proprio"].lower() in cits:
                c["citoyen_id"] = cits[c["construction_proprio"].lower()]
            if c["construction_proprio"].lower() in orgas:
                c["organisation_id"] = orgas[c["construction_proprio"].lower()]
        if c["construction_gerant"]:
            if c["construction_gerant"].lower() in cits:
                c["citoyen_id"] = cits[c["construction_gerant"].lower()]
        cons_id=c["construction_id"]
        del c["construction_id"]
        c["construction_timestamp"]=str(c["construction_timestamp"])
        del c["construction_proprio"], c["construction_gerant"]
        update_row("construction", cons_id, timestamp=False, unique=False, **c)
        count += 1
        if 0==count%(total/100): print "%d/%d"%(count,total)

    sql.commit()

def migrate_3_meta():
    updatedef("empire", [[0, "Zorglub's Zorgland"]])
    update_row("citoyen", 1000000,
               timestamp=True, unique=True, alsohistory=True,
               citoyen_nom="(sans)", citoyen_sex=-1, citoyen_level=-1,
               citoyen_fonction=-1, citoyen_area=-1, citoyen_money=-1,
               citoyen_link='', empire_id=0)

def migrate_4_stocks():
    sql("ALTER TABLE kraland_stocks RENAME TO kraland_history_stocks")
    sql([s for s in statements if "CREATE TABLE kraland_stocks" in s][0])
    sql.commit()

def migrate_5_conshist():
    s = [s for s in statements if "CREATE TABLE kraland_constructions" in s][0]
    s = s.replace("kraland_constructions","kraland_history_constructions")
    s = s.replace("PRIMARY KEY","")
    sql(s)

class kracon:
    def __init__(self, cookiefile=None):
        if cookiefile and os.path.isfile(cookiefile):
            self.cj = cookielib.MozillaCookieJar()
            self.cj.load(cookiefile)
        else:
            self.cj = cookielib.CookieJar()
        self.opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(self.cj))
        self.verbose = True
    def getcookie(self, name):
        try: r = self.cj._cookies['www.kraland.org']['/'][name].value
        except: r = "-"
        return r
    def __call__(self, url, *postdata):
        if self.verbose:
            print >>sys.stderr, "Using account",
            print >>sys.stderr, self.getcookie('citoyen'),
            print >>sys.stderr, self.getcookie('citoyen_id'),
            print >>sys.stderr, self.getcookie('pc_id')
            print >>sys.stderr, "Opening", url, "...",
        req = self.opener.open(url, *postdata)
        if self.verbose:
            print >>sys.stderr, "Reading", "...",
        data = req.read()
        if self.verbose:
            print >>sys.stderr, "Done."
        return data

tmp = map(lambda a: (os.stat(a).st_mtime, a), glob.glob("%s/.mozilla/firefox/*/cookies.txt"%os.environ["HOME"]))
tmp.sort()
if tmp: cookiefile = tmp[-1][1]
else: cookiefile = "cookies.txt"
kraget=kracon(cookiefile)

@exceptpass
def update_construction(cons_id):
    vid = sql("SELECT ville_id FROM kraland_constructions "+
              "WHERE construction_id=%s", cons_id)[0]["ville_id"]
    data=kraget("http://www.kraland.org/order.php?p1=1301&p2=%d&p3=%s"%
                (cons_id,vid))
    if "Unknown column 'ill' in 'field list'" in data:
        print "Deleted."
        sql("DELETE FROM kraland_stocks "
            "WHERE construction_id=%s",
            cons_id)
        sql("DELETE FROM kraland_constructions "
            "WHERE construction_id=%s",
            cons_id)
        sql.commit()
        return
    #print data
    if ">Salaire :" not in data:
        print "Special."
        sql("UPDATE kraland_constructions "
            "SET construction_timestamp=NOW() "
            "WHERE construction_id=%s",
            cons_id)
        sql.commit()
        return
    salaire = re.findall(r">Salaire : ([0-9]+) ", data)[0]
    caissedata = re.findall(r">Caisse\W+\(([^)]+)\)", data)[0]
    pdb = re.findall(r"<p>Points de Bâtiment : ([0-9]+)/100<p>", data)
    if pdb: pdb=pdb[0]
    proprio, gerant, prixindicatif = None, None, None
    if "Gestionnaire" in data:
        proprio, gerant, prixindicatif = re.findall(r"Propriétaire : (.*) - Gestionnaire : (.*)</p><p>.*</p><p>Valeur Indicative : ([0-9]+) ", data)[0]
    elif "Propriétaire" in data:
        proprio, prixindicatif = re.findall(r"Propriétaire : (.*)</p><p>.*</p><p>Valeur Indicative : ([0-9]+) ", data)[0]
    organisation_id, citoyen_id = ocpg(proprio, gerant)
    #print repr(caissedata)
    caisse = None ; approx = None
    if "plus de" in caissedata: approx="9"
    elif "salaire" in caissedata: approx=caissedata.split(" ")[0]
    else: caisse=caissedata.split(" ")[0]
    update_row("construction", int(cons_id),
               timestamp=True, unique=True, alsohistory=True,
               organisation_id=organisation_id, citoyen_id=citoyen_id,
               construction_caisse=caisse, construction_salaire=salaire,
               construction_approx=approx, construction_pdb=pdb,
               construction_prixindicatif=prixindicatif)
        
    objs=re.findall(r'/([0-9]+).gif"[^(]+\(([0-9]+)/([0-9]+)\)( \[ill\])?</p>(<p class="priceind">Productible au niveau [1-4]</p>)?(<p class="compact">construction: [0-9]+/[0-9]+</p>)?</td><td class="gametdcomp"><p class="price">([0-9]+)(/([0-9]+))?</p>', data)
    for obj_id, stk_cur, stk_max, ill, productible_au_niveau, construction, prixhaut, slashprixbas, prixbas in objs:
        if prixbas=="": prixbas=0
        if int(prixhaut)>32767: prixhaut=32767
        update_row("stock", (int(cons_id), int(obj_id)),
                   timestamp=True, unique=True, alsohistory=True,
                   stock_courant=stk_cur, stock_max=stk_max,
                   stock_prixhaut=prixhaut, stock_prixbas=prixbas)
    sql.commit()

def ocpg(proprio,gerant):
    proprio = proprio or "(sans)"
    gerant  = gerant  or "(sans)"
    proprio = removesmileys(proprio).lower()
    gerant  = removesmileys(gerant).lower()
    if proprio in name2id["organisation"]:
        organisation_id = name2id["organisation"][proprio]
        citoyen_id = name2id["citoyen"][gerant]
    else:
        organisation_id = None
        citoyen_id = name2id["citoyen"][proprio]
    return organisation_id,citoyen_id

def parseoperacookies():
    f = open(os.environ["HOME"]+"/.opera/cookies4.dat")
    f.read(4+4) # skip file version number and app version number
    f.read(2+2) # skip idtag length(=1) and lengthlength(=2)
    idtag_length = 1 ; length_length = 2
    def readrecord():
        maintype=f.read(1)
        if not maintype: return None
        if ord(maintype)>0x80: return [maintype]
        mainlength=struct.unpack("!H",f.read(2))[0]
        #print mainlength
        packet=f.read(mainlength)
        payload=[maintype]
        while packet:
            print repr(packet)
            if ord(packet[0])>0x80:
                payload.append((packet[0],""))
                packet=packet[1:]
                continue
            subtype=packet[0:2]
            sublength=ord(packet[2])
            subdata=packet[3:3+sublength]
            packet=packet[3+sublength:]
            payload.append((subtype,subdata))
        return payload
    while 1:
        rec=readrecord()
        if not rec: return
        yield rec
def convertoperacookies(outputfile):
    f=open(outputfile,"w")
    f.write("# HTTP Cookie File\n\n")
    for rec in parseoperacookies():
        if rec[0]!="\x03": continue
        h=dict(rec[1:])
        name=h["\x10\x00"] ; value=h.get("\x11\x00","")
        expiry=int(time.time()+3600)
        if name in ["citoyen_id", "pc_id", "citoyen", "citoyen_pass"]:
            print >>f, "\t".join(["www.kraland.org", "FALSE", "/", "FALSE",
                                  str(expiry), name, value])
            print name
    f.close()

def sanitize(h):
    for k in h:
        if type(h[k])==type(""):
            h[k]=h[k].replace("&#39;","'")

def updatedef(base, defs):
    for def_id, def_nom in defs:
        print def_nom
        kv = {"%s_nom"%base : def_nom }
        update_row(base, def_id, **kv)
    sql.commit()

def updatedef_ecoles():
    ecoles = ["Enchantement","Nécromancie", "Illusion", "Divination",
              "Magie Vitale", "Démonologie", "Élémentalisme"]
    updatedef("ecole", [[ecoles.index(ecole)+1, ecole] for ecole in ecoles])

def updatedef_objets():
    data=kraget("http://www.kraland.org/main.php?page=1;1;2;123;0")
    rows=re.findall("<tr>(.*)</tr>", data)
    for row in rows:
        objs=re.findall(r'/([0-9]+).gif" width=32 height=32 alt="([^"]+)"><',
                        row)
        if len(objs)==0: continue
        assert len(objs)==1
        updatedef("objet", objs)
    refreshmappings()
    for row in rows:
        objs=re.findall(r'/([0-9]+).gif"', row)
        if not objs: print repr(objs) ; continue
        obj_id = objs[0]
        if obj_id=="1000": continue # cas spécial Trésor
        charge = re.findall(r'Charge ([0-9.]+)[< ]', row)
        if charge: sql("UPDATE kraland_objets "+
                       "SET objet_millicharge=%s "+
                       "WHERE objet_id=%s",
                       int(1000*float(charge[0])), obj_id)
        productionpar = re.findall(r'\[production par ([0-9]+)\]', row)
        if productionpar: productionpar=productionpar[0]
        else: productionpar="1"
        sql("UPDATE kraland_objets "+
            "SET production_par=%s "+
            "WHERE objet_id=%s",
            productionpar, obj_id)
        productionbat = re.findall(r'Production:</u> (.*) niveau ([0-9])</p>',
                                   row)
        if productionbat and productionbat[0][0]:
            sql("UPDATE kraland_objets "+
                "SET production_batiment=%s, production_niveau=%s "+
                "WHERE objet_id=%s",
                name2id["batiment"][productionbat[0][0]],
                productionbat[0][1],
                obj_id)
        productionprov = re.findall("<u>Province:</u> ([^<]+)<", row)
        if productionprov:
            pass # XXX
        production = re.findall(r'<p class="compact">([0-9]+) ([^<]+)</p>',row)
        if production:
            sql("DELETE FROM kraland_production WHERE objet_id=%s",
                obj_id)
            for qte, ref in production:
                if ref in ["Unité de Travail", "Unités de Travail"]: ref = 0
                else: ref = name2id["objet"][ref]
                sql("INSERT INTO kraland_production "+
                    "(objet_id, production_avec, production_combien)"+
                    " VALUES (%s, %s, %s)", obj_id, ref, qte)
    sql.commit()
        
def updatedef_batiments():
    data=kraget("http://www.kraland.org/main.php?page=1;1;2;122;0")
    bats=re.findall(r'/([0-9]+).gif" width=32 height=32 alt="([^"]+)"><', data)
    updatedef("batiment", bats)

def updatedef_provinces():
    data=kraget("http://www.kraland.org/map.php?map=1;1;0")
    data=data.split("Sélectionner une Ville")[0]
    provs=re.findall(r'<option value="([0-9]+)" class="[^"]+"> ([^<]+)</',data)
    updatedef("province", provs)
    for provid in range(1,196):
        if not sql("SELECT 1 FROM kraland_provinces WHERE province_id=%s",
                   provid):
            data = kraget("http://www.kraland.org/map.php?map=1;%d;0"%provid)
            provname = re.findall("<h3>(.*)</h3>", data)[0]
            updatedef("province", [(provid, provname)])

def updatedef_empires():
    updatedef("empire", [[0, "Zorglub's Zorgland"]])
    for country in range(1,9):
        data=kraget("http://www.kraland.org/main.php?page=1;2;2;%d;0"%country)
        countryname = re.findall(r'"gameth" colspan=2>([^<]+)</th>', data)[0]
        updatedef("empire", [[country, countryname]])
        countryabbrev = re.sub("[^A-Z]","",countryname)
        sql("UPDATE kraland_empires SET empire_abbrev=%s "
            "WHERE empire_id=%s", countryabbrev, country)

        impots=re.findall(r'<p class="compact">Impôts : ([0-9]+)%</p><p class="compact">Taxe à la vente : ([0-9]+) %</p><p class="compact">Taxe production d.or : ([0-9]+) %<', data)
        sql("UPDATE kraland_empires SET empire_impot_salaire=%s, "
            "empire_impot_vente=%s, empire_impot_or=%s WHERE empire_id=%s",
            impots[0][0], impots[0][1], impots[0][2], country)

        provs=re.findall(r'>([^<]+)</a>( \[autonome\])?</td><td class="gametd"[^>]*>[^%]+<td class="gametdcomp"[^>]*>([0-9]+)%</td></tr', data)
        for province_nom, province_autonome, province_impot in provs:
            autonome = province_autonome and "true" or "false"
            sql("UPDATE kraland_provinces "
                "SET province_impot=%s, province_autonome=%s "
                "WHERE province_nom=%s",
                province_impot, autonome, province_nom)
        
        villes=re.findall(r'>([^<]+)</a> \(([^<]+)\)<', data)
        for ville_nom, province_nom in villes:
            ville_id=name2id["ville"][ville_nom]
            province_id=name2id["province"][province_nom]
            sql("UPDATE kraland_villes SET province_id=%s "
                "WHERE ville_id=%s", province_id, ville_id)
            sql("UPDATE kraland_provinces SET empire_id=%s "
                "WHERE province_id=%s", country, province_id)

        sql.commit()

def updatedef_villes():
    data=kraget("http://www.kraland.org/map.php?map=1;1;0")
    data=data.split("Sélectionner une Ville")[1]
    vils=re.findall(r'<option value="([0-9]+)" class="[^"]+"> ([^<]+)</',data)
    updatedef("ville", vils)

def update_ville(nom_ou_id):
    try: ville_id=int(nom_ou_id)
    except: ville_id=name2id["ville"][nom]
    data=kraget("http://www.kraland.org/map.php?map=1;0;%d"%ville_id)
    lines = data.split("<tr>")
    for line in lines:
        #print line
        data1 = re.findall(r'/([0-9]+).gif"[^>]+></td><td class="gametd"><a href="order.php\?p1=1301&amp;p2=([0-9]+)&amp;p3=[0-9]+" >([^(]+) \(([A-Z][0-9]+)\)<', line)
        if not data1:
            data1 = re.findall(r'/([0-9]+).gif"[^>]+></td><td class="gametd"><a href="order.php\?p1=1301&amp;p2=([0-9]+)&amp;p3=[0-9]+" onclick="[^"]+" >([^(]+) \(([A-Z][0-9]+)\)<', line)
        data2 = re.findall(r'>Bâtiment Public - salaire ([0-9]+) ..</p>', line)
        data3 = re.findall(r'>(Gestionnaire|Propriétaire) : (.*) - salaire ([0-9]+) ..</p>', line)
        data4 = re.findall(r'(À louer|En vente) : ([0-9]+) ', line)
        if not data1: continue
        if not (data2 or data3):
            print line
            print "Skipping"
            continue
        bat_type, cons_id, cons_nomcomplet, cons_pos = data1[0]
        if data2:
            proprio, gerant, salaire = None, None, data2[0]
        elif data3[0][0]=='Gestionnaire':
            proprio, gerant, salaire = None, data3[0][1], data3[0][2]
        else:
            proprio, gerant, salaire = data3[0][1], None, data3[0][2]
        prixvente, prixlouer = None, None
        if data4 and data4[0][0]=='En vente': prixvente = data4[0][1]
        if data4 and data4[0][0]=='À louer': prixlouer = data4[0][1]
        if "<I>" in cons_nomcomplet:
            cons_nom = re.findall("<I>(.*)</I>", cons_nomcomplet)[0]
        else:
            cons_nom = ""
        orga_id, cit_id = ocpg(proprio,gerant)
        update_row("construction", cons_id, timestamp=False,
                   ville_id=ville_id, batiment_id=bat_type,
                   construction_nom=cons_nom, construction_pos=cons_pos,
                   organisation_id=orga_id, citoyen_id=cit_id,
                   construction_loyer=prixlouer, construction_salaire=salaire,
                   construction_prixvente=prixvente)
    sql("UPDATE kraland_villes SET ville_timestamp=NOW() "
        "WHERE ville_id=%s", ville_id)
    sql.commit()

def update_row(table, rowid,
               timestamp=False, unique=True, alsohistory=False, **kv):
    assert kv
    if rowid==Ellipsis:
        rowid = kv[table+"_id"]
        del kv[table+"_id"]
    kv = kv.items() ; columns = [k for k,v in kv] ; values = [v for k,v in kv]
    if table=="stock":
        pkey="construction_id,objet_id"
        rowid="%d,%d"%rowid
    elif table=="indice":
        pkey="empire_id"
    else:
        pkey = table+"_id" 
    if (unique and
        sql("SELECT 1 FROM kraland_%ss WHERE (%s)=(%s)"%(table,pkey,rowid))):
        sql("UPDATE kraland_%ss SET "%table+
            ", ".join(["%s=%%s"%c for c in columns])+
            (timestamp and ", %s_timestamp=NOW()"%table or "")+
            " WHERE (%s)=(%s)"%(pkey,rowid),
            *(tuple(values)))
    else:
        sql("INSERT INTO kraland_%ss (%s, "%(table,pkey)+
            ", ".join(columns)+
            (timestamp and ", %s_timestamp"%table or "")+
            ") VALUES (%s, "%rowid+
            ", ".join(["%s" for x in columns])+
            (timestamp and ", NOW()" or "")+
            ")", *values)
    if alsohistory:
        sql("INSERT INTO kraland_history_%ss (%s, "%(table,pkey)+
            ", ".join(columns)+
            (timestamp and ", %s_timestamp"%table or "")+
            ") VALUES (%s, "%rowid+
            ", ".join(["%s" for x in columns])+
            (timestamp and ", NOW()" or "")+
            ")", *values)


def update_constructions(nom_ou_id, extrasql="1=1"):
    try: id_ville=int(nom_ou_id)
    except: id_ville=name2id["ville"][nom_ville]
    r=sql("SELECT construction_id FROM kraland_constructions "+
          "WHERE ville_id=%s AND ("+extrasql+")", id_ville)
    cons_id_list = [row["construction_id"] for row in r]
    for cons_id in cons_id_list:
        update_construction(cons_id)
    sql.commit()

def update_justice():
    for empire in range(1,9):
        data = kraget("http://www.kraland.org/map.php?map=8;%d;0"%empire)
        print data
    h = {}
    
    #<tr><td class="gametd">Alejandro Valverde</td><td class="gametdcomp">3</td></tr

    hdr, adrs = data.split('<th class="gameth">Personnes Recherch')
    adrs, eps = data.split('<tr><th class="gameth">Ennemis Public')
    eps, prim = data.split('<tr><th class="gameth">Primes</th><th')
    prim, esc = data.split('<tr><th class="gameth">Esclaves en Ve')
    
    #for cit_nom, cit_adr in re.findall('<tr><td class="gametd">(.*)Alejandro Valverde</td><td class="gametdcomp">3</td></tr>'    

def cleanup_(table, pkey, *cols):
    cols = ",".join((pkey,)+cols)
    minmaxtemplate = ("SELECT %(qual)s(%(table)s_timestamp), %(cols)s "
                      "FROM kraland_%(table)ss "
                      "WHERE %(pkey)s=%%s "
                      "GROUP BY %(cols)s")
    qual = "min" ; mintemplate = minmaxtemplate%locals()
    qual = "max" ; maxtemplate = minmaxtemplate%locals()
    for pkeyvalue in [row[pkey]
                      for row in sql("SELECT DISTINCT %(pkey)s "
                                     "FROM kraland_%(table)ss"%locals())]:
        print pkeyvalue
        sql("DELETE FROM kraland_%(table)ss "
            "WHERE %(pkey)s=%%s "
            "  AND (%(table)s_timestamp,%(cols)s) NOT IN (%(mintemplate)s) "
            "  AND (%(table)s_timestamp,%(cols)s) NOT IN (%(maxtemplate)s) "
            %locals(), pkeyvalue,pkeyvalue,pkeyvalue)
        sql.commit()
    
def cleanup_citoyens():
    cleanup_('citoyen', 'citoyen_id', 'empire_id', 'citoyen_nom',
             'citoyen_sex', 'citoyen_level', 'citoyen_fonction',
             'citoyen_area', 'citoyen_money', 'citoyen_link')

def update_citoyens():
    update_row("citoyen", 1000000,
               timestamp=True, unique=True, alsohistory=True,
               citoyen_nom="(sans)", citoyen_sex=-1, citoyen_level=-1,
               citoyen_fonction=-1, citoyen_area=-1, citoyen_money=-1,
               citoyen_link='',empire_id=0)
    def recparse(node, empire):
        if node.localName!='item':
            for child in node.childNodes:
                recparse(child, empire)
            return
        cit = {"empire_id": empire}
        for child in node.childNodes:
            if not child.localName: continue
            k = str(child.localName)
            if k=="name": k="nom"
            cit["citoyen_"+k]=(
                child.childNodes[0].nodeValue.encode("latin-1"))
        del cit["citoyen_org1"]
        cit_id = cit["citoyen_id"]
        del cit["citoyen_id"]
        #print cit
        update_row("citoyen", cit_id, timestamp=True, unique=True, alsohistory=True, **cit)
        
    import Ft.Xml
    for empire in range(1,9):
        print "Getting empire", empire
        recparse(Ft.Xml.Parse("http://www.kraland.org/xml/cit%d.xml"%empire),
                 empire)
    sql.commit()

def update_organisations():
    for empire in range(1,9):
        print "Orgas", empire
        o_type =3
        data = kraget("http://www.kraland.org/main.php?page=1;2;3;%d;%d"%(empire,o_type))
        for div in data.split('<div class="bigcadre">'):
            o_data = re.findall(r'p1=4100&amp;p2=([0-9]+)"[^>]*>(.*) '
                                r'\(indice de gloire : ([0-9]+)\)'
                                r'(?s).*>([0-9]+) actions . ([0-9]+) .. '
                                r'\(([0-9]+) encore sur le march', div)
            for o_id, o_name, o_ig, o_a_total, o_a_value, o_a_avail in o_data:
                print o_name
                update_row("organisation", o_id, timestamp=False, unique=True,
                           organisation_nom = o_name, empire_id = empire,
                           organisation_type=o_type)
    sql.commit()

def p2xy(p):
    return (p-1)%15, (p-1)/15
def xy2p((x,y)):
    if x>=0 and x<=14 and y>=0 and y<=12:
        return y*15+x+1
    else:
        return None
def provneigh(p):
    x,y = p2xy(p)
    n = [ (x+1,y) , (x-1,y) , (x,y+1) , (x,y-1) ]
    if y%2: n.extend([ (x+1,y-1) , (x+1,y+1) ])
    else: n.extend([   (x-1,y-1) , (x-1,y+1) ])
    return [xy2p(xy) for xy in n if xy2p(xy)]
provdistcache={}
def provdist(p1, p2):
    if p1>p2: p1,p2=p2,p1
    if (p1,p2) not in provdistcache:
        distance = 0
        provset = [p1]
        while p2 not in provset:
            provset = sum([provneigh(p) for p in provset],[])
            provset = dict(zip(provset,provset)).keys()
            distance += 1
        provdistcache[p1,p2]=distance
    return provdistcache[p1,p2]
    
def makecoords(prov, pos):
    line = 15
    prov = prov-1
    x = prov%line
    y = prov/line
    x *= 20
    if y%2: x += 10
    y *= 13
    x += int(pos[1:])
    y += ord(pos[0])-64
    return x,y

def makepath(*components):
    directories = components[:-1]
    for n in range(len(directories)):
        dirpath = os.path.join(*directories[:n+1])
        if not os.path.isdir(dirpath): os.mkdir(dirpath)
    return os.path.join(*components)

class kramages:
    def __init__(self, scale=32, theme=1):
        self.scale = scale
        self.theme = theme
        self.iconcache = {}
    def makeurl(self, i):
        return "http://90plan.ovh.net/~kramages/1/map/%d/%03d.gif"%(
            self.theme, i)
    def makepath(self, scale, i, ext):
        return makepath("MAP", "THEME_%d"%self.theme, "SCALE_%d"%scale,
                        "%03d.%s"%(i, ext))
    def __getitem__(self, i):
        if i not in self.iconcache:
            scaledfilename = self.makepath(self.scale, i, "png")
            originalfilename = self.makepath(0, i, "gif")
            if not os.path.isfile(scaledfilename):
                if not os.path.isfile(originalfilename):
                    data = urllib2.urlopen(self.makeurl(i)).read()
                    print "Loading images %s from website"%i
                    open(originalfilename,"w").write(data)
                img = Image.open(originalfilename)
                img = img.resize((self.scale,self.scale)).save(scaledfilename)
            self.iconcache[i] = Image.open(scaledfilename)
        return self.iconcache[i]
    def forcecacheload(self):
        for row in sql("SELECT DISTINCT map_type AS i FROM kraland_maps "
                       "UNION "
                       "SELECT DISTINCT batiment_id FROM kraland_batiments"):
            self[row["i"]]

class kramap:
    def __init__(self, scale=32, theme=1):
        self.scale = scale
        self.theme = theme
        self.kramages = kramages(scale, theme)
        self.filename = makepath("MAP", "THEME_%d"%theme, "SCALE_%d"%scale,
                                 "map.png")
        if not os.path.isfile(self.filename):
            self.img = Image.new("RGB",(scale*320,scale*200))
            for tile in sql("SELECT * FROM kraland_maps"):
                coords = makecoords(tile["province_id"],tile["map_pos"])
                self[coords] = self.kramages[tile["map_type"]]
            self.img.save(self.filename)
        else:
            self.img = Image.open(self.filename)
    def __setitem__(self, (x,y), icon):
        self.img.paste(icon, (x*self.scale,y*self.scale))

def update_map(prov=0):
    if not prov:
        for prov in [row["province_id"] for row in 
                     sql("SELECT province_id FROM kraland_provinces")]:
            #if not sql("SELECT 1 FROM kraland_maps WHERE province_id=%s"%prov):
                update_map(prov)
        return
    htmldata = kraget("http://www.kraland.org/map.php?map=1;%d;0"%prov)
    data = re.findall('<img src="http://90plan.ovh.net/~kramages/1/map/[0-9]/([0-9]+).gif"( alt="([^"]+)" title="[^"]+" border=0)?>',htmldata)

    if len(data)!=260:
        print htmldata
        print 79*"*"
        print data
        assert 0
    sql("DELETE FROM kraland_maps WHERE province_id=%s", prov)
    coords=[]
    for y in "ABCDEFGHIJKLM":
        for x in range(1,21):
            coords.append("%s%d"%(y,x))
    print coords
    for pos, (imgid, junk, city) in zip(coords,data):
        print pos, imgid
        if city: city=name2id["ville"].get(city, None)
        else: city=None
        sql("INSERT INTO kraland_maps "
            "(province_id, map_pos, map_type, ville_id) "
            "VALUES (%s, %s, %s, %s)",
            prov, pos, imgid, city)
    sql.commit()
        
def updatedef_fonctions():
    for empire in range(1,9):
        print "Empire", empire
        for f_id, f_male, f_female in re.findall(r'fonctionlist\[([0-9]+)\] = new Array\( "([^"]+)", "([^"]+)" \);', urllib2.urlopen("http://www.kraland.org/js/cit%d.js"%empire).read()):
            update_row("fonction", f_id, unique=False, empire_id=empire,
                       fonction_masculin=f_male, fonction_feminin=f_female)
    sql.commit()

def update_ddp(nom_ville):
    update_constructions(nom_ville, "batiment_id>=3000 AND batiment_id<3300")
def update_commerce(nom_ou_id_ville):
    update_constructions(nom_ou_id_ville, "batiment_id<5000")

def update_indices():
    data = kraget("http://www.kraland.org/main.php?page=1;0;0;0;0&p0=9")
    i=re.findall('>(I[EMIS])</th><td class="gametd">([0-9]+) \([^)]+\) <',data)
    assert len(i)==32
    m = {"E":"economique","M":"militaire","I":"ideologique","S":"scientifique"}
    for empire_id in range(8):
        update_row("indice", 1+empire_id,
                   alsohistory=True, unique=True, timestamp=True,
                   **dict([("indice_"+m[k[1]], v)
                           for (k,v) in i[4*empire_id:4+4*empire_id]]))
    sql.commit()
    
def crawl(table, updatefunc, _params, sqlcontext="1"):
    if table in ["construction","ville","stock"]:
        view = "view_"
    else:
        view = ""
    params = { "interval":"2", "range":"604800" }
    if "-f" in _params: params.update({"interval":0, "range":1000})
    if "-F" in _params: params.update({"interval":0, "range":10})
    params.update(dict([kv.split("=",1) for kv in _params.split(" ") if "=" in kv]))
    range = int(params["range"]) ; interval = int(params["interval"])
    errors = 0
    done = set()
    while 1:
        try:
            histo=sql("SELECT COUNT(*),"
                      "  date_trunc('day',NOW()-%(table)s_timestamp) "
                      "FROM kraland_%(view)s%(table)ss "
                      "WHERE (%(sqlcontext)s) "
                      "GROUP BY "
                      "  date_trunc('day',now()-%(table)s_timestamp) "
                      "ORDER BY "
                      "  date_trunc('day',now()-%(table)s_timestamp) "
                      "DESC"%locals())
            print " ".join(["%sj:%s"%(str(h["date_trunc"]).split(":")[0]
                                      ,h["count"]) for h in histo])
            if table=="construction":
                forsale = ("(construction_prixvente IS NOT NULL "
                           "AND construction_prixindicatif IS NULL)")
            else: forsale="false"
            rows = sql("SELECT %(table)s_id FROM kraland_%(view)s%(table)ss "
                       "WHERE (%(sqlcontext)s) "
                       "AND (%(table)s_timestamp<now()+'-%(range)ds' "
                       "     OR %(table)s_timestamp IS NULL "
                       "     OR %(forsale)s) "
                       "ORDER BY random() ASC"%locals())
            rows = map(lambda a: a[table+"_id"], rows)
            rows = set(rows).difference(done)
            print "Todo: %d"%len(rows)
            if not rows: return
            #if len(rows)<=errors: return
            row = rows.pop()
            done.add(row)
            updatefunc(row)
            time.sleep(interval)
        except KeyboardInterrupt: break
        except:
            errors += 1
            continue


def batiments_producteurs(objrow):
    if not objrow["production_batiment"]: return ()
    bat = 10*(objrow["production_batiment"]/10)
    bat_min, bat_max = bat+objrow["production_niveau"], bat+9
    return range(bat_min,bat_max)

def dispcons(cons):
    if type(cons)!=type({}):
        cons = sql("SELECT * FROM kraland_view_constructions "
                   "WHERE construction_id=%s", (cons,))[0]
    cons["owner"] = "« %(organisation_nom)s »« %(citoyen_nom)s »"%cons
    cons["pdb"] = cons["construction_pdb"]
    if not cons["pdb"]: cons["pdb"]="---"
    return ("[%(construction_id)6d] [%(pdb)3s] %(empire_abbrev)2s "
            "%(construction_pos)-3s %(province_nom)s - %(ville_nom)s - "
            "%(batiment_nom)s %(construction_nom)s %(owner)s")%cons

def dispsalaire(row):
    return "%(construction_salaire)d(%(construction_salairenet).1f)"%row
def dispstk(stk):
    return "%-20s - Stock:%5d/%-5d- Prix:%5d/%-5d- Caisse:%5s - %s - %s"%(
        stk["objet_nom"], stk["stock_courant"], stk["stock_max"],
        stk["stock_prixhaut"], stk["stock_prixbas"],
        calccaisse(stk), dispcons(stk), dispsalaire(stk))
def printstk(stk):
    print dispstk(stk)
def printcons(cons):
    str_salaire = dispsalaire(cons)
    print "Salaire: %-8s Caisse:%6s PxInd:%6s %s"%(
        str_salaire, cons["caisse"], cons["construction_prixindicatif"],
        dispcons(cons))
class patrimoine_enumerator:
    def __init__(self): self.total = 0
    def __call__(self, cons):
        printcons(cons)
        if cons["construction_prixindicatif"]:
            self.total += cons["construction_prixindicatif"]
    def __str__(self): return str(self.total)

from asyncore import compact_traceback
class AutoCatch(type):
    def __init__(cls, name, bases, dict):
        super(AutoCatch, cls).__init__(name, bases, dict)
        def f(func):
            def g(self, *l, **kv):
                savectx = self.context.copy()
                try: return func(self, *l, **kv)
                except SystemExit: raise
                except Exception,e:
                    nil, t, v, tbinfo = compact_traceback()
                    print nil, tbinfo
                    print "%s : %s"%(t, v)
                    sql.rollback()
                    self.context = savectx
                    return None                
            return g
        for attr in filter(lambda a: a.startswith("do_"), cls.__dict__.keys()):
            setattr(cls, attr, f(getattr(cls, attr)))

def check_temp_tables():
    print "CHECKING CACHE"
    try:
        temp_tables_timestamp = sql("SELECT max(stock_timestamp) AS t "
                                    "FROM kraland_temp_stocks")[0]["t"]
        real_tables_timestamp = sql("SELECT max(stock_timestamp) AS t "
                                    "FROM kraland_stocks")[0]["t"]
        if real_tables_timestamp > temp_tables_timestamp:
            print "DROP CACHE"
            sql("DROP TABLE kraland_temp_stocks")
            sql.commit()
            raise Exception("not fresh")
        print "CACHE IS FRESH"
        return
    except: sql.rollback()
    t0 = time.time()
    print "CREATE CACHE"
    sql("""CREATE TABLE kraland_temp_stocks AS SELECT *,
        bool('f') AS biz,
        stock_max-stock_courant AS stock_libre,
        COALESCE(
            construction_caisse,construction_salaire*construction_approx,0)
            AS construction_mincaisse
        FROM (SELECT construction_id, objet_id,
                     max(stock_timestamp) as stock_timestamp
                     FROM kraland_stocks
                     GROUP BY construction_id, objet_id) AS stock_now
        NATURAL JOIN kraland_stocks
        NATURAL JOIN kraland_constructions
        NATURAL JOIN kraland_batiments
        NATURAL JOIN kraland_objets
        NATURAL JOIN kraland_villes
        NATURAL JOIN kraland_provinces
        NATURAL JOIN kraland_empires""")
    print "CREATE INDEX"
    sql("CREATE INDEX kraland_temp_stocks_c "
        "ON kraland_temp_stocks(construction_id)")
    sql("CREATE INDEX kraland_temp_stocks_o "
        "ON kraland_temp_stocks(objet_id)")
    print "BIZ"
    sql("UPDATE kraland_temp_stocks SET biz='t' "
        "WHERE construction_gerant IN (SELECT nom FROM kraland_biz_membres) "
        "   OR construction_proprio IN (SELECT nom FROM kraland_biz_membres)")
    negolo=0.82
    sql("UPDATE kraland_temp_stocks "
        "SET stock_prixhaut=(SELECT prix FROM kraland_biz_prix "
        "                    WHERE kraland_biz_prix.objet_id = "
        "                          kraland_temp_stocks.objet_id)/%s "
        "WHERE biz AND objet_id IN (SELECT objet_id FROM kraland_biz_prix)"%
        negolo)
           

    
    print "EXEC TIME: %ds"%int(time.time()-t0)

import cmd
class CLI(cmd.Cmd,object):
    __metaclass__ = AutoCatch
    def __init__(self):
        cmd.Cmd.__init__(self)
        self.newcontext()
        self.setprompt()
        self.ges = 0
        self.commerce = 0
        self.dealtable = "deal"
        if os.path.isfile("kratool_history"):
            readline.read_history_file("kratool_history")
    def emptyline(self):
        pass
    def setprompt(self):
        self.prompt = ""
        for constraintname,constraintlist in self.context.items():
            for constraint in constraintlist:
                self.prompt += "{%s=%d|%s} "%(constraintname, constraint,
                                              id2name[constraintname][constraint])
        self.prompt += "$ "
    def precmd(self, line):
        if re.match("^[-+*./0-9 ]+$", line):
            return "eval %s"%line
        return line
    def postcmd(self, stop, line):
        self.setprompt()
    def addcontext(self, k, v):
        self.context[k].append(v)
    def delcontext(self, k, v=None):
        if v and v in self.context[k]: self.context[k].remove(v)
        if not v: self.context[k]=[]
    def setcontext(self, category, line):
        if not line:
            self.delcontext(category)
        else:
            words = line.split(" ")
            if words[0] not in ["+","-"]:
                self.delcontext(category)
                method = self.addcontext
                args = words
            else:
                method = {"+": self.addcontext, "-": self.delcontext}[words[0]]
                args = words[1:]
            args = [name2id[category].get(arg.replace("_"," "), False) or (int(arg) in id2name[category] and int(arg)) for arg in args]
            if False in args: raise Exception,"Cannot find one or more argument in context : %s"%line
            for arg in args: method(category, arg)
    def newcontext(self):
        self.context = { "ville":[], "objet":[],
                         "batiment":[], "construction":[],
                         "province":[], "empire":[]}
    def runwithcontext(self, **context):
        def functor(func):
            def function(*l, **kv):
                oldcontext = self.context
                self.context = context
                retval = func(*l, **kv)
                self.context = oldcontext
                return retval
            return function
        return functor

    def completion(self, objtype, text, line, begidx, endidx):
        return [nom.replace(" ","_")
                for nom in name2id[objtype]
                if nom.replace(" ","_").startswith(text.lower())]
    def complete_citoyen(self, text, line, begidx, endidx):
        return self.completion("citoyen", text, line, begidx, endidx)
    def complete_organisation(self, text, line, begidx, endidx):
        return self.completion("organisation", text, line, begidx, endidx)    
    def complete_empire(self, text, line, begidx, endidx):
        return self.completion("empire", text, line, begidx, endidx)
    def complete_province(self, text, line, begidx, endidx):
        return self.completion("province", text, line, begidx, endidx)
    def complete_ville(self, text, line, begidx, endidx):
        return self.completion("ville", text, line, begidx, endidx)
    def complete_objet(self, text, line, begidx, endidx):
        return self.completion("objet", text, line, begidx, endidx)
    def complete_batiment(self, text, line, begidx, endidx):
        return self.completion("batiment", text, line, begidx, endidx)
    def complete_empire(self, text, line, begidx, endidx):
        return self.completion("empire", text, line, begidx, endidx)

    def do_clear(self, junk):
        self.newcontext()

    def do_sqlviews(self, junk):
        for statement in statements[::-1]:
            if "CREATE VIEW" not in statement.upper(): continue
            viewname = re.findall(" (kraland_view_[a-z]+) ",statement)[0]
            sql("DROP VIEW "+viewname)
        for statement in statements:
            if "CREATE VIEW" in statement.upper(): sql(statement)
        sql.commit()

    def do_kramail(self, line):
        dst, subject, body = [urllib.quote(s) for s in line.split("|")]
        frm = kraget.getcookie("citoyen_id")
        res = kraget("http://www.kraland.org/main.php",
               "page=1;4;3;0;0&action=km_post&p1=%s&p2=%s&p3=%s&message=%s&p4=off&p5=off&p6=off&p7=0&Submit=Envoyer+%%21"%(dst,subject,frm,body))
        open("/tmp/foo","w").write(res)

    def do_daily(self, line):
        updatedef_villes()
        refreshmappings()
        updatedef_empires()
        update_citoyens()
        update_organisations()
        update_indices()

    def do_clan(self, line):
        cits = line.split(" ")
        for cit in cits:
            data = kraget("http://www.kraland.org/main.php?page=5;1;3;0;0&p2=%s"%cit)
            print data
            nom = re.findall("tr><td>Nom</td><td>([^<]+)<", data)[0]
            clan = re.findall(">Politique : ([^>]+)<", data)
            print "%30s %s"%(nom, clan)

    def do_highprice(self, line):
        objname, goalprice = line.split(" ")
        objname = objname.replace("_"," ")
        r = sql("SELECT *, stock_courant*((stock_prixbas+stock_prixhaut)/2-%s) AS influence "
                "  FROM kraland_view_stocks"
                "  WHERE objet_nom=%s"
                "  ORDER BY influence ASC",
                int(goalprice), objname)
        total = 0
        for cons in r:
            total += cons["stock_courant"]
            if cons["influence"]>0:
                print "%-8d %s"%(cons["influence"],dispstk(cons))
        print "Stock cybermondial:",total

    def do_enquete(self, nom):
        try:
            int(nom)
            ids = [row["citoyen_id"] for row in
                   sql("SELECT DISTINCT citoyen_id FROM kraland_history_citoyens "
                       "WHERE citoyen_id=%s", nom)]
        except:
            ids = [row["citoyen_id"] for row in
                   sql("SELECT DISTINCT citoyen_id FROM kraland_history_citoyens "
                       "WHERE citoyen_nom ILIKE %s", "%"+nom+"%")]
        for i in ids:
            print "http://www.kraland.org/main.php?page=5;1;3;0;0&p2=%d"%i
            for row in sql("SELECT MIN(citoyen_timestamp), "
                           " citoyen_nom, citoyen_link, citoyen_level, empire_nom "
                           "FROM kraland_history_citoyens "
                           "NATURAL JOIN kraland_empires "
                           "WHERE citoyen_id=%s "
                           "GROUP BY "
                           " citoyen_nom, citoyen_link, citoyen_level, empire_nom "
                           "ORDER BY MIN(citoyen_timestamp) ASC"
                           , i):
                print "- %(citoyen_nom)-25s %(citoyen_level)d %(empire_nom)-25s %(citoyen_link)s"%row
            print

    def do_eval(self, line):
        print eval(line)

    def do_gouv(self, line):
        rows = sql("select citoyen_nom from kraland_view_citoyens "
                   "where empire_nom='Confédération Libre' "
                   "and fonction_id < 10")
        print " ; ".join([row["citoyen_nom"] for row in rows])
    def do_etatmajor(self, empire):
        empire = empire.replace("_"," ")
        rows = sql("SELECT * FROM kraland_view_citoyens "
                   "WHERE empire_nom ilike %s "
                   "AND fonction_id IN (2, 22, 52, 102) "
                   "ORDER BY fonction_id ASC",
                   empire)
        for row in rows:
            if row["endroit"]==None: row["endroit"]=""
            print "%(citoyen_nom)-30s %(fonction)s %(endroit)s"%row
        print
        print " ; ".join([removesmileys(row["citoyen_nom"])
                          for row in rows])
    complete_etatmajor = complete_empire
    def do_listeddpcl(self, junk):
        rows = sql("SELECT citoyen_nom FROM kraland_view_citoyens "
                   "WHERE fonction_id=105 AND endroit IN "
                   " (SELECT ville_nom "
                   "  FROM kraland_villes NATURAL JOIN kraland_provinces "
                   "  WHERE empire_id=7)")
        print " ; ".join([row["citoyen_nom"] for row in rows])
    def do_senat(self, empire):
        empire = id2name["empire"][self.context["empire"][0]]
        rows = sql("select citoyen_nom from kraland_view_citoyens "
                   "where empire_nom=%s "
                   "and fonction_id = 51",
                   empire)
        print " ; ".join([row["citoyen_nom"] for row in rows])
    complete_senat = complete_empire
    def do_fonctionnaires(self, line):
        args = ([id2name["ville"][x] for x in self.context["ville"]]+
                [id2name["province"][x] for x in self.context["province"]])
        extrasql = " OR ".join(["endroit=%s" for x in args])
        rows = sql("select * from kraland_view_citoyens "
                   "where (%s)"%extrasql, *args)
        for row in rows: print "%(citoyen_nom)-30s %(fonction)s %(endroit)s"%row
        print
        print " ; ".join([row["citoyen_nom"] for row in rows])

    def do_voyage(self,line):
        assert len(self.context["ville"])==2
        def c(vid):
            h=sql("SELECT province_id,map_pos FROM kraland_maps "
                "WHERE ville_id=%s"%vid)[0]
            return makecoords(h["province_id"],h["map_pos"])
        x0,y0=c(self.context["ville"][0])
        x1,y1=c(self.context["ville"][1])
        delta = abs(x0-x1)+abs(y0-y1)
        def h(m): return "%d:%d"%(m/60,m%60)
        print "%d (%s - %s - %s)"%(
            delta, h(delta*20), h(delta*10), h(delta*20/3))
    def do_kramap(self, junk):
        o = open("/tmp/kramap.psv","w")
        print "SQL..."
        rows=sql("SELECT map_absx, map_absy, map_type, ville_nom, province_nom "
                 "from kraland_view_map natural left join kraland_villes natural join kraland_provinces")
        for line in rows:
            o.write("%(map_absx)d|%(map_absy)d|%(map_type)d|%(ville_nom)s|%(province_nom)s\n"%line)
        o.close()
    def do_traveltable(self, junk):
        o = open("/tmp/traveltable.psv","w")
        nomsbat = { 311: "Port", 312: "Gare", 313: "Aéroport", 211: "Mairie" }
        nomstrans= { 311: "le bateau", 312:"le train", 313: "l'avion"}
        temps = { 311: 480, 312: 360, 313: 240 }
        distances = { 311: 3, 312: 3, 313: 4 }
        print "SQL..."
        rows=sql("SELECT * FROM kraland_view_map NATURAL JOIN kraland_view_villes "
                 "WHERE ville_id IS NOT NULL ORDER BY province_id ASC")
        villes = {}
        for line in rows:
            pid = line["province_id"]
            villes[pid] = villes.get(pid, []) + [ "%(ville_nom)s|%(map_absx)d|%(map_absy)d"%line ]
        print "Generating... VPA"
        for p in range(1, 196):
            print >>sys.stderr, p, 
            provs = set([p])
            for pp in provneigh(p): # on itere sur la distance 1 pour avoir la 2
                provs.update(provneigh(pp))
            # Pour chaque province a portee, on rajoute un chemin de toute les cases de la province vers les villes qui y sont presente.
            xp, yp = p2xy(p)
            for pp in provs:
                for ville in villes.get(pp, []):
                    o.write("%s|%s|%s|%s|%s\n"%("province|-%d|-%d"%(xp,yp), ville,
                                                        "vpa","vpa",105))
        print >>sys.stderr, ""
        print "SQL..."
        rows=sql("SELECT *,batiment_id/10 AS type, mod(batiment_id,10) AS niveau "
                 "FROM kraland_view_constructions "
                 "NATURAL JOIN kraland_view_map "
                 "WHERE mod(batiment_id,10)>0 "
                 "AND (batiment_id BETWEEN 3111 AND 3139) "
                 "ORDER BY province_id ASC")
        for row in rows:
            assert row["type"] in temps
        print "Generating..."
        # Et ensuite de tout les autres trucs.
        for depart in rows:
            print >>sys.stderr, depart["province_id"],
            for arrivee in rows:
                if depart["type"]!=arrivee["type"]: continue
                typ = depart["type"]
                distance = provdist(depart["province_id"], arrivee["province_id"])
                if distance>distances[typ]: continue
                if depart["batiment_id"]==3131: continue # skip Aérodrome
                tempsvoyage = temps[typ]
                if typ != 211:
                    if distance==0: tempsvoyage /= 2
                    else: tempsvoyage *= distance
                    if depart["niveau"]==3: tempsvoyage=tempsvoyage*7/8
                    if depart["niveau"]==4: tempsvoyage=tempsvoyage*3/4
                s_depart="%(ville_nom)s|%(map_absx)d|%(map_absy)d"%depart
                s_arrivee="%(ville_nom)s|%(map_absx)d|%(map_absy)d"%arrivee
                o.write("%s|%s|%s|%s|%s\n"%(s_depart,s_arrivee,
                                            nomsbat[typ],nomstrans[typ],tempsvoyage))
        print "/tmp/traveltable.psv is ready"
        o.close()

    def do_geo(self, line):
        assert len(self.context["province"])==1
        distance = int(line)
        while distance:
            distance -= 1
            nextprov = self.context["province"][:]
            for p in self.context["province"]:
                for pp in provneigh(p): # a tester : faire un set et ensuite nextprov.update(proveneight(p))
                    if pp not in nextprov:
                        nextprov.append(pp)
            self.context["province"] = nextprov
            

    def do_empire(self, line):
        self.setcontext("empire", line)
    def do_province(self, line):
        self.setcontext("province", line)
    def do_ville(self, line):
        if line or self.context["ville"]:
            self.setcontext("ville", line)
        else:
            for p_id in self.context["province"]:
                for v_id in [row["ville_id"] for row in
                             sql("SELECT ville_id FROM kraland_villes "
                                 "WHERE province_id=%s", p_id)]:
                    if v_id not in self.context["ville"]:
                        self.context["ville"].append(v_id)
            self.setcontext("province", "")
    def do_citoyen(self, line):
        self.setcontext("citoyen", line)
    def do_organisation(self, line):
        self.setcontext("organisation", line)
    def do_objet(self, line):
        self.setcontext("objet", line)
    def do_batiment(self, line):
        self.setcontext("batiment", line)
        if line.count(" ")>0: return
        for batiment in self.context["batiment"]:
            for niveau in range(5):
                batniv = batiment-batiment%10+niveau
                if batniv not in self.context["batiment"]:
                    if batniv in id2name["batiment"]:
                        self.context["batiment"].append(batniv)
        self.context["batiment"].sort()
    def do_construction(self, line):
        self.setcontext("construction", line)
    def do_def(self, junk):
        updatedef_villes()
        refreshmappings()
        updatedef_provinces()
        refreshmappings()
        updatedef_empires()
        refreshmappings()
        updatedef_batiments()
        refreshmappings()
        updatedef_objets()
        refreshmappings()
        updatedef_ecoles()
        refreshmappings()
        updatedef_fonctions()
        refreshmappings()
    def do_variations(self, junk):
        if not junk:
            print "variations Nom_Objet YYYY-MM-DD HH:MM:SS"
            return
        obj_name, timestamp = junk.split(" ",1)
        obj_id = name2id["objet"][obj_name]
        rows = sql("""
SELECT kraland_stocks_before.construction_id as construction_id,
       kraland_stocks_before.stock_courant as stock_before,
       kraland_stocks_after.stock_courant as stock_after,
       *
FROM (SELECT max(stock_timestamp) AS stock_timestamp,construction_id,objet_id
          FROM kraland_stocks
          WHERE objet_id = %s AND stock_timestamp<%s
          GROUP BY construction_id,objet_id) AS kraland_stocks_before_keys
    NATURAL JOIN kraland_stocks AS kraland_stocks_before,
    (SELECT stock_courant, construction_id
         FROM kraland_stocks
         WHERE objet_id=%s AND stock_timestamp>%s) AS kraland_stocks_after
NATURAL JOIN kraland_view_constructions
WHERE kraland_stocks_before.construction_id=kraland_stocks_after.construction_id AND kraland_stocks_before.stock_courant!=kraland_stocks_after.stock_courant
""", obj_id, timestamp, obj_id, timestamp)
        for r in rows:
            print dispcons(r)
            print "%(stock_before)d -> %(stock_after)d\n"%r

    def do_patrimoine(self, junk):
        self.conslist(action=patrimoine_enumerator())
    def do_patriwarn(self, mail):
        if mail:
            savestdout = sys.stdout
            sys.stdout = StringIO.StringIO()
        self.conslist(extrasql="construction_pdb <=40", action=patrimoine_enumerator(), sortby=("organisation_id", "citoyen_id", "construction_pdb"))
        if mail:
            result = sys.stdout.getvalue()
            sys.stdout = savestdout
            sys.stdout.write(result)
            sys.stdout.flush()
            if result.strip() != "0":
                sys.stderr.write("Sending Mail to %s\n"%mail)
                import smtplib
                server = smtplib.SMTP("localhost")
                server.sendmail("kratool@enix.org", mail, "From: kratool@enix.org\nTo: %s\nSubject: Rapport kratool\nGenerated from kratool with context %s\n%s"%(mail,self.sqlcontext(), result))
                server.quit()

    def do_updatemaptiles(self, junk):
        for scale in (0,8,32):
            kramap(scale).forcecacheload()

    def do_flagmap(self, junk):
        class accumulator:
            def __init__(self):
                self.accum = []
            def __call__(self, cons):
                self.accum.append(cons)
            def __iter__(self):
                for x in self.accum: yield x

        mapscale = 8
        iconscale = 32
        themap = kramap(mapscale)
        theicons = kramages(iconscale)
        accum = accumulator()
        self.conslist("(organisation_id=%d)"%self.context["organisation"][0],
                      action=accum)
        byville = {}
        for cons in accum:
            byville[cons["ville_id"]] = (byville.get(cons["ville_id"],[])
                                         + [cons["batiment_id"]])
        for ville_id in byville:
            bat_id_list = byville[ville_id]
            row = sql("SELECT * FROM kraland_view_map WHERE ville_id=%s"%ville_id)[0]
            x,y=makecoords(row["province_id"],row["map_pos"])
            x*=mapscale ; y*=mapscale
            for n in range(len(bat_id_list)):
                bat_id = bat_id_list[n]
                icon = theicons[bat_id]
                xpos=n%2-0.5
                ypos=n/2-0.5
                themap.img.paste(icon,(x+xpos*iconscale,y+ypos*iconscale))
        themap.img.save("mapflag.png")
            

    def do_immo(self, foo):
        immo = sql("SELECT * FROM kraland_view_constructions "
                   "WHERE (%s) AND construction_prixvente IS NOT NULL "
                   "ORDER BY 1.0*construction_prixvente/(1+construction_prixindicatif) DESC"%self.sqlcontext())
        for cons in immo:
            if cons["construction_prixindicatif"]:
                cons["pm"] = (100*cons["construction_prixvente"]/
                              cons["construction_prixindicatif"]-100)
            else: cons["pm"] = 666
            print ("%(construction_pos)-3s %(ville_nom)-30s "
                   "%(batiment_nom)-30s "
                   "%(construction_proprio)-30s "
                   "%(construction_prixindicatif)6s "
                   "%(construction_prixvente)6s "
                   "%(pm)5.1f%% "
                   "http://www.kraland.org/order.php?p1=1301"
                   "&p2=%(construction_id)d&p3=%(ville_id)d"%cons)

    def do_gold(self, foo):
        print sql("SELECT SUM(stock_courant) AS s "
                  "FROM kraland_view_stocks "
                  "WHERE objet_nom='Or' "
                  "AND batiment_id/10=307 "
                  "AND (%s)"%self.sqlcontext())[0]["s"]

    def do_refreshorga(self, foo):
        orga_id = self.context["organisation"][0]
        data = kraget("http://www.kraland.org/order.php?p1=4100&p2=%s"%orga_id)
        cons = re.findall(r'"order.php\?p1=1301&amp;p2=([0-9]+)"', data)
        for cons_id in cons:
            if not sql("SELECT 1 FROM kraland_constructions "
                       "WHERE construction_id=%s AND organisation_id=%s",
                       cons_id, orga_id):
                crawl("construction", update_construction,
                      "interval=0 range=69",
                      "construction_id=%s"%cons_id)
    
    def do_crawlville(self, params):
        crawl("ville", update_ville, params,
              self.sqlcontext("empire","province","ville"))
    def do_crawlstock(self, params):
        crawl("stock", update_construction, params, self.sqlcontext())
    def do_crawlcons(self, params):
        crawl("construction", update_construction, params,
              self.sqlcontext("empire","province","ville",
                              "citoyen","organisation",
                              "batiment","construction"))
    def do_crawlgold(self, junk):
        crawl("construction", update_construction, "interval=0 range=3600",
              "(batiment_id/10=307)")
    def do_crawlmap(self, junk):
        update_map()
    def do_ddp(self, foo):
        self.conslist("batiment_id>=2000 AND batiment_id<3200")
    def do_pubcons(self, foo):
        self.objlist("batiment_id>=3000 AND batiment_id<3200"+
                     " AND ((objet_id>1000 AND objet_id<1110) "+
                     "    OR (objet_id>1600 AND objet_id<2070))"+
                     " AND (stock_max>0)",
                     ("ville_nom", "batiment_nom"))
    def do_priv(self, foo):
        self.conslist("batiment_id>=3200 and batiment_id<5000",
                      ("ville_nom","batiment_id"))
    def do_ls(self, foo):
        self.objlist("stock_courant>0 or stock_max>0",
                     ("objet_nom","stock_courant"))
    def do_vendeur(self, foo):
        self.objlist("stock_courant>0", ("objet_nom","stock_prixhaut"))
    def do_acheteur(self, foo):
        self.objlist("stock_max>stock_courant AND construction_mincaisse>stock_prixbas", ("objet_nom","stock_prixbas"))
    def do_activity(self, param):
        if "-all" in param: all, param = True, param.replace("-all", "").strip()
        else: all=False
        if param:
            interval = param
        else: interval = "8 days"
        sqlreq = ("SELECT * from kraland_history_stocks natural join kraland_constructions natural join kraland_objets natural left join kraland_organisations natural join kraland_batiments natural join kraland_villes " +
                  "WHERE (%s) and stock_timestamp > CURRENT_TIMESTAMP - interval '%s' order by construction_id, objet_id, stock_timestamp"%(self.sqlcontext(), interval))
        res = sql(sqlreq)
        lastbat, lastobj, lastvalue = None, None, None
        import mx.DateTime
        today = mx.DateTime.now()
        buff = ""
        toprint = 0
        for line in res:
            if lastbat != line["construction_id"] or lastobj != line["objet_id"]:
                if toprint or all: sys.stdout.write(buff + "\n")
                toprint = 0
                buff = ("%(batiment_nom)s - %(ville_nom)s(%(construction_pos)s) : %(objet_nom)s"%line).ljust(80)
                lastvalue = None
            day = (today - line["stock_timestamp"]).day
            buff += "|"+ "%2d:"%day + str(line["stock_courant"]).rjust(5)
            if lastvalue != None:
                if lastvalue > line["stock_courant"]: toprint = z = "-"
                elif lastvalue < line["stock_courant"]: toprint = z = "+"
                else: z= " "
                buff += "%s"%z
            lastobj = line["objet_id"]
            lastbat = line["construction_id"]
            lastvalue = line["stock_courant"]
        if toprint or all: sys.stdout.write(buff + "\n")
    def objlist(self, extrasql="1=1", sortby=("objet_nom",), limit=0, action=printstk):
        res=sql("SELECT * FROM kraland_view_stocks "+
                "WHERE "+self.sqlcontext()+" AND "+
                "("+extrasql+") ORDER BY stock_timestamp ASC")
        squash(res, "construction_id", "objet_id")
        order(res, *sortby)
        if limit>0: res=res[:limit]
        if limit<0: res=res[limit:]
        for stk in res: action(stk)
    def conslist(self, extrasql="1=1", sortby=("ville_nom","batiment_nom"),
                 limit=0, action=printcons):
        res=sql("SELECT * from kraland_view_constructions "+
                "WHERE ("+extrasql+ ") AND ("+
                self.sqlcontext("province","ville","empire",
                                "construction","batiment",
                                "citoyen","organisation",
                                )+")")
        order(res, *sortby)
        if limit>0: res=res[:limit]
        if limit<0: res=res[limit:]
        for cons in res:
            if not cons["construction_salaire"]: continue
            calccaisse(cons)
            action(cons)
        print str(action)
    def do_ultrashark(self, margemini=""):
        if margemini=="": margemini="0"
        negohi=1+self._coeff()
        negolo=1-self._coeff()
        marge="(%s*acheteur.stock_prixbas-%s*vendeur.stock_prixhaut)"%(
            negohi,negolo)
        lim_vendeur="vendeur.stock_courant"
        lim_acheteur="acheteur.stock_libre"
        lim_caisse="""
        CASE WHEN acheteur.stock_prixbas>0
        THEN acheteur.construction_mincaisse/acheteur.stock_prixbas
        ELSE 0
        END"""
        quantite="""(
CASE WHEN %(lim_vendeur)s <= %(lim_acheteur)s
          AND %(lim_vendeur)s <= %(lim_caisse)s
     THEN %(lim_vendeur)s
     WHEN %(lim_acheteur)s <= %(lim_vendeur)s
          AND %(lim_acheteur)s <= %(lim_caisse)s
     THEN %(lim_acheteur)s
     ELSE %(lim_caisse)s
     END)"""%locals()
        contexta=self._sqlcontext("acheteur.")
        contextv=self._sqlcontext("vendeur.")
        try: sql("DROP TABLE "+self.dealtable) ; sql.commit()
        except:
            sql.rollback()
        #check_temp_tables()
        t0=time.time() ; print "KRA20KRA..."
        sql("CREATE TEMPORARY TABLE "+self.dealtable+" AS "+
"""SELECT
--vendeur.biz AS biz,
vendeur.objet_id, vendeur.objet_nom, vendeur.objet_millicharge,
vendeur.construction_id AS v_cons, vendeur.ville_nom AS v_ville,
vendeur.batiment_nom AS v_batiment_nom, vendeur.batiment_id AS v_batiment_id,
vendeur.construction_nom AS v_nom, vendeur.construction_pos AS v_pos, 
vendeur.stock_prixhaut, vendeur.stock_courant,
acheteur.construction_id AS a_cons, acheteur.ville_nom AS a_ville,
acheteur.batiment_nom AS a_batiment_nom, acheteur.batiment_id AS a_batiment_id,
acheteur.construction_nom AS a_nom, acheteur.construction_pos AS a_pos, 
acheteur.stock_prixbas, acheteur.stock_libre, acheteur.construction_approx,
%(marge)s AS marge, %(quantite)s AS quantite, %(marge)s*%(quantite)s AS benef,
%(quantite)s*vendeur.objet_millicharge/1000.0 AS charge,
CASE WHEN vendeur.objet_millicharge>0
THEN 1000.0*%(marge)s/vendeur.objet_millicharge
ELSE 1000 END AS rentabilite,
1 FROM kraland_view_stocks AS acheteur, kraland_view_stocks AS vendeur
WHERE acheteur.objet_id=vendeur.objet_id
AND acheteur.batiment_id<5000
AND vendeur.batiment_id<5000
AND acheteur.stock_prixbas!=0
AND %(quantite)s > 0
AND %(marge)s*%(quantite)s > %(margemini)s
AND %(contexta)s AND %(contextv)s
AND acheteur.stock_prixbas > 0
ORDER BY v_ville ASC, a_ville ASC, %(marge)s*%(quantite)s DESC"""%locals())
        sql.commit()
        print "KRA20Q en %ds."%int(time.time()-t0)
    def do_flow(self, n=""):
        if not n: n="50"
        n = int(n)
        sql.execute("SELECT v_ville, a_ville, sum(benef) FROM deal "
                     "WHERE 1::boolean GROUP BY v_ville,a_ville "
                     "ORDER BY sum(benef) DESC LIMIT %s"%n)
        for row in sql.dbcu.fetchall():
            print "%-20s %-20s %s"%row
    def do_zbam(self, args=""):
        if not args: args="50&20"
        m = re.match("^([0-9]+)([&|])([0-9]+)$", args)
        if not m: print "Specify perdealf&perload or benef|perload" ; return
        min_per_deal, and_or, min_per_load = m.groups()
        and_or = {"&":"AND", "|":"OR"}[and_or]
        query = []
        if not self.context["ville"] and self.context["province"]: self.do_ville("")
        villes = [id2name["ville"][i] for i in self.context["ville"]]
        assert villes, "Aucune ville trouvée !"
        params = []
        for i in range(len(villes)):
            v = villes[:i+1]
            a = villes[i:]
            params.extend(v)
            params.extend(a)
            v = " OR ".join(["v_ville=%s" for vi in v])
            a = " OR ".join(["a_ville=%s" for vi in a])
            query.append("SELECT * FROM deal WHERE (%s) AND (%s) AND "
                         "(benef>%s %s rentabilite>%s)"%
                         (v,a,min_per_deal,and_or,min_per_load))
        query = " UNION ".join(query)
        query += " ORDER BY objet_nom ASC, v_ville ASC, v_pos ASC, rentabilite DESC"
        lastobj = lastlieu =""
        for row in sql(query,*params):
            #print row
            #if row["objet_nom"] in ["Essence", "Fronde"]: continue
            v = "%-3s %s%s %s"%(row["v_pos"],"",#row["biz"] and "*" or "",
                                row["v_ville"],row["v_batiment_nom"])
            a = "%-3s %s %s"%(row["a_pos"],row["a_ville"],row["a_batiment_nom"])
            q = "%d/%d/%d"%(row["stock_courant"], row["stock_libre"],
                            row["quantite"])
            if (row["quantite"]!=row["stock_courant"]
                and row["quantite"]!=row["stock_libre"]
                and row["construction_approx"]==9): q+="+"
            r = "%d-%d=%d*%d=%d"%(row["stock_prixhaut"], row["stock_prixbas"],
                                  row["marge"],row["quantite"], row["benef"])
            while len(q)<10: q=(1-len(q)%2)*" " + q + (len(q)%2)*" "
            while len(r)<20: r=(1-len(r)%2)*" " + r + (len(r)%2)*" "
            if row["charge"]==None: print row ; continue # XXX
            # on saute les deals inapplicables
            if not deal_is_applicable(row): continue
            if lastobj == row["objet_nom"] and lastlieu == v: row["objet_nom"], v = '"', '"'
            else: lastobj, lastlieu = row["objet_nom"], v
            print "%-20s | %-40s | %-40s |%s|%s| %.2fc (%d/c)"%(
                row["objet_nom"], v, a, q, r,
                row["charge"], row["rentabilite"])

    def biz_commit(self):
        sql.commit()
        try: sql("DROP TABLE kraland_temp_stocks")
        except: sql.rollback()
    def do_biz_membres_add(self,name):
        sql("INSERT INTO kraland_biz_membres VALUES (%s)", name)
        self.biz_commit()
    def do_biz_membres_remove(self,name):
        sql("DELETE FROM kraland_biz_membres WHERE nom=%s", name)
        self.biz_commit()
    def do_biz_membres_list(self,junk):
        for row in sql("SELECT * FROM kraland_biz_membres"):
            print row["nom"]
    def do_biz_prix_set(self,line):
        o_nom,prix = line.split(" ")
        self.do_biz_prix_clear(o_nom)
        sql("INSERT INTO kraland_biz_prix (objet_id,prix) "
            "VALUES (%s, %s)", name2id["objet"][o_nom], prix)
        self.biz_commit()
    def do_biz_prix_clear(self,o_nom):
        sql("DELETE FROM kraland_biz_prix WHERE objet_id=%s",
            name2id["objet"][o_nom])
        self.biz_commit()
    def do_biz_prix_list(self, junk):
        for row in sql("SELECT * FROM kraland_biz_prix"):
            print "%-40s %d"%(id2name["objet"][row["objet_id"]],row["prix"])

    def do_update(self, kind):
        if not self.context["ville"]:
            print "Sélectionnez d'abord au moins une ville."
            return
        for ville in self.context["ville"]:
            update_ville(ville)
            update_commerce(ville)
    complete_update = complete_ville
    def do_url(self, cons_id):
        cons_id = int(cons_id)
        ville_id = sql("SELECT ville_id FROM kraland_view_constructions "
                       "WHERE construction_id=%s", cons_id)[0]["ville_id"]
        print "http://www.kraland.org/order.php?p1=1301&p2=%d&p3=%s"%(
            cons_id, ville_id)
    def do_salaires(self, foo):
        self.conslist("construction_mincaisse>=construction_salaire",
                      ("construction_salaire",))
    def sqlcontext(self, *constraints):
        return self._sqlcontext("", *constraints)
    def _sqlcontext(self, tableprefix="", *constraints):
        if not constraints: constraints = self.context.keys()
        def sqlconstraint(constraint):
            sqlcode = " OR ".join(["%s%s_id %s %d"%(tableprefix, constraint,
                                                    elem>0 and "=" or "!=",
                                                    abs(elem))
                                   for elem in self.context.get(constraint,[])])
            return "(%s)"%(sqlcode or "1=1")
        sqlcode = " AND ".join([sqlconstraint(constraint)
                                for constraint in constraints])
        return "(%s)"%(sqlcode or "1=1")
    def do_EOF(self, junk):
        print ""
        #readline.write_history_file("kratool_history")
        sys.exit(0)
    def do_prod(self, junk):
        if "objet" not in self.context or not self.context["objet"]:
            print "Un objet doit être spécifié."
        else:
            for objet in self.context["objet"]:
                self.printprod(objet)

    def boutique(self, cons_id, obj, stock):
        rows = sql("SELECT objet_id,stock_prixbas "
                   "FROM kraland_view_stocks "
                   "WHERE construction_id=%s"%cons_id)
        prix_achat = dict([(row["objet_id"],row["stock_prixbas"])
                           for row in rows])

        prixvente = obj["production_par"]*stock["stock_prixhaut"]
        salaires, matieres = 0, 0
        for prod in sql("SELECT * FROM kraland_production "
                        "WHERE objet_id=%s"%obj["objet_id"]):
            if prod["production_avec"]!=0:
                matieres += (prod["production_combien"]*
                             prix_achat[prod["production_avec"]])
            else:
                salaires += stock["construction_salaire"]*prod["production_combien"]
        salaire_net = stock["construction_salairenet"]*prod["production_combien"]
        tva = 0.01*stock["empire_impot_vente"]
        margemin = (1-tva)*0.8*prixvente - salaires - 1.2*matieres
        margez = (1-tva)*0.8*prixvente - salaires - 1.0*matieres
        c = self._coeff()
        margemoi = (1-tva)*(1-c)*prixvente - salaires - (1+c)*matieres
        coutmoi = (1-c)*prixvente-salaire_net-(1+c)*matieres
        return margemin,margez,margemoi,coutmoi

    def do_boutique(self, coutmax=""):
        if coutmax=="all":
            print "***ALL***"
            coutmax=""
            all=True
        else: all=False
        obj_list = (self.context["objet"] or
                    [row["objet_id"]
                     for row in sql("SELECT * FROM kraland_objets")])
        stocks = sql("SELECT * FROM kraland_view_stocks "
                     "WHERE (%s)"%self.sqlcontext())
        for obj_id in obj_list:
            obj = sql("SELECT * FROM kraland_objets WHERE objet_id=%s"%obj_id)[0]
            bat_ok_list = batiments_producteurs(obj)
            if not bat_ok_list: continue
            if obj["objet_nom"]=="Caillou": continue # cas particulier
            for stock in stocks:
                if all:
                    stock["batiment_id"]=4+10*(stock["batiment_id"]/10)
                    stock["stock_max"]=999
                if stock["objet_id"]!=obj_id: continue
                if stock["batiment_id"] not in bat_ok_list: continue
                if stock["stock_courant"]==0 and stock["stock_max"]==0:continue
                cons_id = stock["construction_id"]

                margemin,margez,margemoi,coutmoi = self.boutique(cons_id,obj,stock)

                if coutmax=="": coutmax=10000
                if coutmoi < int(coutmax):
                    printstk(stock)
                    print "U/Marge min/zero/moi/cout: %3d %5d %5d %5d %5d"%(
                        obj["production_par"],
                        margemin, margez, margemoi, coutmoi)
            
    def printprod(self, obj_id, combien=1.0, total=None, pad=""):
        if total: skiprecap=True
        else: skiprecap=False ; total={}
        obj_id = int(obj_id)
        obj = sql("SELECT * FROM kraland_objets WHERE objet_id=%d"%obj_id)[0]
        res=sql("SELECT * FROM kraland_production WHERE objet_id=%d"%obj_id)
        for prod in res:
            if prod["production_avec"] not in total:
                total[prod["production_avec"]]=0.0
            qte = combien * prod["production_combien"] / obj["production_par"]
            total[prod["production_avec"]] += qte
            #if prod["production_avec"]: # on saute les UT (qui ont l'id 0)
            #    self.printprod(prod["production_avec"], qte, total)
        if skiprecap: return
        for quoi, combien in total.items():
            if quoi==0: name="UT"
            else: name=id2name["objet"][quoi]
            print "%s%-6.2f x %s"%(pad, combien, name)
            if quoi: self.printprod(quoi, combien, pad="%s\t"%pad)
    def do_cdp(self, foo):
        for obj_id in self.context["objet"]:
            row = sql("SELECT * FROM kraland_objets WHERE objet_id=%s"%obj_id)
            if not row: continue
            context = self.context.copy()
            context["objet"]=[obj_id]
            context["batiment"]=batiments_producteurs(row[0])
            self.runwithcontext(**context)(self.objlist)()
    #def do_opera(self, foo):
    #    global cookiefile
    #    cookiefile="/tmp/opera.txt"
    #    convertoperacookies(cookiefile)
    #    os.environ["http_proxy"]="http://localhost:3131/"
    def do_ges(self, ges):
        if not ges: print self.ges
        else: self.ges = int(ges)
    def do_commerce(self, commerce):
        if not commerce: print self.commerce
        else: self.commerce = min(95,int(commerce))
    def _coeff(self):
        return self.commerce*min(3*self.ges,20)/10000.0
    def do_marge(self, args, verbose=True):
        args = args.split(" ")
        if len(args) in [2, 3]:
            try:
                args = [int(a) for a in args]
                if len(args)==2: args += [1]
                lo, hi, qty = args
                lo *= qty
                hi *= qty
                cumul = 0.0
                rabais = min(20,3*self.ges)
                for (pourcent, marge) in [
                    (self.commerce*self.commerce/100.0,
                     hi*(100+rabais)/100.0-lo*(100-rabais)/100.0),
                    (self.commerce*(100-self.commerce)/100.0,
                     hi-lo*(100-rabais)/100.0),
                    (self.commerce*(100-self.commerce)/100.0,
                     hi*(100+rabais)/100.0-lo),
                    ((100-self.commerce)*(100-self.commerce)/100.0,
                     hi-lo*1.0)
                    ]:
                    if pourcent>0 and verbose: print "%5.3g%% %.2f"%(pourcent, marge)
                    cumul += pourcent/100*marge
                if verbose: print "=> %.2f"%cumul
                return cumul
            except: pass
        print "marge <prix_achat> <prix_vente> [quantité]"

    def do_save(self, table):
        headers = []
        count = 0
        print sql("SELECT COUNT(*) AS c FROM kraland_%s"%table)[0]["c"], "rows"
        for row in sql.sqli("SELECT * FROM kraland_%s"%table):
            if not headers:
                headers = row.keys()
                f = open("%s.dump"%table,"w")
                cPickle.dump(headers, f)
            cPickle.dump([row[k] for k in headers], f)
            count += 1
            if 0==count%1000: print count
            
    def do_load(self, table):
        f = open("%s.dump"%table)
        headers = cPickle.load(f)
        count = 0
        while 1:
            try: row = cPickle.load(f)
            except EOFError: break
            sql("INSERT INTO kraland_%s (%s) VALUES (%s)"%
                (table, ",".join(headers), ",".join(["%s"]*len(headers))),
                *row)
            count += 1
            if 0==count%1000: print count
        sql.commit()

    def do_sync(self, table):
        sync_cnx = psycopg.connect("dbname=rikst user=rorikst "
                                   "host=the-plate.enix.org")
        sync_cur = sync_cnx.cursor()
        tables = table.split(" ") or [
            "empire", "province", "ville", "construction", "stock",
            "citoyen"]
        for table in tables:
            t = sql("SELECT max(%s_timestamp) AS m FROM kraland_%ss"%
                    (table,table))[0]["m"] or '2000-01-01 00:00:00'
            print t
            sync_cur.execute("SELECT * FROM kraland_%ss "
                             "WHERE %s_timestamp>%%s::timestamp+'1s'"%
                             (table, table), (str(t),))
            cols = [c[0] for c in sync_cur.description]
            rows = sync_cur.fetchall()
            print len(rows), "rows"
            for row in rows:
                print row
                update_row(table, Ellipsis,
                           timestamp=False, unique=True, alsohistory=False,
                           **dict(zip(cols,row)))
        # XXX manque le commit et l'history
        
    def do_rc(self, filename):
        for line in open(filename):
            self.onecmd(line.strip())

    def do_httpd(self, port):
        port = int(port)
        self.runhttpd=True
        from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
        import cgi
        class KraHTTPRequestHandler(BaseHTTPRequestHandler):
            def do_GET(self, clii=self):
                self.send_response(200)
                self.send_header("Content-Type", "text/html")
                self.end_headers()
                if "?" in self.path:
                    os.environ["QUERY_STRING"] = self.path.split("?",1)[1]
                form = cgi.parse(self.rfile)
                for ctx in clii.context:
                    if ctx in form:
                        clii.context[ctx]=[name2id[ctx][e] for e in form[ctx]
                                           if e in name2id[ctx]]
                print >>self.wfile, clii.context, "<br>"
                cmdname = form.get("cmd",[])
                cmdname = cmdname and cmdname[0] or ""
                args = form.get("arg",[])
                args = args and args[0] or ""
                self.wfile.write(open("menu.html").read())
                if cmdname in ["priv", "cdp", "patrimoine", "ls", "enquete",
                               "boutique", "forums", "topics", "messages"]:
                    stdout = sys.stdout
                    sys.stdout = self.wfile
                    print "<pre>"
                    getattr(clii,"do_"+cmdname)(args)
                    print "</pre>"
                    sys.stdout = stdout
                self.log_request(200)
        self.httpd = HTTPServer(('',port),KraHTTPRequestHandler)
        print "HTTP server running on port %d."%port
        while self.runhttpd: self.httpd.handle_request()

    def do_contacts(self, junk):
        for i in self.context['citoyen']:
            print "http://www.kraland.org/main.php?page=5;1;3;1;1&action=add_ctct&p1=%d&p3=%d"%(i,i)

    def do_forums(self, junk):
        print '</pre>'
        for forum in sql("SELECT * FROM kraland_forums ORDER BY forum_id ASC"):
            print '<a href="/?cmd=topics&arg=%(forum_id)s">%(forum_nom)s</a> - %(forum_desc)s<br>'%forum
        print '<pre>'

    def do_topics(self, forum_id):
        print '</pre>'
        for topic in sql("SELECT * FROM kraland_topics "
                         "WHERE forum_id=%s ", forum_id):
            print '<a href="/?cmd=messages&arg=%(topic_id)s">%(topic_nom)s</a><br>'%topic
        print '<pre>'

    def do_messages(self, topic_id):
        print '</pre>'
        for message in sql("SELECT * FROM kraland_messages "
                           "WHERE topic_id=%s "
                           "ORDER BY message_id ASC", topic_id):
            print '<p>%(message_text)s</p><hr>'%message
        print '<pre>'

    def do_forums_init(self, args):
        data = kraget("http://www.kraland.org/main.php?page=4;0;0;0;0")
        for line in data.split("</tr>"):
            forum = re.findall('page=4;([0-9]+);0;0;0">([^<]+)</a></p><p>(.*)</p></td><td class="info">.*<td class="info">([0-9]+)</td><td class="info">([0-9]+)</td><td class="info"><p>([^<]+)</p>', line)
            if not forum: continue
            forum_id, forum_nom, forum_desc, forum_topics, forum_messages, forum_ts = forum[0]
            update_row("forum", forum_id, forum_nom=forum_nom, forum_desc=forum_desc)
            print ">>>", forum_id, forum_nom
            dbco.commit()
        for pid,pname in id2name["province"].items():
            if pid<0: continue
            if "Sujets en cours" in kraget("http://www.kraland.org/main.php?page=4;%d;0;0;0"%(1000+pid)):
                update_row("forum", 1000+pid, forum_nom=pname, forum_desc="Forum provincial - %s"%pname, forum_crawl='t')
        dbco.commit()

    def do_forums_crawl(self, args):
        kraget.verbose=False
        for forum in sql("SELECT * FROM kraland_forums WHERE forum_crawl"):
            update_topics(forum["forum_id"], False)
        kraget.verbose=True

def update_topics(forum_id,playerprofile=True):
    okforplayer = ["fc1", "fc2"] # all messages already read !
    data = kraget("http://www.kraland.org/main.php?page=4;%s;0;0;0&p0=1"%forum_id)
    for line in data.split("</tr>"):
        topic = re.findall('(/4/(...).gif">.*)?'
                           'page=4;([0-9]+);([0-9]+);1;0">(.*)</a></td>'
                           '<td class="info">(<a [^>]+action=rc[^>]+>)?'
                           '([0-9]+)(</a>)?</td>.*'
                           'end">([^<]+)</a>', line)
        if not topic: continue
        (img_markup, topic_img, forum_id, topic_id, topic_nom,
         rc_a, topic_messages, rc_aa, topic_ts) = topic[0]
        url="http://www.kraland.org/main.php?page=4;%s;%s;1;0"%(forum_id,topic_id)
        if playerprofile and topic_img not in okforplayer:
            print "*Skipping %s %s"%(topic_nom,url)
            continue
        update_row("topic", topic_id, topic_nom=topic_nom, forum_id=forum_id)
        print " Fetching %s %s %s (%s)"%(forum_id,topic_nom,url,topic_messages)
        sql.commit()
        n = sql("SELECT COUNT(*) AS c FROM kraland_messages WHERE topic_id=%s", topic_id)[0]["c"]
        topic_messages = int(topic_messages)
        if n<topic_messages:
            for page in range(n/10,(topic_messages+9)/10):
                update_messages(forum_id, topic_id, page+1)

def update_messages(forum, topic, page):
    data = kraget("http://www.kraland.org/main.php?page=4;%s;%s;%s;0"%(forum,topic,page))
    for post in data.split('<a NAME="msg'):
        if "post_container" not in post: continue
        message_id = post.split('"')[0]
        citoyen_id = re.findall('page=5;1;3;1;0&amp;p1=([0-9]+)"',post)
        citoyen_id = citoyen_id and citoyen_id[0] or None
        ts = re.findall("<p>(([0-9]+)/([0-9]+)|Aujourd'hui) \(([0-9]+):([0-9]+)\)</p>", post)[0]
        daymonth,day,month,hour,minute=ts
        year, curmonth, curday = time.localtime()[:3]
        if daymonth=="Aujourd'hui": day,month = curday,curmonth
        if month>curmonth or (month==curmonth and day>curday): year -= 1
        message_ts = "%s-%s-%s %s:%s"%(year,month,day,hour,minute)
        rea, mod, txt = re.findall('<div class="post_central">[ \n\t]*'
                                   '(<div .* 20px;">Réaction à.*</a></div>)?[ \n\t]*'
                                   '(<div class="mod[012]">.*</div>)?[ \n\t]*'
                                   '<p>(.*)</p>[ \n\t]*<hr class="hidden"',
                                   post)[0]
        update_row("message", message_id, citoyen_id=citoyen_id, message_text=txt, topic_id=topic, message_timestamp=message_ts)
    sql.commit()

def forum_check_ts():
    foo = sql("SELECT message_id AS id, "
              "message_timestamp+'1d' AS tsup, "
              "message_timestamp+'-1d' AS tsdown "
              "FROM kraland_messages "
              "ORDER BY message_id ASC")
    prevts = '9999'
    while foo:
        m = foo.pop(-1)
        tsup, tsdown = str(m["tsup"]), str(m["tsdown"])
        if tsdown>prevts:
            print "GAP, %d=%s (previous was %s)"%(m["id"],tsdown,prevts)
            sql("UPDATE kraland_messages "
                "SET message_timestamp=message_timestamp+'-1y' "
                "WHERE message_id <= %s", m["id"])
            sql.commit()
            print "Updated. Restart procedure."
            break
        prevts = tsup

def pos_distance(p1, p2):
    l1, c1 = ord(p1[0]), int(p1[1:])
    l2, c2 = ord(p2[0]), int(p2[1:])
    return abs(l1-l2)+abs(c1-c2)

def calccaisse(cons):
    salaire = cons["construction_salaire"]
    caisse = cons["construction_caisse"]
    approx = cons["construction_approx"]
    if approx==9: cons["caisse"] = ">%d"%(9*salaire)
    elif approx!=None: cons["caisse"] = "~%d"%(approx*salaire)
    elif caisse!=None: cons["caisse"] = "%d"%caisse
    else: cons["caisse"] = "?"
    return cons["caisse"]

if __name__=='__main__':
    cli=CLI()
    if len(sys.argv)>1:
        cli.do_rc(sys.argv[1])
        sys.exit(0)
    kratoolrc = os.path.join(os.environ.get("HOME",""), ".kratoolrc")
    if os.path.isfile(kratoolrc):
        cli.do_rc(kratoolrc)
    cli.setprompt()
    cli.cmdloop()

