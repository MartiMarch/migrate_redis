import redis

class redisManager:
        host = ""
        port = 0
        db = 0
        password = ""
        user = ""
        ssl = False
        ssl_cert = None
        health_check_interval = 0
        connection = None

        def __init__(self, host, port, db, password, user, ssl, ssl_cert, version):
                self.host = host
                self.port = port
                self.db = db
                self.password = password
                self.ssl = ssl
                self.ssl_cert = ssl_cert
                if version <= 5:
                        self.connection = redis.Redis(
                                host=self.host,
                                port=self.port,
                                password=self.password,
                                db=self.db,
                                ssl=self.ssl,
                                ssl_ca_certs=self.ssl_cert)
                elif version >= 6:
                        self.user = user
                        self.connection = redis.Redis(
                                host=self.host,
                                port=self.port,
                                password=self.password,
                                username=self.user,
                                db=self.db,
                                ssl=self.ssl,
                                ssl_ca_certs=self.ssl_cert)

        def getConnection():
                return self.connection

        def ping(self):
                return self.connection.ping()

        def getSize(self):
                return self.connection.dbsize()

        def getKeys(self):
                keys = []
                for key in self.connection.keys('*'):
                        keys.append(key)
                return keys

        def getDB(self, keys):
                n = 0
                size = self.getSize()
                db = []
                for key in keys:
                        try:
                                value = self.connection.dump(key)
                                rc = redisComponent(key, value)
                                db.append(rc)
                        except:
                                n += 1
                                print("Failed getting key " + str(n) + "/" + str(size) + ": " + str(key))
                return db

        def getTTL(self, key):
                return  self.connection.ttl(key)

        def loadDB(self, db, oldRedis):
                for rc in db:
                        ttl = oldRedis.getTTL(rc.getKey())
                        if ttl < 0:
                                ttl = 0
                        self.connection.restore(rc.getKey(), ttl * 1000, rc.getValue(), replace=True)

class redisComponent:
        key = None
        value = None

        def __init__(self, key, value):
                self.key = key
                self.value = value

        def getKey(self):
                return self.key

        def getValue(self):
                return self.value

OLD_HOST = ""
OLD_PORT = 0
OLD_PASS = ""
OLD_USER = None
#OLD_DB = 0
OLD_SSL = True
OLD_SSL_CERT = "…/oldCert.crt"
OLD_VER = 4

NEW_HOST = ""
NEW_PORT = 0
NEW_PASS = ""
NEW_USER = ""
#NEW_DB = 0
NEW_SSL = True
NEW_SSL_CERT = "…/newCert.crt"
NEW_VER = 6

n = 0
print("\n")
while n < 16:
        print("**** Migrando la base de datos " + str(n) + " ****")
        oldRedis = redisManager(OLD_HOST, OLD_PORT, n, OLD_PASS, OLD_USER, OLD_SSL, OLD_SSL_CERT, OLD_VER)
        print("* Estado de la conexion con la base de datos de origen: " + str(oldRedis.ping()))
        newRedis = redisManager(NEW_HOST, NEW_PORT, n, NEW_PASS, NEW_USER, NEW_SSL, NEW_SSL_CERT, NEW_VER)
        print("* Estado de la conexion con la base de datos de destino: " + str(newRedis.ping()))
        oldKeys = oldRedis.getKeys()
        db = oldRedis.getDB(oldKeys)
        print("* Migrando la base de datos " + str(n))
        newRedis.loadDB(db, oldRedis)
        print("* " + str(newRedis.getSize()) + "/" + str(oldRedis.getSize()) + " elementos migrados")
        print("*************************************\n")
        n += 1
