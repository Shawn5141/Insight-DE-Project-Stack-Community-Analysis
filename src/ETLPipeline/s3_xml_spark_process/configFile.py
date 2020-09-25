
class config:
    def __init__(self):
        self.host_ip='172.31.63.19'
        self.database='stackOverflowDatabase'
        self.username='dbuser'
        self.password='00000000'
        self.port = 5432
        self.region='us-west-2'
        bucket='stackoverflowdumpdata'
        key = 'Badges.xml'
        self.s3file = f's3a://{bucket}/{key}' 
        #self.jdbcUrl = "jdbc:postgres://{0}:{1}/{2}?user={3}&password={4}".format(self.host_ip, self.port, self.database, self.username, self.password)
        self.jdbcUrl = "jdbc:postgresql://{0}:{1}/{2}".format(self.host_ip, self.port, self.database)
        self.connectionProperties = {
          "user" : self.username,
          "password" : self.password,
          "driver": "org.postgresql.Driver"
        }

