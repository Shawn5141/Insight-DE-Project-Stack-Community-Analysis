
class config:
    def __init__(self):
        # connect to asw
        self.host_ip='172.31.63.19'
        self.database='stackOverflowDatabase'
        self.username='dbuser'
        self.password='00000000'
        self.port = 5432
        self.region='us-west-2'
        
        # Bucket Initialization
        bucket='stackoverflowdumpdata'
        bucketparquet = 'stackoverflowparquet'
        self.bucketTag ='stackoverflow-tag'
        # Xml path
        Badges,Comments,PostHistory,PostLinks,Posts ,Tags,Users,Votes= 'Badges.xml','Comments.xml','PostHistory.xml','PostLinks.xml','Posts.xml','Tags.xml','Users.xml','Votes.xml'
        self.s3file_Badges = f's3a://{bucket}/{Badges}' 
        self.s3file_Comments = f's3a://{bucket}/{Comments}' 
        self.s3file_PostHistory = f's3a://{bucket}/{PostHistory}' 
        self.s3file_PostLinks = f's3a://{bucket}/{PostLinks}' 
        self.s3file_Posts = f's3a://{bucket}/{Posts}' 
        self.s3file_Tags = f's3a://{bucket}/{Tags}' 
        self.s3file_Users = f's3a://{bucket}/{Users}'
        self.s3file_Votes = f's3a://{bucket}/{Votes}'
        
        # Parquet path
        Badges_Parquet,Comments_Parquet,PostHistory_Parquet,PostLinks_Parquet,Posts_Parquet,Answers_Parquet ,Tags_Parquet,Users_Parquet,Votes_Parquet= 'Badges.parquet','Comments.parquet','PostHistory.parquet','PostLinks.parquet','Posts.parquet','Answers.Parquet','Tags.parquet','Users.parquet','Votes.parquet'
        self.s3file_parquet_Badges = f's3a://{bucketparquet}/{Badges_Parquet}' 
        self.s3file_parquet_Comments = f's3a://{bucketparquet}/{Comments_Parquet}' 
        self.s3file_parquet_PostHistory = f's3a://{bucketparquet}/{PostHistory_Parquet}' 
        self.s3file_parquet_PostLinks = f's3a://{bucketparquet}/{PostLinks_Parquet}' 
        self.s3file_parquet_Posts = f's3a://{bucketparquet}/{Posts_Parquet}' 
        self.s3file_parquet_Answers = f's3a://{bucketparquet}/{Answers_Parquet}' 
        self.s3file_parquet_Tags = f's3a://{bucketparquet}/{Tags_Parquet}' 
        self.s3file_parquet_Users = f's3a://{bucketparquet}/{Users_Parquet}'
        self.s3file_parquet_Votes = f's3a://{bucketparquet}/{Votes_Parquet}'
        
        # stackoverflow-tag-synonyms-list
        self.TagName ='TagName.csv'
        self.TagSyn = 'TagSyn.csv'
        self.TagNameLink = f's3a://{self.bucketTag}/{self.TagName}'
        self.TagSynLink = f's3a://{self.bucketTag}/{self.TagSyn}'
        # Postgres connection
        
        self.jdbcUrl = "jdbc:postgresql://{0}:{1}/{2}".format(self.host_ip, self.port, self.database)
        self.connectionProperties = {
          "user" : self.username,
          "password" : self.password,
          "driver": "org.postgresql.Driver"
        }
    

