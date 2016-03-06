import ConfigParser, os
from flask import Flask
#from flask.ext.basicauth import BasicAuth
#from flask.ext.pymongo import PyMongo
from flask.ext.mongoengine import MongoEngine

cfg = ConfigParser.SafeConfigParser()
cfg.read(os.getenv("WorkflowConfig","config/dampe.cfg"))

app = Flask(__name__)

## using pymongo -- don't like that very much
#app.config['PREFIX_DBNAME']=cfg.get("database","name")
#app.config['PREFIX_USERNAME']=cfg.get("database","user")
#app.config['PREFIX_PASSWORD']=cfg.get("database","password")
#app.config['PREFIX_HOST']=cfg.get("database","host")
#db = PyMongo(app)

## using mongoengine - for now this is good enough, eventually this will be sqlalchemy anyways... probably
app.config['MONGODB_DB']=cfg.get("database","name")
app.config['MONGODB_USERNAME']=cfg.get("database","user")
app.config['MONGODB_PASSWORD']=cfg.get("database","password")
app.config['MONGODB_HOST']=cfg.get("database","host")
app.config["SECRET_KEY"] = "KeepThisS3cr3t"
db = MongoEngine(app)

def register_blueprints(app):
    # Prevents circular imports
    from core.views import jobs
    app.register_blueprint(jobs)

register_blueprints(app)

if __name__ == '__main__':
    app.run()