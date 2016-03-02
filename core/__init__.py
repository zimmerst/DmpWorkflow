import ConfigParser, os
from flask import Flask
#from flask.ext.basicauth import BasicAuth
from flask.ext.mongoengine import MongoEngine

cfg = ConfigParser.RawConfigParser()
cfg.read(os.getenv("WorkflowConfig","config/default.cfg")

app = Flask(__name__)
app.config['MONGODB_DB']=cfg.get("database","name")
app.config['MONGODB_USERNAME']=cfg.get("database","user")
app.config['MONGODB_PASSWORD']=cfg.get("database","password")
app.config['MONGODB_HOST']=cfg.get("database","host")
db = MongoEngine(app)

#def register_blueprints(app):
#    # Prevents circular imports
#    from tumblelog.views import posts
#    app.register_blueprint(posts)

#register_blueprints(app)

if __name__ == '__main__':
    app.run()