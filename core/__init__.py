import ConfigParser, os
from flask import Flask
#from flask.ext.basicauth import BasicAuth
from flask.ext.pymongo import PyMongo

cfg = ConfigParser.SafeConfigParser()
cfg.read(os.getenv("WorkflowConfig","config/default.cfg"))

app = Flask(__name__)
app.config['PREFIX_DBNAME']=cfg.get("database","name")
app.config['PREFIX_USERNAME']=cfg.get("database","user")
app.config['PREFIX_PASSWORD']=cfg.get("database","password")
app.config['PREFIX_HOST']=cfg.get("database","host")
db = PyMongo(app)

#def register_blueprints(app):
#    # Prevents circular imports
#    from tumblelog.views import posts
#    app.register_blueprint(posts)

#register_blueprints(app)

if __name__ == '__main__':
    app.run()