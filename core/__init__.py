import ConfigParser, os
from flask import Flask
from flask.ext.mongoengine import MongoEngine

cfg = ConfigParser.SafeConfigParser()
cfg.read(os.getenv("WorkflowConfig","config/dampe.cfg"))

app = Flask(__name__)
app.config['MONGODB_DB']=cfg.get("database","name")
app.config['MONGODB_USERNAME']=cfg.get("database","user")
app.config['MONGODB_PASSWORD']=cfg.get("database","password")
app.config['MONGODB_HOST']=cfg.get("database","host")
app.config["SECRET_KEY"] = "KeepThisS3cr3t"
db = MongoEngine(app)


def register_blueprints(app):
    # Prevents circular imports
    from core.views import jobs
    from core.admin import admin
    app.register_blueprint(jobs)
    app.register_blueprint(admin)

register_blueprints(app)

if __name__ == '__main__':
    app.run()