import logging
from logging.handlers import RotatingFileHandler

from DmpWorkflow.config.defaults import cfg
from flask import Flask
from flask.ext.mongoengine import MongoEngine

kind = cfg.get("global","installation")
LOG_FILENAME = cfg.get("server","logfile")


if kind == 'server':
    app = Flask(__name__)
    app.config['MONGODB_DB'] = cfg.get("database", "name")
    app.config['MONGODB_USERNAME'] = cfg.get("database", "user")
    app.config['MONGODB_PASSWORD'] = cfg.get("database", "password")
    app.config['MONGODB_HOST'] = cfg.get("database", "host")
    app.config["SECRET_KEY"] = "KeepThisS3cr3t"
 
    formatter = logging.Formatter(
        "[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s")
    handler = RotatingFileHandler(LOG_FILENAME, maxBytes=10000000, backupCount=5)
    debug = cfg.getboolean("server","use_debugger")
    if debug: handler.setLevel(logging.DEBUG)
    else: handler.setLevel(logging.WARNING)
    handler.setFormatter(formatter)
    app.logger.addHandler(handler)
    
    db = MongoEngine(app)
    
    def register_blueprints(app):
        # Prevents circular imports
        from DmpWorkflow.core.views import jobs
        from DmpWorkflow.core.admin import admin
        app.register_blueprint(jobs)
        app.register_blueprint(admin)
    

    register_blueprints(app)
    
    
    def main():
        app.run()


if __name__ == '__main__':
    if kind == 'server':
        main()
