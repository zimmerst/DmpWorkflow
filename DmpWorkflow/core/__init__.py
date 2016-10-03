from DmpWorkflow.config.defaults import cfg
from DmpWorkflow import version
from socket import getfqdn
kind = cfg.get("global", "installation")

if kind == 'server':    
    
    from flask import Flask
    from flask.ext.mongoengine import MongoEngine
    app = Flask(__name__)
    app.config.update(LOGGER_NAME="core")
    app.config['MONGODB_DB'] = cfg.get("database", "name")
    app.config['MONGODB_USERNAME'] = cfg.get("database", "user")
    app.config['MONGODB_PASSWORD'] = cfg.get("database", "password")
    app.config['MONGODB_HOST'] = cfg.get("database", "host")
    app.config['MONGODB_PORT'] = int(cfg.get("database", "port"))
    # make connection numbers unbound
    app.config['MONGODB_MAXPOOLSIZE'] = None 
    # time out after 100ms
    app.config['MONGODB_WAITQUEUETIMEOUTMS'] = 100
    app.config["SECRET_KEY"] = "KeepThisS3cr3t"
    db = MongoEngine(app)
    
    def register_blueprints(app):
        # Prevents circular imports
        from DmpWorkflow.core.views import jobs
        from DmpWorkflow.core.admin import admin
        app.register_blueprint(jobs)
        app.register_blueprint(admin)
    

    register_blueprints(app)
    
    def main():
        app.logger.info("started DmpWorkflow Server Version: %s on %s",version,getfqdn())
        app.run()
else:
    def main():
        pass

if __name__ == '__main__':
    if kind == 'server':
        main()
