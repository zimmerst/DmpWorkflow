# Set the path
from DmpWorkflow.config.defaults import cfg, os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from flask.ext.script import Manager, Server, Shell
from DmpWorkflow.core import app, db, models

def _make_context():
    return dict(app=app, db=db, models=models)


manager = Manager(app)
# Turn on debugger by default and reloader
manager.add_command("runserver", Server(use_debugger=cfg.getboolean("server", "use_debugger"),
                                        use_reloader=cfg.getboolean("server", "use_reloader"),
                                        host=cfg.get("server", "host"))
                    )

manager.add_command("shell", Shell(make_context=_make_context))


def main():
    manager.run()


if __name__ == "__main__":
    main()
