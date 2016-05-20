'''
Created on May 20, 2016

@author: zimmer
@brief: profile applications
'''

def main(args):
    from werkzeug.contrib.profiler import ProfilerMiddleware
    from DmpWorkflow.core import app
    app.config['PROFILE'] = True
    app.wsgi_app = ProfilerMiddleware(app.wsgi_app, restrictions=[30])
    app.run(debug = True)

if __name__ == '__main__':
    main()
