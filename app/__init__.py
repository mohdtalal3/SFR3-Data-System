from flask import Flask, g
from flask_bootstrap import Bootstrap
import db_connector
import atexit
import logging

logger = logging.getLogger(__name__)

def create_app():
    app = Flask(__name__)
    app.config['SECRET_KEY'] = 'your-secret-key'
    
    # Initialize Flask-Bootstrap
    Bootstrap(app)
    
    # Initialize database connection
    if not db_connector.initialize_db():
        logger.error("Failed to initialize database connection during app startup")
    
    # Register shutdown handler to close DB connection when app exits
    atexit.register(db_connector.close_db_connection)
    
    @app.teardown_appcontext
    def close_db_connection(error):
        # This will not fully close the connection but will release any resources
        # associated with the current request. The actual connection will be
        # closed by the atexit handler when the app exits.
        if error:
            logger.error(f"Error during request: {error}")
    
    # Register blueprints
    from app.routes.main import main_bp
    from app.routes.scraper import scraper_bp
    from app.routes.checker import checker_bp
    
    app.register_blueprint(main_bp)
    app.register_blueprint(scraper_bp, url_prefix='/scraper')
    app.register_blueprint(checker_bp, url_prefix='/checker')
    
    return app 