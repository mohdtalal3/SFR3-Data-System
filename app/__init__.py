from flask import Flask
from flask_bootstrap import Bootstrap

def create_app():
    app = Flask(__name__)
    app.config['SECRET_KEY'] = 'your-secret-key'
    
    # Initialize Flask-Bootstrap
    Bootstrap(app)
    
    # Register blueprints
    from app.routes.main import main_bp
    from app.routes.scraper import scraper_bp
    from app.routes.checker import checker_bp
    
    app.register_blueprint(main_bp)
    app.register_blueprint(scraper_bp, url_prefix='/scraper')
    app.register_blueprint(checker_bp, url_prefix='/checker')
    
    return app 