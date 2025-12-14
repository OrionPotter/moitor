# api/__init__.py
from .portfolio_routes import portfolio_routes
from .monitor_routes import monitor_routes
from .admin_routes import admin_routes

__all__ = ['portfolio_routes', 'monitor_routes', 'admin_routes']