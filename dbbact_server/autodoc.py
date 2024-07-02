# the autodoc variable for obtaining the REST API documentation for all user exposed APIs
# from flask.ext.autodoc import Autodoc

# from flask_autodoc import Autodoc
# changed to flask_selfdoc as flask_autodoc is not maintained (https://github.com/jwg4/flask-selfdoc)
from flask_selfdoc import Autodoc

auto = Autodoc()
