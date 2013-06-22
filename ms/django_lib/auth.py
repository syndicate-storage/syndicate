from django.http import HttpResponseRedirect
import storage.storage as db
from MS.user import SyndicateUser as User

def authenticate(f):

    def wrapper(*args, **kw):
        session = args[0].session
        if 'authenticated' in session:
            user = db.read_user(session['login_email'])
            if user:
                return f(*args, **kw)
            else:
                kwargs = {}
                kwargs['email'] = session['login_email']
                kwargs['openid_url'] = session['openid_url']
                user = db.create_user(**kwargs)
            session['user_key'] = user
            return f(*args, **kw)
        else:
            return HttpResponseRedirect('/')

    return wrapper
