from django.http import HttpResponseRedirect, HttpResponse
import storage.storage as db
from django.template import Context, loader
from MS.user import SyndicateUser as User

from storage.storagetypes import transactional

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

def verifyownership(f):

    def wrapper(*args, **kw):
        session = args[0].session
        user = db.read_user(session['login_email'])
        try:
            vol = db.read_volume(kw['volume_name'])
        except:
            t = loader.get_template('viewvolume_failure.html')
            c = Context({'username':session['login_email']})
            return HttpResponse(t.render(c))
        if user.owner_id != vol.owner_id:
            t = loader.get_template('viewvolume_failure.html')
            c = Context({'username':session['login_email']})
            return HttpResponse(t.render(c))
        return f(*args, **kw)
    
    return wrapper

