'''

John Whelchel
Summer 2013

Helper library for the django metadata service simplifying
authentication of users.

'''

from django.http import HttpResponse
from django.shortcuts import redirect
from django.template import Context, loader

import storage.storage as db
from MS.user import SyndicateUser as User
from storage.storagetypes import transactional

def authenticate(f):
    '''
    Decorator for any view that ensures the user is logged in before
    handling their request.
    '''
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
            return redirect('/')

    return wrapper

def verifyownership_private(f):
    '''
    Decorator for any volume view that ensures the user owns the volume
    before they can view information about it if the volume is private.
    '''

    def wrapper(*args, **kw):
        session = args[0].session
        user = db.read_user(session['login_email'])
        vol = db.read_volume(kw['volume_id'])
        if not vol:
            return redirect('django_volume.views.failure')
        if not vol.private:
            return f(*args, **kw)
        elif user.owner_id != vol.owner_id:
            return redirect('django_volume.views.failure')
        return f(*args, **kw)
    
    return wrapper

def verifyownership(f):
    '''
    Decorator for any volume view that ensures the user owns the volume
    before they can view information about it. Not really used,
    but available.
    '''

    def wrapper(*args, **kw):
        session = args[0].session
        user = db.read_user(session['login_email'])
        vol = db.read_volume(kw['volume_id'])
        if not vol:
            return redirect('django_volume.views.failure')
        if user.owner_id != vol.owner_id:
            return redirect('django_volume.views.failure')
        return f(*args, **kw)
    
    return wrapper