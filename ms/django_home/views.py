from django.http import HttpResponse, HttpResponseRedirect
from django.template import Context, loader, RequestContext

from django_lib.auth import authenticate

from storage.storagetypes import transactional

import storage.storage as db
from MS.volume import Volume
from MS.user import SyndicateUser as User
from MS.gateway import UserGateway as UG,  ReplicaGateway as RG, AcquisitionGateway as AG

@authenticate
def thanks(request):
    session = request.session
    username = session['login_email']
    new_change = session['new_change']
    next_url = session['next_url']
    next_message = session['next_message']
    t = loader.get_template('thanks.html')
    c = Context({'username':username, 'new_change':new_change, 'next_url':next_url, 'next_message':next_message})
    return HttpResponse(t.render(c))


@authenticate
def make_user(request, email):
    kwargs = {}
    kwargs['email'] = email
    kwargs['openid_url'] = 'http://www.vicci.org/id/' + email
    user = db.create_user(**kwargs)
    return HttpResponseRedirect('/syn/')

@authenticate
def home(request):
    session = request.session
    username = session['login_email']
#   Uncomment this line to test security via @authenticate
#    session.clear()
    t = loader.get_template('home.html')
    c = Context({'username':username})
    return HttpResponse(t.render(c))

@authenticate
def logout(request):
    session = request.session
    session.terminate()
    return HttpResponseRedirect('/')
    

@authenticate
def allvolumes(request):
    session = request.session
    username = session['login_email']
    # all queries to be replaced with db.list_all()
    volumes = Volume.query()
    owners = []
    for v in volumes:
        volume_owner = v.owner_id
        qry = User.query(User.owner_id == volume_owner)
        for owner in qry:
            owners.append(owner)
    vols = zip(volumes, owners)
    t = loader.get_template('allvolumes.html')
    c = Context({'username':username, 'vols':vols})
    return HttpResponse(t.render(c))

@authenticate
def myvolumes(request):
    session = request.session
    username = session['login_email']
    user = db.read_user_fresh(username)

    attrs = {}
    if user.volumes_o:
        attrs['Volume.volume_id'] = ".IN(%s)" % str(user.volumes_o)
        myvols = db.list_volumes(**attrs)
    else:
        myvols = []
    all_users = []

    for v in myvols:
        uattrs = {}
        users_set = []
        uattrs['SyndicateUser.volumes_rw'] = "== %s" % v.volume_id 
        q = db.list_users(**uattrs)
        for u in q:
            users_set.append(u)
        uattrs = {}
        uattrs['SyndicateUser.volumes_r'] = "== %s" % v.volume_id 
        q = db.list_users(**uattrs)
        for u in q:
            users_set.append(u)
        all_users.append(users_set)
            

    vols_users =zip(myvols, all_users)
    t = loader.get_template('myvolumes.html')
    c = Context({'username':username, 'vols':vols_users})
    return HttpResponse(t.render(c))

@authenticate
def settings(request, message=""):
    session = request.session
    username = session['login_email']


    t = loader.get_template('settings.html')
    c = RequestContext(request, {'username':username,
                                 'message':message,
                                 } )
    return HttpResponse(t.render(c))


@authenticate
def downloads(request):
    session = request.session
    username = session['login_email']
    t = loader.get_template('downloads.html')
    c = Context({'username':username})
    return HttpResponse(t.render(c))