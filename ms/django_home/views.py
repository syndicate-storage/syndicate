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
    new_change = session.pop('new_change', "")
    next_url = session.pop('next_url', "")
    next_message = session.pop('next_message', "")
    
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
#   Uncomment this line to test security via @authenticate on any page
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