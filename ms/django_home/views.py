'''

All of these views are predicated on the user already being logged in to
valid session.

djago_ag/views.py
John Whelchel
Summer 2013

These are the basic home views for the metadata service. They include
the home page, a thanks redirect page, and the ability to logout.

'''
from django.http import HttpResponse, HttpResponseRedirect
from django.template import Context, loader, RequestContext

from django_lib.auth import authenticate

@authenticate
def thanks(request):
    '''
    This view handles all redirects as a landing page after updating the database, allowing the server
    to serve fresh data instead of stale data (i.e. by immediately going back to the previous page.)
    '''
    session = request.session
    username = session['login_email']
    new_change = session.pop('new_change', "")
    next_url = session.pop('next_url', "")
    next_message = session.pop('next_message', "")
    
    t = loader.get_template('thanks.html')
    c = Context({'username':username, 'new_change':new_change, 'next_url':next_url, 'next_message':next_message})
    return HttpResponse(t.render(c))

@authenticate
def home(request):
    '''
    This view is the main homepage.
    '''
    session = request.session
    username = session['login_email']

#   Uncomment this line to test security via @authenticate on any page
#    session.clear()

    t = loader.get_template('gumby_templates/home.html')
    c = Context({'username':username})
    return HttpResponse(t.render(c))

@authenticate
def logout(request):
    '''
    This view logs a User out of Syndicate.
    '''
    session = request.session
    session.terminate()
    return HttpResponseRedirect('/')



@authenticate
def providers(request):
    return HttpResponseRedirect('/syn')

@authenticate
def tutorial(request):
    return HttpResponseRedirect('/syn')

@authenticate
def faq(request):
    return HttpResponseRedirect('/syn')

@authenticate
def about(request):
    return HttpResponseRedirect('/syn')


