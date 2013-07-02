'''
We're going to assume you need to be logged in already and have a valid session
for this to work.
'''
import logging

from django_lib.auth import authenticate
from django.http import HttpResponse, HttpResponseRedirect
from django.template import Context, loader, RequestContext
from django.views.decorators.csrf import csrf_exempt, csrf_protect

from storage.storagetypes import transactional
import storage.storage as db
from MS.volume import Volume
from MS.user import SyndicateUser as User
from MS.gateway import UserGateway as UG

@csrf_exempt
@authenticate
def create(request):
    session = request.session
    username = session['login_email']
    user = db.read_user(username)

    if request.POST:
        post = request.POST.dict()
        # Create UG
        kwargs = {}

        if "port" in post:
            kwargs['port'] = int(post['port'])
        else:
            return HttpResponse("Need port number in post data.")
        if "host" in post:
            kwargs['host'] = post['host']
        else:
            return HttpResponse("Need host name in post data.")
        if "ug_name" in post:
            kwargs['ms_username'] = post['ug_name']
        else:
            return HttpResponse("Need ug_name in post data.")
        if "ug_password" in post:
            kwargs['ms_password'] = post['ug_password']
        else:
            return HttpResponse("Need ug_hash in post data.")

        if 'volume' in post:
            volume_name = post['volume']
        else:
            return HttpResponse( "Need volume name in post data." )
        try:
            vol = db.read_volume(volume_name)
        except:
            return HttpResponse("No volume %s exists." % volume_name)
        if (vol.volume_id not in user.volumes_r) and (vol.volume_id not in user.volumes_rw):
            return HttpResponse("Must have read rights to volume %s to create UG for it." % volume_name)

        try:
            new_ug = db.create_user_gateway(user, vol, **kwargs)
        except Exception as E:
            return HttpResponse("UG creation error: %s" % E)

        return HttpResponse("UG succesfully created: " + str(new_ug))
    else:
        return HttpResponse("Hi")

@csrf_exempt
@authenticate
def delete(request):
    session = request.session
    username = session['login_email']
    user = db.read_user(username)
    if request.POST:
        post = request.POST.dict()
        if "ug_name" in post:
            ug = db.read_user_gateway(post['ug_name'])
            if ug.owner_id != user.owner_id:
                return HttpResponse("You must own this UG to delete it.")
        else:
            return HttpResponse("Need ug_name in post data.")
        if "ug_password" in post:
            if not UG.authenticate(ug, post['ug_password']):
                return HttpResponse("Incorrect UG password.")
        else:
            return HttpResponse("Need ug_password.")
        db.delete_user_gateway(post['ug_name'])
        return HttpResponse("Gateway succesfully deleted.")
    else:
        return HttpResponse("Hi")