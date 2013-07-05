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
from MS.gateway import AcquisitionGateway as AG

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
            kwargs['ms_username'] = post['ag_name']
        else:
            return HttpResponse("Need ag_name in post data.")
        if "ug_password" in post:
            kwargs['ms_password'] = post['ag_password']
        else:
            return HttpResponse("Need ag_hash in post data.")

        if 'volume' in post:
            volume_name = post['volume']
        else:
            return HttpResponse( "Need volume name in post data." )
        try:
            vol = db.read_volume(volume_name)
        except:
            return HttpResponse("No volume %s exists." % volume_name)
        if (vol.volume_id not in user.volumes_r) and (vol.volume_id not in user.volumes_rw):
            return HttpResponse("Must have read rights to volume %s to create AG for it." % volume_name)

        try:
            new_ug = db.create_acquisition_gateway(vol, **kwargs)
        except Exception as E:
            return HttpResponse("AG creation error: %s" % E)

        return HttpResponse("AG succesfully created: " + str(new_ug))
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
        if "ag_name" in post:
            ag = db.read_acquisition_gateway(post['ag_name'])
            if not ug:
                return HttpResponse("AG %s does not exist." % post['ag_name'])
            if ag.owner_id != user.owner_id:
                return HttpResponse("You must own this Ag to delete it.")
        else:
            return HttpResponse("Need ag_name in post data.")
        if "ag_password" in post:
            if not AG.authenticate(ag, post['ug_password']):
                return HttpResponse("Incorrect AG password.")
        else:
            return HttpResponse("Need ag_password.")
        db.delete_acquisition_gateway(post['ag_name'])
        return HttpResponse("Gateway succesfully deleted.")
    else:
        return HttpResponse("Hi")

@csrf_exempt
@authenticate
def urlcreate(request, volume_name, ag_name, ag_password, host, port):
    session = request.session
    username = session['login_email']
    user = db.read_user(username)

    kwargs = {}

    kwargs['port'] = int(port)
    kwargs['host'] = host
    kwargs['ms_username'] = ag_name
    kwargs['ms_password'] = ag_password
    vol = db.read_volume(volume_name)
    if not vol:
        return HttpResponse("No volume %s exists." % volume_name)
    if (vol.volume_id not in user.volumes_r) and (vol.volume_id not in user.volumes_rw):
        return HttpResponse("Must have read rights to volume %s to create AG for it." % volume_name)

    try:
        new_ag = db.create_acquisition_gateway(vol, **kwargs)
    except Exception as E:
        return HttpResponse("AG creation error: %s" % E)

    return HttpResponse("AG succesfully created: " + str(new_ag))

@csrf_exempt
@authenticate
def urldelete(request, AG_name, AG_password):
    session = request.session
    username = session['login_email']
    user = db.read_user(username)

    ag = db.read_user_gateway(ag_name)
    if not ag:
        return HttpResponse("AG %s does not exist." % ag_name)
    if ag.owner_id != user.owner_id:
        return HttpResponse("You must own this AG to delete it.")
    if not AG.authenticate(ag, ag_password):
        return HttpResponse("Incorrect AG password.")
    db.delete_user_gateway(ag_name)
    return HttpResponse("Gateway succesfully deleted.")
