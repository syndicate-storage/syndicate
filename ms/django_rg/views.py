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
from MS.gateway import ReplicaGateway as RG

@csrf_exempt
@authenticate
def create(request):
    session = request.session
    username = session['login_email']
    user = db.read_user(username)

    if request.POST:
        post = request.POST.dict()
        # Create RG
        kwargs = {}

        if "port" in post:
            kwargs['port'] = int(post['port'])
        else:
            return HttpResponse("Need port number in post data.")
        if "host" in post:
            kwargs['host'] = post['host']
        else:
            return HttpResponse("Need host name in post data.")
        if "rg_name" in post:
            kwargs['ms_username'] = post['rg_name']
        else:
            return HttpResponse("Need rg_name in post data.")
        if "rg_password" in post:
            kwargs['ms_password'] = post['rg_password']
        else:
            return HttpResponse("Need rg_hash in post data.")

        if 'volume' in post:
            volume_name = post['volume']
        else:
            return HttpResponse( "Need volume name in post data." )
        try:
            vol = db.read_volume(volume_name)
        except:
            return HttpResponse("No volume %s exists." % volume_name)
        if (vol.volume_id not in user.volumes_r) and (vol.volume_id not in user.volumes_rw):
            return HttpResponse("Must have read rights to volume %s to create RG for it." % volume_name)

        try:
            new_rg = db.create_replica_gateway(vol, **kwargs)
        except Exception as E:
            return HttpResponse("RG creation error: %s" % E)

        return HttpResponse("RG succesfully created: " + str(new_rg))
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
        if "rg_name" in post:
            rg = db.read_replica_gateway(post['rg_name'])
            if not rg:
                return HttpResponse("RG %s does not exist." % post['rg_name'])
            if rg.owner_id != user.owner_id:
                return HttpResponse("You must own this RG to delete it.")
        else:
            return HttpResponse("Need rg_name in post data.")
        if "rg_password" in post:
            if not RG.authenticate(rg, post['rg_password']):
                return HttpResponse("Incorrect RG password.")
        else:
            return HttpResponse("Need rg_password.")
        db.delete_replica_gateway(post['rg_name'])
        return HttpResponse("Gateway succesfully deleted.")
    else:
        return HttpResponse("Hi")

@csrf_exempt
@authenticate
def urlcreate(request, volume_name, rg_name, rg_password, host, port):
    session = request.session
    username = session['login_email']
    user = db.read_user(username)

    kwargs = {}

    kwargs['port'] = int(port)
    kwargs['host'] = host
    kwargs['ms_username'] = rg_name
    kwargs['ms_password'] = rg_password
    vol = db.read_volume(volume_name)
    if not vol:
        return HttpResponse("No volume %s exists." % volume_name)
    if (vol.volume_id not in user.volumes_r) and (vol.volume_id not in user.volumes_rw):
        return HttpResponse("Must have read rights to volume %s to create RG for it." % volume_name)

    try:
        new_rg = db.create_replica_gateway(vol, **kwargs)
    except Exception as E:
        return HttpResponse("RG creation error: %s" % E)

    return HttpResponse("RG succesfully created: " + str(new_rg))

@csrf_exempt
@authenticate
def urldelete(request, rg_name, rg_password):
    session = request.session
    username = session['login_email']
    user = db.read_user(username)

    rg = db.read_replica_gateway(rg_name)
    if not rg:
        return HttpResponse("RG %s does not exist." % rg_name)
    if rg.owner_id != user.owner_id:
        return HttpResponse("You must own this RG to delete it.")
    if not RG.authenticate(rg, rg_password):
        return HttpResponse("Incorrect RG password.")
    db.delete_replica_gateway(rg_name)
    return HttpResponse("Gateway succesfully deleted.")
