from django.http import HttpResponse, HttpResponseRedirect
from django.template import Context, loader, RequestContext

from django_lib.auth import authenticate
import django_home.forms as forms

import storage.storage as db
from MS.volume import Volume
from MS.user import SyndicateUser as User
from MS.gateway import UserGateway as UG,  ReplicaGateway as RG, AcquisitionGateway as AG


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
    user = db.read_user(username)
    vols = Volume.query()
    vols = vols.filter(Volume.owner_id == user.owner_id)
    t = loader.get_template('myvolumes.html')
    c = Context({'username':username, 'vols':vols})
    return HttpResponse(t.render(c))

# This view currently doesn't check to see if you
# are an owner because update_user isn't ready in createvolume views.
@authenticate
def viewvolume(request, volume_id):
    session = request.session
    username = session['login_email']
    user = db.read_user(username)
#    owner = User.query(User.volumes == volume_id)
#    for o in owner:
 #       if o.owner_id = user.owner_id:
            
#    for user_volume_id in user.volumes:
#        if user_volume_id == volume_id:
    q = Volume.query(Volume.volume_id == int(volume_id))
    for volume in q: # Just one volume should be returned. Ugly code sorry.
        v = volume  
    t = loader.get_template('viewvolume.html')
    c = Context({'username':username, 'volume':v})
    return HttpResponse(t.render(c))
        
#    user_id = user.owner_id
#    vols = Volume.query(Volume.owner_id == user_id)
#    for v in vols:
#        if v.volume_id == volume_id:
#            t = loader.get_template('viewvolume.html')
#            c = Context({'username':username, 'volume':v})
#            return HttpResponse(t.render(c))
#
    t = loader.get_template('viewvolume_failure.html')
    c = Context({'username':username})
    return HttpResponse(t.render(c))

@authenticate
def settings(request):
    session = request.session
    username = session['login_email']
    t = loader.get_template('settings.html')
    # myvol = Queryset.getvols(user=user)
    c = Context({'username':username})
    return HttpResponse(t.render(c))

@authenticate
def downloads(request):
    session = request.session
    username = session['login_email']
    t = loader.get_template('downloads.html')
    c = Context({'username':username})
    return HttpResponse(t.render(c))

@authenticate
def createvolume(request):
    session = request.session
    username = session['login_email']

    message = ""

    if request.method == "POST":

        # Validate input forms
        form = forms.CreateVolume(request.POST)
        if form.is_valid():

            # Ensure volume name is unique
            if db.read_volume(form.cleaned_data['name']):
                message = "A volume with the name '{}' already exists.".format(form.cleaned_data['name'])
                form = forms.CreateVolume()
                t = loader.get_template('createvolume.html')
                c = RequestContext(request, {'username':username,'form':form, 'message':message})
                return HttpResponse(t.render(c))

            # CREATE VOLUME
            kwargs = {}
            kwargs['name'] = form.cleaned_data['name']
            kwargs['blocksize'] = form.cleaned_data['blocksize']
            kwargs['description'] = form.cleaned_data['description']
            kwargs['volume_secret'] = form.cleaned_data['password']
            user = db.read_user(username)
            volume_key = db.create_volume(user, **kwargs)
        
            user_volumes = user.volumes
            if user_volumes is None:
                user.volumes = [volume_key.get().volume_id]
            else:
                user.volumes.append(volume_key.get().volume_id)

            # This isn't iplemented yet but is key
            # db.update_user(username, FIELDS)

            return HttpResponseRedirect('/syn/myvolumes/')

        else:

            # Prep returned form values (so they don't have to re-enter stuff)

            if 'name' in form.errors:
                oldname = ""
            else:
                oldname = form.cleaned_data['name']
            if 'blocksize' in form.errors:
                oldblocksize = ""
            else:
                oldblocksize = form.cleaned_data['blocksize']
            if 'description' in form.errors:
                olddescription = ""
            else:
                olddescription = form.cleaned_data['description']

            # Prep error message
            message = "Invalid form entry: "

            for k, v in form.errors.items():
                message = message + "\"" + k + "\"" + " -> " 
                for m in v:
                    message = message + m + " "

            # Give then the form again
            form = forms.CreateVolume({'name': oldname,
                                       'blocksize': oldblocksize,
                                       'description': olddescription
                                       })
            t = loader.get_template('createvolume.html')
            c = RequestContext(request, {'username':username,'form':form, 'message':message})
            return HttpResponse(t.render(c))

    # Not a POST, give them blank form
    form = forms.CreateVolume()
    t = loader.get_template('createvolume.html')
    c = RequestContext(request, {'username':username,'form':form, 'message':message})
    return HttpResponse(t.render(c))
