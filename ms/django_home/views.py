from django.http import HttpResponse, HttpResponseRedirect
from django.template import Context, loader, RequestContext
from django.forms.formsets import formset_factory

from django_lib.auth import authenticate
import django_home.forms as forms

import logging

import storage.storage as db
from MS.volume import Volume
from MS.user import SyndicateUser as User
from MS.gateway import UserGateway as UG,  ReplicaGateway as RG, AcquisitionGateway as AG


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
    user = db.read_user(username)
    myvols = []
    for v_id in user.volumes_o:
        q = Volume.query(Volume.volume_id == v_id)
        for v in q: # should be one
            myvols.append(v)
    t = loader.get_template('myvolumes.html')
    c = Context({'username':username, 'vols':myvols})
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
        
#            user_volumes = user.volumes
#            if user_volumes is None:
#                user.volumes = [volume_key.get().volume_id]
#            else:
#                user.volumes.append(volume_key.get().volume_id)

            # Update user volume fields (o and rw)
            new_volumes_o = user.volumes_o
            new_volumes_rw = user.volumes_rw

            v_id = volume_key.get().volume_id

            new_volumes_o.append(v_id)
            new_volumes_rw.append(v_id)

            fields = {'volumes_o':new_volumes_o, 'volumes_rw':new_volumes_rw}
            db.update_user(username, **fields)

            return HttpResponseRedirect('/syn/myvolumes/')

        else:

            # Prep returned form values (so they don't have to re-enter stuff)

            if 'name' in form.errors:
                oldname = ""
            else:
                oldname = request.POST['name']
            if 'blocksize' in form.errors:
                oldblocksize = ""
            else:
                oldblocksize = request.POST['blocksize']
            if 'description' in form.errors:
                olddescription = ""
            else:
                olddescription = request.POST['description']

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
