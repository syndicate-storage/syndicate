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
def addpermissions(request, volume_name):
    session = request.session
    username = session['login_email']
    vol = db.read_volume(volume_name)

    if request.method != "POST":
        return HttpResponseRedirect('syn/volume/' + volume_name + '/permissions')
    else:

        addform = forms.AddPermissions(request.POST)
        passwordform = forms.Password(request.POST)

        adv = False # addform is valid?
        psv = False # passwordform is valid?

        if addform.is_valid():
            afv = True
        if passwordform.is_valid():
            psv = True

        # Password required, send them back.
        if not psv:
            return volumepermissions(request, volume_name, message="Password required")
        else:
            # Check password hash
            if vol.volume_secret_salted_hash != Volume.generate_password_hash(passwordform.cleaned_data['password'], vol.volume_secret_salt):
                message = "Incorrect password"
                return volumepermissions(request, volume_name, message) 
        
        # Adduser fulfillment
        if afv:
            new_username = addform.cleaned_data['user']
            read = addform.cleaned_data['read']
            write = addform.cleaned_data['write']
            
            new_user = db.read_user(new_username)
            if write:
                if read:
                    new_volumes_rw = new_user.volumes_rw + [vol.volume_id]
                    fields = {'volumes_rw':new_volumes_rw}
                    db.update_user(new_username, **fields)
                else:
                    message = "Write permissions require read permissions as well."
                    return volumepermissions(request, volume_name, message)
            elif read:
                new_volumes_r = new_user.volumes_r + [vol.volume_id]
                fields = {'volumes_r':new_volumes_r}
                db.update_user(new_username, **fields)

        return HttpResponseRedirect('/syn/volume/' + volume_name + '/permissions')    
                    


@authenticate
def changepermissions(request, volume_name):
    session = request.session
    username = session['login_email']
#    user = db.read_user(username)
    vol = db.read_volume(volume_name)

    PermissionFormSet = formset_factory(forms.Permissions, extra=0)
    
    if request.method != "POST":
        return HttpResponseRedirect('syn/volume/' + volume_name + '/permissions')
    else:

        fsv = False # Formset is valid?
        psv = False # passwordform is valid?

        passwordform = forms.Password(request.POST)
        formset = PermissionFormSet(request.POST)
        
        if formset.is_valid():
            fsv = True
        if passwordform.is_valid():
            psv = True

        # Password required, send them back.
        if not psv:
            return volumepermissions(request, volume_name, message="Password required")
        else:
            # Check password hash
            if vol.volume_secret_salted_hash != Volume.generate_password_hash(passwordform.cleaned_data['password'], vol.volume_secret_salt):
                message = "Incorrect password"
                return volumepermissions(request, volume_name, message)

        # Formset fulfillment
        if fsv:
            user_and_forms = zip(session['initial_data'], formset.forms)
            for data, form in user_and_forms:
                check_username = data['user']
                check_read = form.cleaned_data['read']
                check_write = form.cleaned_data['write']
                check_user = db.read_user(check_username)
                print check_username, check_write, check_read, check_user
                if check_write and not check_read:
                    message = "Write permissions require read permissions as well."
                    return volumepermissions(request, volume_name, message)
                # this can be simplified by using data['read'] and data['write']
                if data['write']:
                    if check_write:
                        continue
                    elif check_read:
                        # Give read, take away write
                        new_volumes_r = check_user.volumes_r + [vol.volume_id]
                        new_volumes_rw = check_user.volumes_rw.remove(vol.volume_id)
                        if not new_volumes_rw:
                            new_volumes_rw = []
                        fields = {'volumes_r':new_volumes_r, 'volumes_rw':new_volumes_rw}
                        db.update_user(check_username, **fields)
                    else:
                        # change to no permissions
                        new_volumes_rw = check_user.volumes_rw.remove(vol.volume_id)
                        if not new_volumes_rw:
                            new_volumes_rw = []
                        fields = {'volumes_rw':new_volumes_rw}
                        db.update_user(check_username, **fields)
                if data['read']:
                    if check_write:

                        # Give write, take away read
                        new_volumes_r = check_user.volumes_r.remove(vol.volume_id)
                        new_volumes_rw = check_user.volumes_rw + [vol.volume_id]
                        if not new_volumes_r:
                            new_volumes_r = []
                        fields = {'volumes_r':new_volumes_r, 'volumes_rw':new_volumes_rw}
                        db.update_user(check_username, **fields)

                    elif check_read:
                        continue
                    else:
                        # change to no permissions
                        new_volumes_r = check_user.volumes_r.remove(vol.volume_id)
                        if not new_volumes_r:
                            new_volumes_r = []
                        fields = {'volumes_r':new_volumes_r}
                        db.update_user(check_username, **fields)

                # will this code ever get executed? Implies both data are false which shouldn't happen        
                '''else:
                    if check_write:
                        # give write
                        new_volumes_rw = check_user.volumes_rw + [vol.volume_id]
                        fields = {'volumes_rw':new_volumes_rw}
                        db.update_user(check_username, **fields)
                    elif check_read:
                        # give read
                        new_volumes_r = check_user.volumes_r + [vol.volume_id]
                        fields = {'volumes_rs':new_volumes_r}
                        db.update_user(check_username, **fields)'''
        else:
            return volumepermissions(request, volume_name, message="Invalid field entries.")

        return HttpResponseRedirect('/syn/volume/' + volume_name + '/permissions')    


@authenticate
def volumepermissions(request, volume_name, message=""):
    session = request.session
    username = session['login_email']
#    user = db.read_user(username)
    vol = db.read_volume(volume_name)

    users = User.query()
    rw = User.query(User.volumes_rw==vol.volume_id)
    r = User.query(User.volumes_r==vol.volume_id)

    initial_data = []
    for u in rw:
        if u.email == username:
            continue;
        initial_data.append( {'user':u.email,
                              'read':True,
                              'write':True} )
    for u in r:
        initial_data.append( {'user':u.email,
                              'read':True,
                              'write':False} )

    session['initial_data'] = initial_data
    PermissionFormSet = formset_factory(forms.Permissions, extra=0)
    addform = forms.AddPermissions
    passwordform = forms.Password
    if initial_data:
        formset = PermissionFormSet(initial=initial_data)
    else:
        formset = None


    t = loader.get_template('volumepermissions.html')
    c = RequestContext(request,
                       {'username':username,
                        'volume':vol,
                        # 'users':users,
                        # 'rw':rw,
                        # 'r':r,
                        'addform':addform,
                        'passwordform':passwordform,
                        'formset':formset,
                        'message':message} )
    return HttpResponse(t.render(c))

@authenticate
def viewvolume(request, volume):
    session = request.session
    username = session['login_email']
    user = db.read_user(username)

    # Ensure ownership
    
    if user.owner_id != db.read_volume(volume).owner_id:
        t = loader.get_template('viewvolume_failure.html')
        c = Context({'username':username})
        return HttpResponse(t.render(c))
   
    # The reason why query() is used here instead of db.read_volume() is
    # because it is guaranteed to skip the cache, which is what we want in
    # this case. (active issues with stale cache).
    vol = Volume.query(Volume.name == volume)
    for v in vol:
        realvol = v
        if v.active:
            active = True
        else:
            active = None
    t = loader.get_template('viewvolume.html')
    c = RequestContext(request, {'username':username,
                                 'volume':realvol,
                                 'active':active,
                                 } 
                       )
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
def activatevolume(request, volume_name):
    session = request.session
    username = session['login_email']

    if request.method == "POST":
        fields = {'active':True}
        db.update_volume(volume_name, **fields)
    return HttpResponseRedirect("/syn/volume/" + volume_name)

@authenticate
def deactivatevolume(request, volume_name):
    session = request.session
    username = session['login_email']

    if request.method == "POST":
        fields = {'active':False}
        db.update_volume(volume_name, **fields)
    return HttpResponseRedirect("/syn/volume/" + volume_name)

@authenticate
def deletevolume(request, volume):

    session = request.session
    username = session['login_email']

    message = ""
    vol = db.read_volume(volume)
    if request.method == "POST":
        form = forms.DeleteVolume(request.POST)
        if form.is_valid():
            # Check password hash
            hash_check = Volume.generate_password_hash(form.cleaned_data['password'], vol.volume_secret_salt)
            if hash_check == vol.volume_secret_salted_hash:
                # Ok to delete
                db.delete_volume(volume)
                return HttpResponseRedirect("/syn/myvolumes")
            else:
                message = "Invalid password"
                form = forms.DeleteVolume()
                t = loader.get_template('deletevolume.html')
                c = RequestContext(request, {'username':username, 'form':form, 'message':message, 'volume':vol} )
                return HttpResponse(t.render(c))
    else:
        form = forms.DeleteVolume()
        t = loader.get_template('deletevolume.html')
        c = RequestContext(request, {'username':username, 'form':form, 'message':message,'volume':vol} )
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
