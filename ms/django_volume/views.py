from django.http import HttpResponse, HttpResponseRedirect
from django.template import Context, loader, RequestContext
from django.forms.formsets import formset_factory

from django_lib.auth import authenticate
import django_volume.forms as forms

import logging

import storage.storage as db
from MS.volume import Volume
from MS.user import SyndicateUser as User
from MS.gateway import UserGateway as UG,  ReplicaGateway as RG, AcquisitionGateway as AG

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

        afv = False # addform is valid?
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

            for data in session['initial_data']:
                if data['user'] == new_username:
                    message = "User already has rights for volume."
                    return volumepermissions(request, volume_name, message)
            
            new_user = db.read_user(new_username)
            if not new_user:
                message = "No Syndicate user with the email {} exists.".format(new_username)
                return volumepermissions(request, volume_name, message)
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
        else:
            message = "Incorrect entry fields: likely invalid email address."
            return volumepermissions(request,volume_name, message)

        return HttpResponseRedirect('/syn/volume/' + volume_name + '/permissions')    
                    


@authenticate
def changepermissions(request, volume_name):
    session = request.session
    username = session['login_email']
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
            intial_and_forms = zip(session['initial_data'], formset.forms)
            for data, form in initial_and_forms:

                check_username = data['user']
                check_read = form.cleaned_data['read']
                check_write = form.cleaned_data['write']
                check_user = db.read_user(check_username)

                if check_write and not check_read:
                    message = "Write permissions require read permissions as well."
                    return volumepermissions(request, volume_name, message)

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
def viewvolume(request, volume_name):
    session = request.session
    username = session['login_email']
    user = db.read_user(username)

    # Ensure ownership
    
    if user.owner_id != db.read_volume(volume_name).owner_id:
        t = loader.get_template('viewvolume_failure.html')
        c = Context({'username':username})
        return HttpResponse(t.render(c))
   
    # The reason why query() is used here instead of db.read_volume() is
    # because it is guaranteed to skip the cache, which is what we want in
    # this case. (active issues with stale cache).
    vol = Volume.query(Volume.name == volume_name)
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
def deletevolume(request, volume_name):

    session = request.session
    username = session['login_email']

    message = ""
    vol = db.read_volume(volume_name)
    if request.method == "POST":
        form = forms.DeleteVolume(request.POST)
        if form.is_valid():
            # Check password hash
            hash_check = Volume.generate_password_hash(form.cleaned_data['password'], vol.volume_secret_salt)
            if hash_check == vol.volume_secret_salted_hash:
                # Ok to delete
                db.delete_volume(volume_name)
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