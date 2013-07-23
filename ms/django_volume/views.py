from django.http import HttpResponse, HttpResponseRedirect
from django.template import Context, loader, RequestContext
from django.shortcuts import redirect

from django_lib.auth import authenticate, verifyownership
import django_volume.forms as forms
import django_lib.forms as libforms
from django.forms.formsets import formset_factory

import logging

from storage.storagetypes import transactional, clock_gettime, get_time
import storage.storage as db
from MS.volume import Volume
from MS.user import SyndicateUser as User
from MS.entry import MSENTRY_TYPE_DIR

@authenticate
def myvolumes(request):
    session = request.session
    username = session['login_email']
    user = db.read_user(username)

    attrs = {}
    if user.volumes_o:
        attrs['Volume.volume_id IN'] = user.volumes_o
        myvols = db.list_volumes(attrs)
    else:
        myvols = []
    all_users = []

    for v in myvols:
        uattrs = {}
        users_set = []
        uattrs['SyndicateUser.volumes_rw =='] = v.volume_id 
        q = db.list_users(uattrs)
        for u in q:
            users_set.append(u)
        uattrs = {}
        uattrs['SyndicateUser.volumes_r =='] = v.volume_id 
        q = db.list_users(uattrs)
        for u in q:
            users_set.append(u)
        all_users.append(users_set)

    vols_users = zip(myvols, all_users)
    t = loader.get_template('myvolumes.html')
    c = Context({'username':username, 'vols':vols_users})
    return HttpResponse(t.render(c))

@authenticate
def allvolumes(request):
    session = request.session
    username = session['login_email']
    v_attrs = {'Volume.private !=': True}
    volumes = db.list_volumes(v_attrs)
    owners = []
    for v in volumes:
        attrs = {"SyndicateUser.owner_id ==": v.owner_id}
        owners.append(db.get_user(attrs))
    vols = zip(volumes, owners)
    t = loader.get_template('allvolumes.html')
    c = Context({'username':username, 'vols':vols})
    return HttpResponse(t.render(c))

#@verifyownership
@authenticate
def addpermissions(request, volume_id):
    session = request.session
    username = session['login_email']
    vol = db.read_volume( volume_id )

    if request.method != "POST":
        return HttpResponseRedirect('syn/volume/' + str(vol.volume_id) + '/permissions')
    else:

        addform = forms.AddPermissions(request.POST)
        passwordform = libforms.Password(request.POST)

        afv = False # addform is valid?
        psv = False # passwordform is valid?

        if addform.is_valid():
            afv = True
        if passwordform.is_valid():
            psv = True

        # Password required, send them back.
        if not psv:
            return volumepermissions(request, vol.volume_id, message="Password required", initial_data=session['initial_data'])
        else:
            # Check password hash
            if vol.volume_secret_salted_hash != Volume.generate_password_hash(passwordform.cleaned_data['password'], vol.volume_secret_salt):
                message = "Incorrect password"
                return volumepermissions(request, vol.volume_id, message=message, initial_data=session['initial_data']) 
        
        # Adduser fulfillment
        if afv:
            new_username = addform.cleaned_data['user']
            read = addform.cleaned_data['read']
            write = addform.cleaned_data['write']

            for data in session['initial_data']:
                if data['user'] == new_username:
                    message = "User already has rights for volume."
                    return volumepermissions(request, vol.volume_id, message=message, initial_data=session['initial_data'])
            
            new_user = db.read_user(new_username)
            if not new_user:
                message = "No Syndicate user with the email {} exists.".format(new_username)
                return volumepermissions(request, vol.volume_id, message=message, initial_data=session['initial_data'])
            if write:
                if read:
                    new_volumes_rw = new_user.volumes_rw + [vol.volume_id]
                    fields = {'volumes_rw':new_volumes_rw}
                    db.update_user(new_username, **fields)
                else:
                    message = "Write permissions require read permissions as well."
                    return volumepermissions(request, vol.volume_id, message=message, initial_data=session['initial_data'])
            elif read:
                new_volumes_r = new_user.volumes_r + [vol.volume_id]
                fields = {'volumes_r':new_volumes_r}
                db.update_user(new_username, **fields)
        else:
            message = "Incorrect entry fields: likely invalid email address."
            return volumepermissions(request,vol.volume_id, message=message, initial_data=session['initial_data'])

        session['new_change'] = "We've saved a new user to your volume."
        session['next_url'] = '/syn/volume/' + str(vol.volume_id) + '/permissions'
        session['next_message'] = "Click here to see your volumes permissions."
        return HttpResponseRedirect('/syn/thanks')    


# Since this method is transactional, all queries must be ancestral. Since currently,
# we don't seem to be doing ancestral keys, we can skip it by passing initial data back.
# This also saves calculation etc. I think I'll add it to all calls of volumepermissions().                    
@transactional(xg=True)
#@verifyownership
@authenticate
def changepermissions(request, volume_id):
    session = request.session
    username = session['login_email']
    vol = db.read_volume( volume_id )
    PermissionFormSet = formset_factory(forms.Permissions, extra=0)
    
    if request.method != "POST":
        return HttpResponseRedirect('syn/volume/' + str(vol.volume_id) + '/permissions')
    else:

        fsv = False # Formset is valid?
        psv = False # passwordform is valid?

        passwordform = libforms.Password(request.POST)
        formset = PermissionFormSet(request.POST)
        
        if formset.is_valid():
            fsv = True
        if passwordform.is_valid():
            psv = True

        # Password required, send them back.
        if not psv:
            return volumepermissions(request, vol.volume_id, message="Password required", initial_data=session['initial_data'])
        else:
            # Check password hash
            if vol.volume_secret_salted_hash != Volume.generate_password_hash(passwordform.cleaned_data['password'], vol.volume_secret_salt):
                message = "Incorrect password"
                return volumepermissions(request, vol.volume_id, message=message, initial_data=session['initial_data'])

        # Formset fulfillment
        if fsv:
            initial_and_forms = zip(session['initial_data'], formset.forms)
            for data, form in initial_and_forms:

                check_username = data['user']
                check_read = form.cleaned_data['read']
                check_write = form.cleaned_data['write']
                check_user = db.read_user(check_username)

                if check_write and not check_read:
                    message = "Write permissions require read permissions as well."
                    return volumepermissions(request, vol.volume_id, message=message, initial_data=session['initial_data'])

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
            return volumepermissions(request, vol.volume_id, message="Invalid field entries.", initial_data=session['initial_data'])

        session['new_change'] = "We've saved your new permissions."
        session['next_url'] = '/syn/volume/' + str(vol.volume_id) + '/permissions'
        session['next_message'] = "Click here to see your volumes permissions."
        return HttpResponseRedirect('/syn/thanks') 

#@verifyownership
@authenticate
def volumepermissions(request, volume_id, message="", initial_data=None):
    session = request.session
    username = session['login_email']
    vol = db.read_volume( volume_id )
    #user = db.read_user(username)

    if not initial_data:
        rw_attrs = {'SyndicateUser.volumes_rw ==': vol.volume_id}
        rw = db.list_users(rw_attrs)
        r_attrs = {'SyndicateUser.volumes_r ==': vol.volume_id}
        r = db.list_users(r_attrs)
    
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
    passwordform = libforms.Password
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
def viewvolume(request, volume_id):

    session = request.session
    username = session['login_email']
    user = db.read_user(username)
    vol = db.read_volume( volume_id )

    if not vol:
        t = loader.get_template('viewvolume_failure.html')
        c = RequestContext(request, {'username':username} 
                           )    
        return HttpResponse(t.render(c))

    # Ensure right to read
    
    # if (vol.volume_id not in user.volumes_r) and (vol.volume_id not in user.volumes_rw):
    #     t = loader.get_template('viewvolume_failure.html')
    #     c = Context({'username':username})
    #     return HttpResponse(t.render(c))

    rgs = db.list_replica_gateways_by_volume(vol.volume_id)
    ags = db.list_acquisition_gateways_by_volume(vol.volume_id)

    logging.info(ags)
    logging.info(rgs)

    t = loader.get_template('viewvolume.html')
    c = RequestContext(request, {'username':username,
                                 'volume':vol,
                                 'ags':ags,
                                 'rgs':rgs,
                                 } 
                       )
    return HttpResponse(t.render(c))

#@verifyownership
@authenticate
def activatevolume(request, volume_id):

    session = request.session
    username = session['login_email']
    vol = db.read_volume( volume_id )

    if request.method == "POST":

        form = libforms.Password(request.POST)
        if not form.is_valid():
            message = "Password required."
            return volumesettings(request, vol.volume_id, message=message)
        # Check password hash
        hash_check = Volume.generate_password_hash(form.cleaned_data['password'], vol.volume_secret_salt)
        if hash_check != vol.volume_secret_salted_hash:
            message = "Incorrect password."
            return volumesettings(request, vol.volume_id, message=message)

        fields = {'active':True}
        db.update_volume(vol.volume_id, **fields)
        session['new_change'] = "We've activated your volume."
        session['next_url'] = '/syn/volume/' + str(vol.volume_id)
        session['next_message'] = "Click here to go back to your volume."
        return HttpResponseRedirect('/syn/thanks') 
    return HttpResponseRedirect('/syn/volume/' + str(vol.volume_id))

#@verifyownership
@authenticate
def deactivatevolume(request, volume_id):
    session = request.session
    username = session['login_email']
    vol = db.read_volume( volume_id )

    if request.method == "POST":

        form = libforms.Password(request.POST)
        if not form.is_valid():
            message = "Password required."
            return volumesettings(request, vol.volume_id, message=message)
        # Check password hash
        hash_check = Volume.generate_password_hash(form.cleaned_data['password'], vol.volume_secret_salt)
        if hash_check != vol.volume_secret_salted_hash:
            message = "Incorrect password."
            return volumesettings(request, vol.volume_id, message=message)

        fields = {'active':False}
        db.update_volume(vol.volume_id, **fields)
        session['new_change'] = "We've deactivated your volume."
        session['next_url'] = '/syn/volume/' + str(vol.volume_id)
        session['next_message'] = "Click here to go back to your volume."
        return HttpResponseRedirect('/syn/thanks')
    return HttpResponseRedirect("/syn/volume/" + str(vol.volume_id))

#@verifyownership
@authenticate
def deletevolume(request, volume_id):

    @transactional(xg=True)
    def multi_update(vol, usernames, usergateways, acquisitiongateways, replicagateways):
        v_id = vol.volume_id
        db.delete_volume(v_id)

        for user in users:
            fields = {}

            if v_id in user.volumes_o:
                new_volumes_o = user.volumes_o[:]
                new_volumes_o.remove(v_id)
                fields['volumes_o'] = new_volumes_o

            if v_id in user.volumes_rw:
                new_volumes_rw = user.volumes_rw[:]
                new_volumes_rw.remove(v_id)
                fields['volumes_rw'] = new_volumes_rw

            if v_id in user.volumes_r:
                new_volumes_r = user.volumes_r[:]
                new_volumes_r.remove(v_id)
                fields['volumes_r'] = new_volumes_r

            if fields:
                db.update_user(user.email, **fields)


        for ug in usergateways:
            fields = {}
            fields['volume_id'] = 0
            db.update_user_gateway(ug.ms_username, **fields)

        for ag in acquisitiongateways:
            logging.info(ag)
            fields = {}
            new_ids = ag.volume_ids[:].remove(v_id)
            if not new_ids:
                fields['volume_ids'] = []
            else:
                fields['volume_ids'] = new_ids
            db.update_acquisition_gateway(ag.ms_username, **fields)

        for rg in replicagateways:
            fields = {}
            new_ids = rg.volume_ids[:].remove(v_id)
            if not new_ids:
                fields['volume_ids'] = []
            else:
                fields['volume_ids'] = new_ids
            db.update_replica_gateway(rg.ms_username, **fields)

    session = request.session
    message = session.pop('message', "")
    username = session['login_email']
    vol = db.read_volume( volume_id )

    if request.method == "POST":

        form = forms.DeleteVolume(request.POST)
        if form.is_valid():
            # Check password hash
            hash_check = Volume.generate_password_hash(form.cleaned_data['password'], vol.volume_secret_salt)
            if hash_check == vol.volume_secret_salted_hash:
                # Ok to delete
                attrs = {}
                users = db.list_users({'SyndicateUser.volumes_rw ==':vol.volume_id})
                ags = db.list_acquisition_gateways_by_volume(vol.volume_id)
                rgs = db.list_replica_gateways_by_volume(vol.volume_id)
                ugs = db.list_user_gateways_by_volume(vol.volume_id)
                try:
                    multi_update(vol, users, ugs, ags, rgs)
                except Exception as e:
                     logging.error("Unable to delete volume %s" % e)
                     session['message'] = "Unable to delete volume."
                     return redirect('django_volume.views.deletevolume', volume_id=vol.volume_id)
                session['new_change'] = "We've deleted your volume."
                session['next_url'] = '/syn/volume/myvolumes/'
                session['next_message'] = "Click here to go back to your volumes."
                return HttpResponseRedirect('/syn/thanks')
            else:
                session['message'] = "Invalid password"
                return redirect('django_volume.views.deletevolume', volume_id=vol.volume_id)
    else:
        form = forms.DeleteVolume()
        t = loader.get_template('deletevolume.html')
        c = RequestContext(request, {'username':username, 'form':form, 'message':message,'volume':vol} )
        return HttpResponse(t.render(c))    


#@verifyownership
@authenticate
def changegateways_ag(request, volume_id):
    session = request.session
    username = session['login_email']
    vol = db.read_volume( volume_id )

    @transactional(xg=True)
    def update(v_id, gnames, vfields, gfields):
        db.update_volume(v_id, **vfields)
        for g, gfield in zip(gnames, gfields):
            db.update_acquisition_gateway(g, **gfield)

    if request.POST:
        passwordform = libforms.Password(request.POST)
        if not passwordform.is_valid():
            message = "Password required."
            return volumesettings(request, vol.volume_id, message, initial_data_rg=session['initial_rg'], initial_data_ag=session['initial_ag'])
        else:
            hash_check = Volume.generate_password_hash(passwordform.cleaned_data['password'], vol.volume_secret_salt)
            if hash_check != vol.volume_secret_salted_hash:
                message = "Incorrect password."
                return volumesettings(request, vol.volume_id, message, initial_data_rg=session['initial_rg'], initial_data_ag=session['initial_ag'])

        GatewayFormset = formset_factory(forms.Gateway, extra=0)
        formset = GatewayFormset(request.POST)
        formset.is_valid()
        remove_gateways = []
        remove_gateway_names = []
        remove_gateway_ids = []

        for data, form in zip(session['initial_ag'], formset.forms):
            if form.cleaned_data['remove']:
                g = db.read_acquisition_gateway(data['g_name'])
                remove_gateways.append(g)
                remove_gateway_names.append(data['g_name'])
                remove_gateway_ids.append(g.ag_id)
        if not remove_gateways:
            return redirect('django_volume.views.volumesettings', volume_id=vol.volume_id)
    
        new_ags = list(set(vol.ag_ids) - set(remove_gateway_ids))
        vfields = {'ag_ids':new_ags}

        gfields = []
        for g in remove_gateways:
            new_vol_ids = g.volume_ids[:]
            new_vol_ids.remove(vol.volume_id)
            gfields.append({"volume_ids":new_vol_ids})
        try:
            update(vol.volume_id, remove_gateway_names, vfields, gfields)
        except Exception as e:
            message = "Unable to update volume or AG's."
            return volumesettings(request, vol.volume_id, message, initial_data_rg=session['initial_rg'], initial_data_ag=session['initial_ag'])
        session['new_change'] = "We've updated your volume."
        session['next_url'] = '/syn/volume/' + str(vol.volume_id)+ '/settings'
        session['next_message'] = "Click here to go back to your volume."
        return HttpResponseRedirect('/syn/thanks')

    else:
        return redirect('django_volume.views.volumesettings', volume_id=vol.volume_id)


#@verifyownership
@authenticate
def changegateways_rg(request, volume_id):
    session = request.session
    username = session['login_email']
    vol = db.read_volume( volume_id )

    @transactional(xg=True)
    def update(v_id, gnames, vfields, gfields):
        db.update_volume(v_id, **vfields)
        for g, gfield in zip(gnames, gfields):
            db.update_replica_gateway(g, **gfield)

    if request.POST:
        passwordform = libforms.Password(request.POST)
        if not passwordform.is_valid():
            message = "Password required."
            return volumesettings(request, vol.volume_id, message, initial_data_rg=session['initial_rg'], initial_data_ag=session['initial_ag'])
        else:
            hash_check = Volume.generate_password_hash(passwordform.cleaned_data['password'], vol.volume_secret_salt)
            if hash_check != vol.volume_secret_salted_hash:
                message = "Incorrect password."
                return volumesettings(request, vol.volume_id, message, initial_data_rg=session['initial_rg'], initial_data_ag=session['initial_ag'])

        GatewayFormset = formset_factory(forms.Gateway, extra=0)
        formset = GatewayFormset(request.POST)
        formset.is_valid()
        remove_gateways = []
        remove_gateway_names = []
        remove_gateway_ids = []

        for data, form in zip(session['initial_rg'], formset.forms):
            if form.cleaned_data['remove']:
                g = db.read_replica_gateway(data['g_name'])
                remove_gateways.append(g)
                remove_gateway_names.append(data['g_name'])
                remove_gateway_ids.append(g.rg_id)
        if not remove_gateways:
            return redirect('django_volume.views.volumesettings', volume_id=vol.volume_id)
    
        new_rgs = list(set(vol.rg_ids) - set(remove_gateway_ids))
        vfields = {'rg_ids':new_rgs}

        gfields = []
        for g in remove_gateways:
            new_vol_ids = g.volume_ids[:]
            new_vol_ids.remove(vol.volume_id)
            gfields.append({"volume_ids":new_vol_ids})
        try:
            update(vol.volume_id, remove_gateway_names, vfields, gfields)
        except Exception as e:
            message = "Unable to update volume or RG's."
            return volumesettings(request, vol.volume_id, message, initial_data_rg=session['initial_rg'], initial_data_ag=session['initial_ag'])
        session['new_change'] = "We've updated your volume."
        session['next_url'] = '/syn/volume/' + str(vol.volume_id) + '/settings'
        session['next_message'] = "Click here to go back to your volume."
        return HttpResponseRedirect('/syn/thanks')

    else:
        return redirect('django_volume.views.volumesettings', volume_id=vol.volume_id)

#@verifyownership
@authenticate
def volumesettings(request, volume_id, message="", old_data=None, initial_data_rg=None, initial_data_ag=None):
    '''
    old_data is for keeping state while changing the description when the password is wrong,
    no password is entered, etc.

    initial_data is for keeping state of the gateways when mistakes are made etc.
    '''

    session = request.session
    username = session['login_email']
    vol = db.read_volume( volume_id )

    if not initial_data_ag:
        initial_data_ag = []
        ags = db.list_acquisition_gateways_by_volume(vol.volume_id)
        for g in ags:
            initial_data_ag.append({'g_name':g.ms_username, 'remove':False})
        session['initial_ag'] = initial_data_ag
    
    if not initial_data_rg:
        initial_data_rg = []
        rgs = db.list_replica_gateways_by_volume(vol.volume_id)
        for g in rgs:
            initial_data_rg.append({'g_name':g.ms_username, 'remove':False})
        session['initial_rg'] = initial_data_rg

    if old_data:
        desc_form = forms.ChangeVolumeD(initial={'description': old_data['desc']})
    else:
        desc_form = forms.ChangeVolumeD(initial={'description':vol.description})
    pass_form = libforms.ChangePassword()
    password = libforms.Password()

    GatewayFormset = formset_factory(forms.Gateway, extra=0)
    if initial_data_rg:
        rg_form = GatewayFormset(initial=initial_data_rg)
    else:
        rg_form = None
    if initial_data_ag:
        ag_form = GatewayFormset(initial=initial_data_ag)
    else:
        ag_form = None

    t = loader.get_template('volumesettings.html')
    c = RequestContext(request, {'username':username,
                                 'volume': vol,
                                 'desc_form':desc_form,
                                 'pass_form':pass_form,
                                 'password':password,
                                 'message':message,
                                 'ag_form':ag_form,
                                 'rg_form':rg_form,
                                 } )
    return HttpResponse(t.render(c))

#@verifyownership
@authenticate
def volumeprivacy(request, volume_id):
    session = request.session
    username = session['login_email']
    vol = db.read_volume( volume_id )

    if request.POST:

        form = libforms.Password(request.POST)
        if not form.is_valid():
            message = "Password required."
            return volumesettings(request, vol.volume_id, message=message)
        # Check password hash
        hash_check = Volume.generate_password_hash(form.cleaned_data['password'], vol.volume_secret_salt)
        if hash_check != vol.volume_secret_salted_hash:
            message = "Incorrect password."
            return volumesettings(request, vol.volume_id, message=message)

        if vol.private:
            fields = {"private":False}
            db.update_volume(vol.volume_id, **fields)
            session['new_change'] = "We've publicized your volume."
            session['next_url'] = '/syn/volume/' + str(vol.volume_id)
            session['next_message'] = "Click here to go back to your volume."
            return HttpResponseRedirect('/syn/thanks')
        else:
            fields = {"private":True}
            db.update_volume(vol.volume_id, **fields)
            session['new_change'] = "We've privatized your volume."
            session['next_url'] = '/syn/volume/' + str(vol.volume_id)
            session['next_message'] = "Click here to go back to your volume."
            return HttpResponseRedirect('/syn/thanks')

    else:
        return HttpResponseRedirect("/syn/volume/" + str(vol.volume_id))

#@verifyownership
@authenticate
def changevolume(request, volume_id):
    session = request.session
    username = session['login_email']
    vol = db.read_volume( volume_id )

    if request.method != "POST":
        return HttpResponseRedirect('/syn/volume/' + str(vol.volume_id) + '/settings')


    form = libforms.Password(request.POST)
    desc_form = forms.ChangeVolumeD(request.POST)
    old_data = {}

    if not form.is_valid():
        message = "Password required."
        desc_form.is_valid()
        if desc_form.errors:
            old_data['desc'] = ""
        else:
            old_data['desc'] = desc_form.cleaned_data['description']
        return volumesettings(request, vol.volume_id, message=message, old_data=old_data)
    # Check password hash
    hash_check = Volume.generate_password_hash(form.cleaned_data['password'], vol.volume_secret_salt)
    if hash_check != vol.volume_secret_salted_hash:
        message = "Incorrect password."
        desc_form.is_valid()
        if desc_form.errors:
            old_data['desc'] = ""
        else:
            old_data['desc'] = desc_form.cleaned_data['description']
        return volumesettings(request, vol.volume_id, message=message, old_data=old_data)

    if not desc_form.is_valid():
        message = "Invalid description field entries."
        return volumesettings(request, vol.volume_id, message=message)

    kwargs = {}
    if desc_form.cleaned_data['description']:
        kwargs['description'] = desc_form.cleaned_data['description']
    db.update_volume(vol.volume_id, **kwargs)
    session['new_change'] = "We've changed your volume description."
    session['next_url'] = '/syn/volume/' + str(vol.volume_id)
    session['next_message'] = "Click here to go back to your volume."
    return HttpResponseRedirect('/syn/thanks')

#@verifyownership
@authenticate
def changevolumepassword(request, volume_id):
    session = request.session
    username = session['login_email']
    vol = db.read_volume( volume_id )

    if request.method != "POST":
        return HttpResponseRedirect('/syn/volume/' + str(vol.volume_id) + '/settings')

    form = libforms.ChangePassword(request.POST)
    if not form.is_valid():
        message = "You must fill out all password fields."
        return volumesettings(request, vol.volume_id, message=message)
    else:

        # Check password hash
        hash_check = Volume.generate_password_hash(form.cleaned_data['oldpassword'], vol.volume_secret_salt)
        if hash_check != vol.volume_secret_salted_hash:
            message = "Incorrect password."
            return volumesettings(request, vol.volume_id, message=message)
        elif form.cleaned_data['newpassword_1'] != form.cleaned_data['newpassword_2']:
            message = "Your new passwords did not match each other."
            return volumesettings(request, vol.volume_id, message=message)

        # Ok change password 
        kwargs = {}
        new_volume_secret_salt, new_volume_secret_salted_hash = Volume.generate_volume_secret(form.cleaned_data['newpassword_1'])
        kwargs['volume_secret_salted_hash'] = new_volume_secret_salted_hash
        kwargs['volume_secret_salt'] = new_volume_secret_salt
        db.update_volume(vol.volume_id, **kwargs)

        session['new_change'] = "We've changed your volume's password."
        session['next_url'] = '/syn/volume/' + str(vol.volume_id)
        session['next_message'] = "Click here to go back to your volume."
        return HttpResponseRedirect('/syn/thanks')


@authenticate
def createvolume(request):
    session = request.session
    username = session['login_email']

    message = ""

    if request.method == "POST":

        # Validate input forms
        form = forms.CreateVolume(request.POST)
        if form.is_valid():

            # attempt to create the volume
            # CREATE VOLUME
            kwargs = {}
            kwargs['name'] = form.cleaned_data['name']
            kwargs['blocksize'] = int(form.cleaned_data['blocksize'])
            kwargs['description'] = form.cleaned_data['description']
            kwargs['volume_secret'] = form.cleaned_data['password']
            kwargs['private'] = form.cleaned_data['private']

            try:
               volume_key = db.create_volume(username, **kwargs)
               volume = volume_key.get()
            except Exception, e:
               logging.exception( e )
               message = "Unable to create Volume '{}': {}".format( form.cleaned_data['name'], e.message )
               form = forms.CreateVolume()
               t = loader.get_template('createvolume.html')
               c = RequestContext(request, {'username':username,'form':form, 'message':message})
               return HttpResponse(t.render(c))

            now_sec, now_nsec = clock_gettime()

            rc = db.make_root(volume,
                              ftype=MSENTRY_TYPE_DIR,
                              fs_path="/",
                              url="http://syndicate-metadata.appspot.com",
                              version=1,
                              ctime_sec=now_sec,
                              ctime_nsec=now_nsec,
                              mtime_sec=now_sec,
                              mtime_nsec=now_nsec,
                              owner_id=volume.owner_id,
                              acting_owner_id=volume.owner_id,
                              volume_id=volume.volume_id,
                              mode=0755,
                              size=4096,
                              max_read_freshness=5000,
                              max_write_freshness=0
                            )

            session['new_change'] = "Your new volume is ready."
            session['next_url'] = '/syn/volume/myvolumes/'
            session['next_message'] = "Click here to see your volumes."
            return HttpResponseRedirect('/syn/thanks/')

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
            form = forms.CreateVolume(initial={'name': oldname,
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
