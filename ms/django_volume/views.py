'''

All of these views are predicated on the user already being logged in to
valid session.

djago_volume/views.py
John Whelchel
Summer 2013

These are the views for the Volume section of the administration
site. They are all decorated with @authenticate to make sure that a user is 
logged in; if not, they are redirected to the login page. They can
be decorated with verifyownership to minimize access to un-owned volumes.

'''
import logging

from django.http import HttpResponse, HttpResponseRedirect
from django.template import Context, loader, RequestContext
from django.shortcuts import redirect
from django.forms.formsets import formset_factory

from django_lib.auth import authenticate, verifyownership, verifyownership_private
import django_lib.forms as libforms
import django_volume.forms as forms

import storage.storage as db
from storage.storagetypes import transactional, clock_gettime, get_time

from MS.volume import Volume
from MS.user import SyndicateUser as User
from MS.entry import MSENTRY_TYPE_DIR
from MS.msconfig import *

@authenticate
def myvolumes(request):
    '''
    View that display all of user's owned volumes
    '''
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
    '''
    Display all public volumes
    '''
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

@verifyownership_private
@authenticate
def addpermissions(request, volume_id):
    '''
    This handler allows adding users to volumes so they can have either read access or read and write access.
    '''
    session = request.session
    username = session['login_email']

    if request.method != "POST":
        return redirect('syn/volume/' + str(vol.volume_id) + '/permissions')
    else:

        vol = db.read_volume( volume_id )
        if not vol:
            return redirect('django_volume.views.viewvolume', volume_id)
        addform = forms.AddPermissions(request.POST)
        passwordform = libforms.Password(request.POST)

        if not passwordform.is_valid():
            session['message'] = "Password required."
            return redirect('django_volume.views.volumepermissions', vol.volume_id)
        else:
            # Check password hash
            if vol.volume_secret_salted_hash != Volume.generate_password_hash(passwordform.cleaned_data['password'], vol.volume_secret_salt):
                session['message'] = "Incorrect password"
                return redirect('django_volume.views.volumepermissions', vol.volume_id)

        if not addform.is_valid():
            session['message'] = "Incorrect entry fields: likely invalid email address."
            return redirect('django_volume.views.volumepermissions', vol.volume_id)

        # Ok to update
        else:
            new_username = addform.cleaned_data['user']
            
            read = addform.cleaned_data['read']
            write = addform.cleaned_data['write']

            for data in session['initial_perms' + str(volume_id)]:
                if data['user'] == new_username:
                    session['message'] = "User already has rights for volume."
                    return redirect('django_volume.views.volumepermissions', vol.volume_id)
            
            new_user = db.read_user(new_username)
            if not new_user:
                session['message'] = "No Syndicate user with the email {} exists.".format(new_username)
                return redirect('django_volume.views.volumepermissions', vol.volume_id)
                            
            if vol.owner_id == new_user.owner_id:
                session['message'] = "You already own this volume."
                return redirect('django_volume.views.volumepermissions', vol.volume_id)
                
            if write:
                if read:
                    new_volumes_rw = new_user.volumes_rw + [vol.volume_id]
                    fields = {'volumes_rw':new_volumes_rw}
                    db.update_user(new_username, **fields)
                else:
                    session['message'] = "Write permissions require read permissions as well."
                    return redirect('django_volume.views.volumepermissions', vol.volume_id)
            elif read:
                new_volumes_r = new_user.volumes_r + [vol.volume_id]
                fields = {'volumes_r':new_volumes_r}
                db.update_user(new_username, **fields)
            # Clear out old permissions data.
            session.pop('initial_perms' + str(volume_id), None)

            session['new_change'] = "We've saved a new user to your volume."
            session['next_url'] = '/syn/volume/' + str(vol.volume_id) + '/permissions'
            session['next_message'] = "Click here to see your volumes permissions."
            return redirect('/syn/thanks')    


# Since this method is transactional, all queries must be ancestral. Since currently,
# we don't seem to be doing ancestral keys, we can skip it by passing initial perms back.
                  
@transactional(xg=True)
@verifyownership_private
@authenticate
def changepermissions(request, volume_id):
    '''
    This view handles modification or removal of rights to the volume for users who 
    already had some rights.
    '''
    session = request.session
    username = session['login_email']
    vol = db.read_volume( volume_id )
    if not vol:
        return redirect('django_volume.views.viewvolume', volume_id)

    PermissionFormSet = formset_factory(forms.Permissions, extra=0)
    
    if request.method != "POST":
        return redirect('syn/volume/' + str(vol.volume_id) + '/permissions')
    else:
        passwordform = libforms.Password(request.POST)
        formset = PermissionFormSet(request.POST)
        
        if not passwordform.is_valid():
            session['message'] = "Password required."
            return redirect('django_volume.views.volumepermissions', vol.volume_id)
        else:
            # Check password hash
            if vol.volume_secret_salted_hash != Volume.generate_password_hash(passwordform.cleaned_data['password'], vol.volume_secret_salt):
                session['message'] = "Incorrect password"
                return redirect('django_volume.views.volumepermissions', vol.volume_id)

        if not formset.is_valid():
            session['message'] = "Invalid field entries."
            return redirect('django_volume.views.volumepermissions', vol.volume_id)
        else:
            initial_and_forms = zip(session['initial_perms' + str(volume_id)], formset.forms)
            for data, form in initial_and_forms:

                check_username = data['user']
                check_read = form.cleaned_data['read']
                check_write = form.cleaned_data['write']
                check_user = db.read_user(check_username)

                if check_write and not check_read:
                    session['message'] = "Write permissions require read permissions as well."
                    return redirect('django_volume.views.volumepermissions', vol.volume_id)

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
                elif data['read']:
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

            # Clear out stale data.
            session.pop("initial_perms" + str(volume_id), None)

            session['new_change'] = "We've saved your new permissions."
            session['next_url'] = '/syn/volume/' + str(vol.volume_id) + '/permissions'
            session['next_message'] = "Click here to see your volumes permissions."
            return redirect('/syn/thanks') 

@verifyownership_private
@authenticate
def volumepermissions(request, volume_id):
    '''
    View for the webpage that shows the current state of user permissions for 
    the volume. Populates initial_perms if not already extant.
    '''
    session = request.session
    username = session['login_email']
    vol = db.read_volume( volume_id )
    if not vol:
        return redirect('django_volume.views.viewvolume', volume_id)    

    message = session.pop('message', "")
    initial_data = session.get('initial_perms' + str(volume_id), None)

    if not initial_data:
        rw_attrs = {'SyndicateUser.volumes_rw ==': vol.volume_id}
        rw = db.list_users(rw_attrs)
        r_attrs = {'SyndicateUser.volumes_r ==': vol.volume_id}
        r = db.list_users(r_attrs)
    
        initial_data = []
        for u in rw:
            if u.owner_id == vol.owner_id:
                continue;
            initial_data.append( {'user':u.email,
                                  'read':True,
                                  'write':True} )
        for u in r:
            initial_data.append( {'user':u.email,
                                  'read':True,
                                  'write':False} )
    
        session['initial_perms' + str(volume_id)] = initial_data
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

@verifyownership_private
@authenticate
def viewvolume(request, volume_id):

    session = request.session
    username = session['login_email']
    user = db.read_user(username)
    vol = db.read_volume( volume_id )

    if not vol:
        return redirect('django_volume.views.failure')

    rgs = db.list_replica_gateways_by_volume(vol.volume_id)
    ags = db.list_acquisition_gateways_by_volume(vol.volume_id)

    t = loader.get_template('viewvolume.html')
    c = RequestContext(request, {'username':username,
                                 'volume':vol,
                                 'ags':ags,
                                 'rgs':rgs,
                                 } 
                       )
    return HttpResponse(t.render(c))

@verifyownership_private
@authenticate
def activatevolume(request, volume_id):

    session = request.session
    username = session['login_email']
    vol = db.read_volume( volume_id )

    if not vol:
        return redirect('django_volume.views.viewvolume', volume_id)

    if request.method == "POST":

        form = libforms.Password(request.POST)
        if not form.is_valid():
            session['message'] = "Password required."
            return redirect('django_volume.views.volumesettings', vol.volume_id)
        # Check password hash
        hash_check = Volume.generate_password_hash(form.cleaned_data['password'], vol.volume_secret_salt)
        if hash_check != vol.volume_secret_salted_hash:
            session['message'] = "Incorrect password."
            return redirect('django_volume.views.volumesettings', vol.volume_id)

        fields = {'active':True}
        db.update_volume(vol.volume_id, **fields)
        session['new_change'] = "We've activated your volume."
        session['next_url'] = '/syn/volume/' + str(vol.volume_id)
        session['next_message'] = "Click here to go back to your volume."
        return redirect('/syn/thanks') 
    return redirect('/syn/volume/' + str(vol.volume_id))

@verifyownership_private
@authenticate
def deactivatevolume(request, volume_id):
    session = request.session
    username = session['login_email']
    vol = db.read_volume( volume_id )
    if not vol:
        return redirect('django_volume.views.viewvolume', volume_id)


    if request.method == "POST":

        form = libforms.Password(request.POST)
        if not form.is_valid():
            session['message'] = "Password required."
            return redirect('django_volume.views.volumesettings', vol.volume_id)
        # Check password hash
        hash_check = Volume.generate_password_hash(form.cleaned_data['password'], vol.volume_secret_salt)
        if hash_check != vol.volume_secret_salted_hash:
            session['message'] = "Incorrect password."
            return redirect('django_volume.views.volumesettings', vol.volume_id)
        fields = {'active':False}
        db.update_volume(vol.volume_id, **fields)
        session['new_change'] = "We've deactivated your volume."
        session['next_url'] = '/syn/volume/' + str(vol.volume_id)
        session['next_message'] = "Click here to go back to your volume."
        return redirect('/syn/thanks')
    return redirect("/syn/volume/" + str(vol.volume_id))

@verifyownership_private
@authenticate
def deletevolume(request, volume_id):
    '''
    View for deleting volumes. Since so many other entites have properties related
    to volume ID's, numerous db updates need to be checked. CQ, they are all grouped
    together into the transactional helper method multi_update().
    '''

    # Clear out volume_id in properties for users, UG's, AG's, and RG's.
    @transactional(xg=True)
    def multi_update(vol, users, usergateways, acquisitiongateways, replicagateways):
        v_id = vol.volume_id
        db.delete_volume(v_id)
        logging.info(users)

        for user in users:
            fields = {}

            if v_id in user.volumes_o:
                new_volumes_o = user.volumes_o
                new_volumes_o.remove(v_id)
                fields['volumes_o'] = new_volumes_o

            if v_id in user.volumes_rw:
                new_volumes_rw = user.volumes_rw
                new_volumes_rw.remove(v_id)
                fields['volumes_rw'] = new_volumes_rw

            if v_id in user.volumes_r:
                new_volumes_r = user.volumes_r
                new_volumes_r.remove(v_id)
                fields['volumes_r'] = new_volumes_r

            if fields:
                db.update_user(user.email, **fields)


        for ug in usergateways:
            fields = {}
            fields['volume_id'] = 0
            db.update_user_gateway(ug.g_id, **fields)

        for ag in acquisitiongateways:
            logging.info(ag)
            fields = {}
            new_ids = ag.volume_ids.remove(v_id)
            if not new_ids:
                fields['volume_ids'] = []
            else:
                fields['volume_ids'] = new_ids
            db.update_acquisition_gateway(ag.g_id, **fields)

        for rg in replicagateways:
            fields = {}
            new_ids = rg.volume_ids.remove(v_id)
            if not new_ids:
                fields['volume_ids'] = []
            else:
                fields['volume_ids'] = new_ids
            db.update_replica_gateway(rg.g_id, **fields)

        # Clear initial data session variable to prevent stale tables in ag.views.viewgateway and rg.views.viewgateway
        session.pop("rg_initial_data" + str(v_id), None)
        session.pop("ag_initial_data" + str(v_id), None)
        # Clear initial data session variable to prevent stale data in volume settings, change rgs, and change ags.
        session.pop("volume_initial_ags" + str(v_id), None)
        session.pop("volume_initial_rgs" + str(v_id), None)

    session = request.session
    message = session.pop('message', "")
    username = session['login_email']
    vol = db.read_volume( volume_id )
    if not vol:
        return redirect('django_volume.views.viewvolume', volume_id)

    if request.method == "POST":
        form = forms.DeleteVolume(request.POST)
        if form.is_valid():
            # Check password hash
            hash_check = Volume.generate_password_hash(form.cleaned_data['password'], vol.volume_secret_salt)
            if hash_check == vol.volume_secret_salted_hash:
                # Ok to delete
                attrs = {}
                users = db.list_users({'SyndicateUser.volumes_rw ==':vol.volume_id})
                users.extend(db.list_users({'SyndicateUser.volumes_r ==':vol.volume_id}))
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
                return redirect('/syn/thanks')
            else:
                session['message'] = "Invalid password"
                return redirect('django_volume.views.deletevolume', volume_id=vol.volume_id)
        else:
            session['message'] = "Please fill out all entries"
            return redirect('django_volume.views.deletevolume', vol.volume_id)
    else:
        form = forms.DeleteVolume()
        t = loader.get_template('deletevolume.html')
        c = RequestContext(request, {'username':username, 'form':form, 'message':message,'volume':vol} )
        return HttpResponse(t.render(c))    


@verifyownership_private
@authenticate
def changegateways_ag(request, volume_id):
    session = request.session
    username = session['login_email']
    vol = db.read_volume( volume_id )
    if not vol:
        return redirect('django_volume.views.viewvolume', volume_id)

    # make sure this variable exists, i.e. they came from the settings page.
    if not 'volume_initial_ags' + str(volume_id) in session:
        return redirect("django_volume.views.volumesettings", volume_id)

    @transactional(xg=True)
    def update(v_id, gnames, vfields, gfields):
        db.update_volume(v_id, **vfields)
        for g, gfield in zip(gnames, gfields):
            db.update_acquisition_gateway(g, **gfield)
        session.pop('volume_initial_ags' + str(volume_id), None)

    if request.POST:
        passwordform = libforms.Password(request.POST)
        if not passwordform.is_valid():
            session['message'] = "Password required."
            return redirect('django_volume.views.volumesettings', vol.volume_id)
        else:
            hash_check = Volume.generate_password_hash(passwordform.cleaned_data['password'], vol.volume_secret_salt)
            if hash_check != vol.volume_secret_salted_hash:
                session['message'] = "Incorrect password."
                return redirect('django_volume.views.volumesettings', vol.volume_id)

        GatewayFormset = formset_factory(forms.Gateway, extra=0)
        formset = GatewayFormset(request.POST)
        formset.is_valid()
        remove_gateways = []
        remove_gateway_names = []
        remove_gateway_ids = []

        for data, form in zip(session['volume_initial_ags' + str(volume_id)], formset.forms):
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
            new_vol_ids = g.volume_ids
            new_vol_ids.remove(vol.volume_id)
            gfields.append({"volume_ids":new_vol_ids})
        try:
            update(vol.volume_id, remove_gateway_names, vfields, gfields)
        except Exception as e:
            session['message'] = "Unable to update volume or AG's."
            return redirect('django_volume.views.volumesettings', vol.volume_id)
        session['new_change'] = "We've updated your volume."
        session['next_url'] = '/syn/volume/' + str(vol.volume_id)+ '/settings'
        session['next_message'] = "Click here to go back to your volume."
        return redirect('/syn/thanks')

    else:
        return redirect('django_volume.views.volumesettings', volume_id=vol.volume_id)


@verifyownership_private
@authenticate
def changegateways_rg(request, volume_id):
    session = request.session
    username = session['login_email']
    vol = db.read_volume( volume_id )
    if not vol:
        return redirect('django_volume.views.viewvolume', volume_id)


    @transactional(xg=True)
    def update(v_id, gnames, vfields, gfields):
        db.update_volume(v_id, **vfields)
        for g, gfield in zip(gnames, gfields):
            db.update_replica_gateway(g, **gfield)
        session.pop('volume_initial_rgs' + str(volume_id), None)


    # make sure this variable exists, i.e. they came from the settings page.
    if not 'volume_initial_rgs' + str(volume_id) in session:
        return redirect("django_volume.views.volumesettings", volume_id)

    if request.POST:
        passwordform = libforms.Password(request.POST)
        if not passwordform.is_valid():
            session['message'] = "Password required."
            return redirect('django_volume.views.volumesettings', volume_id)
        else:
            hash_check = Volume.generate_password_hash(passwordform.cleaned_data['password'], vol.volume_secret_salt)
            if hash_check != vol.volume_secret_salted_hash:
                session['message'] = "Incorrect password."
                return redirect('django_volume.views.volumesettings', volume_id)

        GatewayFormset = formset_factory(forms.Gateway, extra=0)
        formset = GatewayFormset(request.POST)
        formset.is_valid()
        remove_gateways = []
        remove_gateway_names = []
        remove_gateway_ids = []

        for data, form in zip(session['volume_initial_rgs' + str(volume_id)], formset.forms):
            if form.cleaned_data['remove']:
                g = db.read_replica_gateway(data['g_name'])
                remove_gateways.append(g)
                remove_gateway_names.append(data['g_name'])
                remove_gateway_ids.append(g.rg_id)
        if not remove_gateways:
            return redirect('django_volume.views.volumesettings', volume_id)
    
        new_rgs = list(set(vol.rg_ids) - set(remove_gateway_ids))
        vfields = {'rg_ids':new_rgs}

        gfields = []
        for g in remove_gateways:
            new_vol_ids = g.volume_ids
            new_vol_ids.remove(vol.volume_id)
            gfields.append({"volume_ids":new_vol_ids})
        try:
            update(vol.volume_id, remove_gateway_names, vfields, gfields)
        except Exception as e:
            session['message'] = "Unable to update volume or RG's."
            return redirect('django_volume.views.volumesettings', volume_id)

        session['new_change'] = "We've updated your volume."
        session['next_url'] = '/syn/volume/' + str(vol.volume_id) + '/settings'
        session['next_message'] = "Click here to go back to your volume."
        return redirect('/syn/thanks')

    else:
        return redirect('django_volume.views.volumesettings', volume_id=vol.volume_id)

@verifyownership_private
@authenticate
def volumesettings(request, volume_id):
    '''
    old_data is for keeping state while changing the description when the password is wrong,
    no password is entered, etc.

    initial_data is for keeping state of the gateways when mistakes are made etc.
    '''

    session = request.session
    username = session['login_email']
    vol = db.read_volume( volume_id )
    if not vol:
        return redirect('django_volume.views.viewvolume', volume_id)

    message = session.pop('message', "")
    initial_data_ag = session.get('volume_initial_ags' + str(volume_id), None)
    initial_data_rg = session.get('volume_initial_rgs' + str(volume_id), None)
    old_data = session.get('old_data' + str(volume_id), None)

    if not initial_data_ag:
        initial_data_ag = []
        ags = db.list_acquisition_gateways_by_volume(vol.volume_id)
        for g in ags:
            initial_data_ag.append({'g_name':g.ms_username, 'remove':False})
        session['volume_initial_ags' + str(volume_id)] = initial_data_ag
    
    if not initial_data_rg:
        initial_data_rg = []
        rgs = db.list_replica_gateways_by_volume(vol.volume_id)
        for g in rgs:
            initial_data_rg.append({'g_name':g.ms_username, 'remove':False})
        session['volume_initial_rgs' + str(volume_id)] = initial_data_rg

    if old_data:
        desc_form = forms.ChangeVolumeD(initial={'description': old_data})
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

@verifyownership_private
@authenticate
def volumeprivacy(request, volume_id):
    session = request.session
    username = session['login_email']
    vol = db.read_volume( volume_id )
    if not vol:
        return redirect('django_volume.views.viewvolume', volume_id)

    if request.POST:

        form = libforms.Password(request.POST)
        if not form.is_valid():
            session['message'] = "Password required."
            return redirect('django_volume.views.volumesettings', volume_id)
        # Check password hash
        hash_check = Volume.generate_password_hash(form.cleaned_data['password'], vol.volume_secret_salt)
        if hash_check != vol.volume_secret_salted_hash:
            session['message'] = "Incorrect password."
            return redirect('django_volume.views.volumesettings', volume_id)

        if vol.private:
            fields = {"private":False}
            db.update_volume(vol.volume_id, **fields)
            session['new_change'] = "We've publicized your volume."
            session['next_url'] = '/syn/volume/' + str(vol.volume_id)
            session['next_message'] = "Click here to go back to your volume."
            return redirect('/syn/thanks')
        else:
            fields = {"private":True}
            db.update_volume(vol.volume_id, **fields)
            session['new_change'] = "We've privatized your volume."
            session['next_url'] = '/syn/volume/' + str(vol.volume_id)
            session['next_message'] = "Click here to go back to your volume."
            return redirect('/syn/thanks')

    else:
        return redirect("/syn/volume/" + str(vol.volume_id))


@verifyownership_private
@authenticate
def changedesc(request, volume_id):
    session = request.session
    username = session['login_email']
    vol = db.read_volume( volume_id )
    if not vol:
        return redirect('django_volume.views.viewvolume', volume_id)

    if request.method != "POST":
        return redirect('/syn/volume/' + str(vol.volume_id) + '/settings')


    form = libforms.Password(request.POST)
    desc_form = forms.ChangeVolumeD(request.POST)
    old_data = {}

    if not form.is_valid():
        session['message'] = "Password required."
        desc_form.is_valid()
        if desc_form.errors:
            session['old_data' + str(volume_id)] = ""
        else:
            session['old_data' + str(volume_id)] = desc_form.cleaned_data['description']
        return redirect('django_volume.views.volumesettings', volume_id)

    # Check password hash
    hash_check = Volume.generate_password_hash(form.cleaned_data['password'], vol.volume_secret_salt)
    if hash_check != vol.volume_secret_salted_hash:
        session['message'] = "Incorrect password."
        desc_form.is_valid()
        if desc_form.errors:
            session['old_data' + str(volume_id)] = ""
        else:
            session['old_data' + str(volume_id)] = desc_form.cleaned_data['description']
        return redirect('django_volume.views.volumesettings', volume_id)

    if not desc_form.is_valid():
        session['message'] = "Invalid description field entries."
        return redirect('django_volume.views.volumesettings', volume_id)

    kwargs = {}
    if desc_form.cleaned_data['description']:
        kwargs['description'] = desc_form.cleaned_data['description']
    db.update_volume(vol.volume_id, **kwargs)
    session.pop('old_data' + str(volume_id), None)
    session['new_change'] = "We've changed your volume description."
    session['next_url'] = '/syn/volume/' + str(vol.volume_id)
    session['next_message'] = "Click here to go back to your volume."
    return redirect('/syn/thanks')

@verifyownership_private
@authenticate
def changevolumepassword(request, volume_id):
    session = request.session
    username = session['login_email']
    vol = db.read_volume( volume_id )
    if not vol:
        return redirect('django_volume.views.viewvolume', volume_id)

    if request.method != "POST":
        return redirect('/syn/volume/' + str(volume_id) + '/settings')

    form = libforms.ChangePassword(request.POST)
    if not form.is_valid():
        session['message'] = "You must fill out all password fields."
        return redirect('django_volume.views.volumesettings', volume_id)
    else:

        # Check password hash
        hash_check = Volume.generate_password_hash(form.cleaned_data['oldpassword'], vol.volume_secret_salt)
        if hash_check != vol.volume_secret_salted_hash:
            session['message'] = "Incorrect password."
            return redirect('django_volume.views.volumesettings', volume_id)
        elif form.cleaned_data['newpassword_1'] != form.cleaned_data['newpassword_2']:
            session['message'] = "Your new passwords did not match each other."
            return redirect('django_volume.views.volumesettings', volume_id)

        # Ok change password 
        kwargs = {}
        new_volume_secret_salt, new_volume_secret_salted_hash = Volume.generate_volume_secret(form.cleaned_data['newpassword_1'])
        kwargs['volume_secret_salted_hash'] = new_volume_secret_salted_hash
        kwargs['volume_secret_salt'] = new_volume_secret_salt
        db.update_volume(vol.volume_id, **kwargs)

        session['new_change'] = "We've changed your volume's password."
        session['next_url'] = '/syn/volume/' + str(vol.volume_id)
        session['next_message'] = "Click here to go back to your volume."
        return redirect('/syn/thanks')


@authenticate
def failure(request):
    ''' 
    This view is passed when someone tries to access
    a volume that doesn't exist or they don't have permission to acess.
    '''
    session = request.session
    username = session['login_email']
    t = loader.get_template('viewvolume_failure.html')
    c = Context({'username':session['login_email']})
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
                              url=MS_URL,
                              version=1,
                              ctime_sec=now_sec,
                              ctime_nsec=now_nsec,
                              mtime_sec=now_sec,
                              mtime_nsec=now_nsec,
                              owner_id=volume.owner_id,
                              coordinator_id=0,
                              volume_id=volume.volume_id,
                              mode=0755,
                              size=4096,
                              max_read_freshness=5000,
                              max_write_freshness=0
                            )

            session['new_change'] = "Your new volume is ready."
            session['next_url'] = '/syn/volume/myvolumes/'
            session['next_message'] = "Click here to see your volumes."
            return redirect('/syn/thanks/')

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
