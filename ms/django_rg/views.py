'''
We're going to assume you need to be logged in already and have a valid session
for this to work.
'''
import logging
import json

from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import redirect

from django.template import Context, loader, RequestContext
from django.views.decorators.csrf import csrf_exempt, csrf_protect
from django.forms.formsets import formset_factory

from django_lib.auth import authenticate
from django_lib.decorators import precheck
from django_lib import gatewayforms
from django_lib import forms as libforms

from storage.storagetypes import transactional
import storage.storage as db

from MS.volume import Volume
from MS.user import SyndicateUser as User
from MS.gateway import ReplicaGateway as RG

PRECHECK_REDIRECT = 'django_rg.views.viewgateway'

@authenticate
def viewgateway(request, g_name=""):
    session = request.session
    username = session['login_email']
    message = session.pop('message', "")
    initial_data = session.get('initial_data', [])

    try:
        g = db.read_replica_gateway(g_name)
        if not g:
            raise Exception("No gateway exists.")
    except Exception as e:
        logging.error("Error reading gateway %s : Exception: %s" % (g_name, e))
        message = "No replica gateway by the name of %s exists." % g_name
        t = loader.get_template("gateway_templates/viewgateway_failure.html")
        c = Context({'message':message, 'username':username})
        return HttpResponse(t.render(c))

    location_form = gatewayforms.ModifyGatewayLocation(initial={'host':g.host,
                                                                'port':g.port})
    add_form = gatewayforms.GatewayAddVolume()

    json_form = gatewayforms.ModifyGatewayConfig()

    owners = []
    vols = []
    for v_id in g.volume_ids:
        vol = db.get_volume(v_id)
        if not vol:
            logging.error("Volume ID in gateways volume_ids does not map to volume. Gateway: %s" % g_name)
        vols.append(vol)
        attrs = {"SyndicateUser.owner_id ==": vol.volume_id}
        owners.append(db.get_user(attrs))
    if not initial_data:
        for v in vols:
            initial_data.append({'volume_name':v.name,
                                 'remove':False})
        session['initial_data'] = initial_data

    vol_owners = zip(vols, owners)
    logging.info("length of vol_owners" + str(len(vol_owners)))
    VolumeFormSet = formset_factory(gatewayforms.GatewayRemoveVolume, extra=0)
    if initial_data:
        formset = VolumeFormSet(initial=initial_data)
        logging.info("length of formset is " + str(len(formset.forms)))
    else:
        formset = []
    password_form = libforms.Password()
    change_password_form = libforms.ChangePassword()

    t = loader.get_template("gateway_templates/viewreplicagateway.html")
    c = RequestContext(request, {'username':username,
                        'gateway':g,
                        'message':message,
                        'vol_owners':vol_owners,
                        'location_form':location_form,
                        'add_form':add_form,
                        'json_form':json_form,
                        'remove_forms':formset,
                        'password_form':password_form,
                        'change_password_form':change_password_form})
    return HttpResponse(t.render(c))

@authenticate
@precheck("RG", PRECHECK_REDIRECT)
def changeprivacy(request, g_name):
    session = request.session
    username = session['login_email']

    g = db.read_replica_gateway(g_name)
    if g.private:
        fields = {'private':False}
    else:
        fields = {'private':True}
    try:
        db.update_replica_gateway(g_name, **fields)
        session['new_change'] = "We've changed your gateways's privacy setting."
        session['next_url'] = '/syn/RG/viewgateway/' + g_name
        session['next_message'] = "Click here to go back to your gateway."
        return HttpResponseRedirect('/syn/thanks')
    except Exception as e:
        session['message'] = "Unable to update gateway."
        logging.info(message)
        logging.info(e)
        return redirect('django_rg.views.viewgateway', g_name)

@authenticate
@precheck("RG", PRECHECK_REDIRECT)
def changejson(request, g_name):
    session = request.session
    username = session['login_email']

    form = gatewayforms.ModifyGatewayConfig(request.POST)
    if form.is_valid():
        logging.info(request.FILES)
        if 'json_config' not in request.FILES:
            session['message'] = "No uploaded file."
            return redirect('django_rg.views.viewgateway', g_name)
        if request.FILES['json_config'].multiple_chunks():
            session['message'] = "Uploaded file too large; please make smaller than 2.5M"
            return redirect('django_rg.views.viewgateway', g_name)
        config = request.FILES['json_config'].read()
        fields = {}
        try:
            fields['json_config'] = json.loads(config)
        except Exception as e:
            logging.info("Possible JSON load error: %s" % e)
            try:
                fields['json_config'] = json.loads("\"" + config + "\"")
            except Exception as e:
                logging.error("Definite JSON load error %s" % e)
                session['message'] = "Error parsing given JSON text."
                return redirect('django_rg.views.viewgateway', g_name)
        db.update_replica_gateway(g_name, **fields)
        session['new_change'] = "We've changed your gateways's JSON configuration."
        session['next_url'] = '/syn/RG/viewgateway/' + g_name
        session['next_message'] = "Click here to go back to your gateway."
        return HttpResponseRedirect('/syn/thanks')
    else:
        session['message'] = "Invalid form. Did you upload a file?"
        return redirect('django_rg.views.viewgateway', g_name)

# Doesn't use precheck() because doesn't use Password() form, just ChangePassword() form.
@authenticate
def changepassword(request, g_name):
    session = request.session
    username = session['login_email']

    if request.method != "POST":
        return HttpResponseRedirect('/syn/RG/viewgateway' + g_name)

    try:
        g = db.read_replica_gateway(g_name)
        if not g:
            raise Exception("No gateway exists.")
    except Exception as e:
        logging.error("Error reading gateway %s : Exception: %s" % (g_name, e))
        message = "No replica gateway by the name of %s exists." % g_name
        t = loader.get_template("gateway_templates/viewgateway_failure.html")
        c = Context({'message':message, 'username':username})
        return HttpResponse(t.render(c))

    form = libforms.ChangePassword(request.POST)
    if not form.is_valid():
        session['message'] = "You must fill out all password fields."
        return redirect('django_rg.views.viewgateway', g_name)
    else:
        # Check password hash
        if not RG.authenticate(g, form.cleaned_data['oldpassword']):
            session['message'] = "Incorrect password."
            return redirect('django_rg.views.viewgateway', g_name)
        elif form.cleaned_data['newpassword_1'] != form.cleaned_data['newpassword_2']:
            session['message'] = "Your new passwords did not match each other."
            return redirect('django_rg.views.viewgateway', g_name)
        # Ok to change password
        else:
            new_hash = RG.generate_password_hash(form.cleaned_data['newpassword_1'])
            fields = {'ms_password_hash':new_hash}
            try:
                db.update_replica_gateway(g_name, **fields)
            except Exception as e:
                logging.error("Unable to update replica gateway %s. Exception %s" % (g_name, e))
                session['message'] = "Unable to update gateway."
                return redirect('django_rg.views.viewgateway', g_name)

            session['new_change'] = "We've changed your gateways's password."
            session['next_url'] = '/syn/RG/viewgateway/' + g_name
            session['next_message'] = "Click here to go back to your volume."
            return HttpResponseRedirect('/syn/thanks')

@authenticate
@precheck("RG", PRECHECK_REDIRECT)
def addvolume(request, g_name):
    session = request.session
    username = session['login_email']

    @transactional(xg=True)
    def update(vname, gname, vfields, gfields):
        db.update_volume(vname, **vfields)
        db.update_replica_gateway(g_name, **gfields)
        session.pop('initial_data')

    form = gatewayforms.GatewayAddVolume(request.POST)
    if form.is_valid():
        volume = db.read_volume(form.cleaned_data['volume_name'])
        if not volume:
            session['message'] = "The volume %s doesn't exist." % form.cleaned_data['volume_name']
            return redirect('django_rg.views.viewgateway', g_name)

        gateway = db.read_replica_gateway(g_name)

        # prepare volume state
        if volume.rg_ids:
            new_rgs = volume.rg_ids[:]
            new_rgs.append(gateway.rg_id)
        else:
            new_rgs = [gateway.rg_id]
        vfields = {'rg_ids':new_rgs}

        # prepate RG state
        old_vids = gateway.volume_ids
        new_vid = volume.volume_id
        if new_vid in old_vids:
            session['message'] = "That volume is already attached to this gateway!"
            return redirect('django_rg.views.viewgateway', g_name)
        if old_vids:
            old_vids.append(new_vid)
            new_vids = old_vids
        else:
            new_vids = [new_vid]
        try:
            gfields={'volume_ids':new_vids}
            update(form.cleaned_data['volume_name'], g_name, vfields, gfields)
        except Exception as e:
            logging.error("Unable to update replica gateway %s or volume %s. Exception %s" % (g_name, form.cleaned_data['volume_name'], e))
            session['message'] = "Unable to update."
            return redirect('django_rg.views.viewgateway', g_name)
        session['new_change'] = "We've updated your RG's volumes."
        session['next_url'] = '/syn/RG/viewgateway/' + g_name
        session['next_message'] = "Click here to go back to your gateway."
        return HttpResponseRedirect('/syn/thanks')
    else:
        session['message'] = "Invalid entries for adding volumes."
        return redirect('django_rg.views.viewgateway', g_name)

@authenticate
@precheck("RG", PRECHECK_REDIRECT)
def removevolumes(request, g_name):

    # This is a helper method that isolates the @transactional decorator, speeding
    # up the code when it doesn't reach update() in this view and allowing for
    # errors that would break in GAE if the decorator was applied to the entire view.
    # It updates multiple volumes at once
    @transactional(xg=True)
    def multi_update(vnames, gname, vfields, gfields):

        for vname, vfield in zip(vnames, vfields):
            db.update_volume(vname, **vfield)
        db.update_replica_gateway(g_name, **gfields)
        session.pop('initial_data', "")

    session = request.session
    username = session['login_email']

    VolumeFormSet = formset_factory(gatewayforms.GatewayRemoveVolume, extra=0)
    formset = VolumeFormSet(request.POST)
    formset.is_valid()

    volume_ids_to_be_removed = []
    volume_names_to_be_removed = []

    initial_and_forms = zip(session.get('initial_data', []), formset.forms)
    new_rgs_set = []
    for i, f in initial_and_forms:
        if f.cleaned_data['remove']:
            vol = db.read_volume(i['volume_name'])

            # update each volumes new RG list
            new_rgs = vol.rg_ids[:]
            new_rgs.remove(db.read_replica_gateway(g_name).rg_id)
            new_rgs_set.append({'rg_ids':new_rgs})

            volume_names_to_be_removed.append(i['volume_name'])
            volume_ids_to_be_removed.append(db.read_volume(i['volume_name']).volume_id)

    if not volume_ids_to_be_removed:
        session['message'] = "You must select at least one volume to remove."
        return redirect('django_rg.views.viewgateway', g_name)

    old_vids = set(db.read_replica_gateway(g_name).volume_ids)
    new_vids = list(old_vids - set(volume_ids_to_be_removed))
    gfields = {'volume_ids':new_vids}
    try:
        multi_update(volume_names_to_be_removed, g_name, new_rgs_set, gfields)
    except Exception as e:
         logging.error("Unable to update replica gateway %s. Exception %s" % (g_name, e))
         session['message'] = "Unable to update gateway."
         return redirect('django_rg.views.viewgateway', g_name)
    session['new_change'] = "We've updated your RG's volumes."
    session['next_url'] = '/syn/RG/viewgateway/' + g_name
    session['next_message'] = "Click here to go back to your gateway."
    return HttpResponseRedirect('/syn/thanks')


@authenticate
@precheck("RG", PRECHECK_REDIRECT)
def changelocation(request, g_name):
    session = request.session
    username = session['login_email']

    form = gatewayforms.ModifyGatewayLocation(request.POST)
    if form.is_valid():
        new_host = form.cleaned_data['host']
        new_port = form.cleaned_data['port']
        fields = {'host':new_host, 'port':new_port}
        try:
            db.update_replica_gateway(g_name, **fields)
        except Exception as e:
            logging.error("Unable to update RG: %s. Error was %s." % (g_name, e))
            session['message'] = "Error. Unable to change replica gateway."
            return redirect('django_rg.views.viewgateway', g_name)
        session['new_change'] = "We've updated your RG."
        session['next_url'] = '/syn/RG/viewgateway/' + g_name
        session['next_message'] = "Click here to go back to your gateway."
        return HttpResponseRedirect('/syn/thanks')
    else:
        session['message'] = "Invalid form entries for gateway location."
        return redirect('django_rg.views.viewgateway', g_name)

@authenticate
def allgateways(request):
    session = request.session
    username = session['login_email']

    try:
        qry = db.list_replica_gateways()
    except:
        qry = []
    gateways = []
    for g in qry:
        gateways.append(g)
    vols = []
    g_owners = []
    for g in gateways:
        volset = []
        for v in g.volume_ids:
            volset.append(db.get_volume(v))
        vols.append(volset)
        attrs = {"SyndicateUser.owner_id ==":g.owner_id}
        g_owners.append(db.get_user(attrs))
    gateway_vols_owners = zip(gateways, vols, g_owners)
    t = loader.get_template('gateway_templates/allreplicagateways.html')
    c = RequestContext(request, {'username':username, 'gateway_vols_owners':gateway_vols_owners})
    return HttpResponse(t.render(c))

@csrf_exempt
@authenticate
def create(request):

    session = request.session
    username = session['login_email']

    def give_create_form(username, session):
        message = session.pop('message' "")
        form = gatewayforms.CreateRG()
        t = loader.get_template('gateway_templates/create_replica_gateway.html')
        c = RequestContext(request, {'username':username,'form':form, 'message':message})
        return HttpResponse(t.render(c))

    if request.POST:
        # Validate input forms
        form = gatewayforms.CreateRG(request.POST, request.FILES)
        if form.is_valid():
            kwargs = {}
            
            # Try and load JSON config file/data, if present
            if "json_config" in request.FILES:
                if request.FILES['json_config'].multiple_chunks():
                    session['message'] = "Uploaded file too large; please make smaller than 2.5M"
                    return give_create_form(username, session)
                config = request.FILES['json_config'].read()
                try:
                    kwargs['json_config'] = json.loads(config)
                except Exception as e:
                    logging.info("Possible JSON load error: %s" % e)
                    try:
                        kwargs['json_config'] = json.loads("\"" + config + "\"")
                    except Exception as e:
                        logging.error("Definite JSON load error %s" % e)
                        session['message'] = "Error parsing given JSON text."
                        return give_create_form(username, session)
            elif "json_config_text" in form.cleaned_data:
                try:
                    kwargs['json_config'] = json.loads(form.cleaned_data['json_config_text'])
                except Exception as e:
                    logging.info("Possible JSON load error: %s" % e)
                    try:
                        kwargs['json_config'] = json.loads("\"" + str(form.cleaned_data['json_config_text']) + "\"")
                    except Exception as e:
                        logging.error("Definite JSON load error %s" % e)
                        session['message'] = "Error parsing given JSON text."
                        return give_create_form(username, session)     


            try:
                kwargs['ms_username'] = form.cleaned_data['g_name']
                kwargs['port'] = form.cleaned_data['port']
                kwargs['host'] = form.cleaned_data['host']
                kwargs['ms_password'] = form.cleaned_data['g_password']
                kwargs['private'] = form.cleaned_data['private']
                new_RG = db.create_replica_gateway(user, **kwargs)
            except Exception as E:
                session['message'] = "RG creation error: %s" % E
                return give_create_form(username, session)

            session['new_change'] = "Your new gateway is ready."
            session['next_url'] = '/syn/RG/allgateways'
            session['next_message'] = "Click here to see all replica gateways."
            return HttpResponseRedirect('/syn/thanks/')
        else:
            # Prep returned form values (so they don't have to re-enter stuff)
            if 'g_name' in form.errors:
                oldname = ""
            else:
                oldname = request.POST['g_name']
            if 'host' in form.errors:
                oldhost = ""
            else:
                oldhost = request.POST['host']
            if 'port' in form.errors:
                oldport = ""
            else:
                oldport = request.POST['port']
            oldjson = request.POST['json_config_text']

            # Prep error message
            message = "Invalid form entry: "

            for k, v in form.errors.items():
                message = message + "\"" + k + "\"" + " -> " 
                for m in v:
                    message = message + m + " "

            # Give then the form again
            form = gatewayforms.CreateRG(initial={'g_name': oldname,
                                       'host': oldhost,
                                       'port': oldport,
                                       'json_config_text':oldjson,
                                       })
            t = loader.get_template('gateway_templates/create_replica_gateway.html')
            c = RequestContext(request, {'username':username,'form':form, 'message':message})
            return HttpResponse(t.render(c))

    else:
        # Not a POST, give them blank form
        return give_create_form(username, session)

@csrf_exempt
@authenticate
def delete(request, g_name):

    def give_delete_form(username, g_name, session):
        form = gatewayforms.DeleteGateway()
        t = loader.get_template('gateway_templates/delete_replica_gateway.html')
        c = RequestContext(request, {'username':username, 'g_name':g_name, 'form':form, 'message':session.pop('message', "")})
        return HttpResponse(t.render(c))

    session = request.session
    username = session['login_email']

    rg = db.read_replica_gateway(g_name)
    if not rg:
        t = loader.get_template('gateway_templates/delete_replica_gateway_failure.html')
        c = RequestContext(request, {'username':username, 'g_name':g_name})
        return HttpResponse(t.render(c))

    '''
    if rg.owner_id != user.owner_id:
                t = loader.get_template('gateway_templates/delete_replica_gateway_failure.html')
                c = RequestContext(request, {'username':username, 'g_name':g_name})
                return HttpResponse(t.render(c))
    '''

    if request.POST:
        # Validate input forms
        form = gatewayforms.DeleteGateway(request.POST)
        if form.is_valid():
            if not RG.authenticate(rg, form.cleaned_data['g_password']):
                session['message'] = "Incorrect Replica Gateway password"
                return give_delete_form(username, g_name, session)
            if not form.cleaned_data['confirm_delete']:
                session['message'] = "You must tick the delete confirmation box."
                return give_delete_form(username, g_name, session)
            
            db.delete_replica_gateway(g_name)
            session['new_change'] = "Your gateway has been deleted."
            session['next_url'] = '/syn/RG/allgateways'
            session['next_message'] = "Click here to see all replica gateways."
            return HttpResponseRedirect('/syn/thanks/')

        # invalid forms
        else:  
            # Prep error message
            session['message'] = "Invalid form entry: "

            for k, v in form.errors.items():
                session['message'] = session['message'] + "\"" + k + "\"" + " -> " 
                for m in v:
                    session['message'] = session['message'] + m + " "

            return give_delete_form(username, g_name, session)
    else:
        # Not a POST, give them blank form
        return give_delete_form(username, g_name, session)

@csrf_exempt
@authenticate
def urlcreate(request, g_name, g_password, host, port, private=False, volume_name="",):
    session = request.session
    username = session['login_email']

    kwargs = {}

    kwargs['port'] = int(port)
    kwargs['host'] = host
    kwargs['ms_username'] = g_name
    kwargs['ms_password'] = g_password
    kwargs['private'] = private
    if volume_name:
        vol = db.read_volume(volume_name)        
        if not vol:
            return HttpResponse("No volume %s exists." % volume_name)
        if (vol.volume_id not in user.volumes_r) and (vol.volume_id not in user.volumes_rw):
            return HttpResponse("Must have read rights to volume %s to create RG for it." % volume_name)
        kwargs['volume_ids'] = list(vol.volume_id)

    try:
        new_rg = db.create_replica_gateway(user, vol, **kwargs)
    except Exception as E:
        return HttpResponse("RG creation error: %s" % E)

    return HttpResponse("RG succesfully created: " + str(new_rg))

@csrf_exempt
@authenticate
def urldelete(request, g_name, g_password):
    session = request.session
    username = session['login_email']

    rg = db.read_replica_gateway(g_name)
    if not rg:
        return HttpResponse("RG %s does not exist." % g_name)
    #if rg.owner_id != user.owner_id:
        #return HttpResponse("You must own this RG to delete it.")
    if not rg.authenticate(rg, g_password):
        return HttpResponse("Incorrect RG password.")
    db.delete_replica_gateway(g_name)
    return HttpResponse("Gateway succesfully deleted.")
