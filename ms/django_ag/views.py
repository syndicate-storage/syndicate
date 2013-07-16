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

from django_lib import gatewayforms
from django_lib import forms as libforms

from storage.storagetypes import transactional
import storage.storage as db

from MS.volume import Volume
from MS.user import SyndicateUser as User
from MS.gateway import AcquisitionGateway as AG

# Ensure passwords/gateway existence.
def precheck(request, g_name):
    session = request.session
    username = session['login_email']
    try:
        g = db.read_acquisition_gateway(g_name)
        if not g:
            raise Exception("No gateway exists.")
    except Exception as e:
        logging.error("Error reading gateway %s : Exception: %s" % (g_name, e))
        message = "No acquisition gateway by the name of %s exists." % g_name
        t = loader.get_template("gateway_templates/viewgateway_failure.html")
        c = Context({'message':message, 'username':username})
        return HttpResponse(t.render(c))

    form = libforms.Password(request.POST)
    if not form.is_valid():
        message = "Password required."
        return viewgateway(request, g_name, message=message)
    # Check password hash
    if not AG.authenticate(g, form.cleaned_data['password']):
        message = "Incorrect password."
        return viewgateway(request, g_name, message=message)
    return None

@authenticate
def changejson(request, g_name):
    session = request.session
    username = session['login_email']
    user = db.read_user(username)

    if request.POST:
        failure = precheck(request, g_name)
        if failure:
            return failure
    else:
        return redirect('django_ag.views.viewgateway', g_name=g_name)

    form = gatewayforms.ModifyGatewayConfig(request.POST)
    if form.is_valid():
        if 'json_config' not in request.FILES:
            message = "No uploaded file."
            return viewgateway(request, g_name, message=message)
        if request.FILES['json_config'].multiple_chunks():
            message = "Uploaded file too large; please make smaller than 2.5M"
            return viewgateway(request, g_name, message=message)
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
                message = "Error parsing given JSON text."
                return viewgateway(request, g_name, message=message)
        db.update_acquisition_gateway(g_name, **fields)
        session['new_change'] = "We've changed your gateways's JSON configuration."
        session['next_url'] = '/syn/AG/viewgateway/' + g_name
        session['next_message'] = "Click here to go back to your gateway."
        return HttpResponseRedirect('/syn/thanks')
    else:
        message = "Invalid form. Did you upload a file?"
        return viewgateway(request, g_name, message=message)


# Doesn't use precheck() because doesn't use Password() form, just ChangePassword() form.
@authenticate
def changepassword(request, g_name):
    session = request.session
    username = session['login_email']
    user = db.read_user(username)

    if request.method != "POST":
        return HttpResponseRedirect('/syn/AG/viewgateway' + g_name)

    try:
        g = db.read_acquisition_gateway(g_name)
        if not g:
            raise Exception("No gateway exists.")
    except Exception as e:
        logging.error("Error reading gateway %s : Exception: %s" % (g_name, e))
        message = "No acquisition gateway by the name of %s exists." % g_name
        t = loader.get_template("gateway_templates/viewgateway_failure.html")
        c = Context({'message':message, 'username':username})
        return HttpResponse(t.render(c))

    form = libforms.ChangePassword(request.POST)
    if not form.is_valid():
        message = "You must fill out all password fields."
        return viewgateway(request, g_name, message=message)
    else:
        # Check password hash
        if not AG.authenticate(g, form.cleaned_data['oldpassword']):
            message = "Incorrect password."
            return viewgateway(request, g_name, message=message)
        elif form.cleaned_data['newpassword_1'] != form.cleaned_data['newpassword_2']:
            message = "Your new passwords did not match each other."
            return viewgateway(request, g_name, message=message)
        # Ok to change password
        else:
            new_hash = AG.generate_password_hash(form.cleaned_data['newpassword_1'])
            fields = {'ms_password_hash':new_hash}
            try:
                db.update_acquisition_gateway(g_name, **fields)
            except Exception as e:
                logging.error("Unable to update acquisition gateway %s. Exception %s" % (g_name, e))
                message = "Unable to update gateway."
                return viewgateway(request, g_name, message)

            session['new_change'] = "We've changed your gateways's password."
            session['next_url'] = '/syn/AG/viewgateway' + g_name
            session['next_message'] = "Click here to go back to your volume."
            return HttpResponseRedirect('/syn/thanks')

@transactional(xg=True)
@authenticate
def addvolume(request, g_name):
    session = request.session
    username = session['login_email']
    user = db.read_user(username)

    if request.POST:
        failure = precheck(request, g_name)
        if failure:
            return failure
    else:
        return redirect('django_ag.views.viewgateway', g_name=g_name)

    form = gatewayforms.GatewayAddVolume(request.POST)
    if form.is_valid():
        volume = db.read_volume(form.cleaned_data['volume_name'])
        if not volume:
            message = "The volume %s doesn't exist." % form.cleaned_data['volume_name']
            return viewgateway(request, g_name, message)

        gateway = db.read_acquisition_gateway(g_name)

        # update volume state
        if volume.ag_ids:
            new_ags = volume.ag_ids[:]
            new_ags.append(gateway.ag_id)
        else:
            new_ags = [gateway.ag_id]
        fields = {'ag_ids':new_ags}
        db.update_volume(form.cleaned_data['volume_name'], **fields)

        # update AG state
        old_vids = gateway.volume_ids
        new_vid = volume.volume_id
        if new_vid in old_vids:
            message = "That volume is already attached to this gateway!"
            return viewgateway(request, g_name, message)
        if old_vids:
            old_vids.append(new_vid)
            new_vids = old_vids
        else:
            new_vids = [new_vid]
        try:
            fields={'volume_ids':new_vids}
            db.update_acquisition_gateway(g_name, **fields)
        except Exception as e:
            logging.error("Unable to update acquisition gateway %s. Exception %s" % (g_name, e))
            message = "Unable to update gateway."
            return viewgateway(request, g_name, message)
        session['new_change'] = "We've updated your AG's volumes."
        session['next_url'] = '/syn/AG/viewgateway/' + g_name
        session['next_message'] = "Click here to go back to your gateway."
        return HttpResponseRedirect('/syn/thanks')
    else:
        message = "Invalid entries for adding volumes."
        return viewgateway(request, g_name, message, session['initial_data']) 

@authenticate
def removevolumes(request, g_name):
    session = request.session
    username = session['login_email']
    user = db.read_user(username)

    if request.POST:
        failure = precheck(request, g_name)
        logging.info(failure)
        if failure:
            return failure
    else:
        return redirect('django_ag.views.viewgateway', g_name=g_name)

    VolumeFormSet = formset_factory(gatewayforms.GatewayRemoveVolume, extra=0)
    formset = VolumeFormSet(request.POST)
    formset.is_valid()

    volume_ids_to_be_removed = []

    initial_and_forms = zip(session['initial_data'], formset.forms)
    for i, f in initial_and_forms:
        if f.cleaned_data['remove']:

            vol = db.read_volume(i['volume_name'])

            # update volume state
            new_ags = vol.ag_ids[:]
            new_ags.remove(db.read_acquisition_gateway(g_name).ag_id)
            fields = {'ag_ids':new_ags}
            db.update_volume(i['volume_name'], **fields)

            # Add to list of volumes to be removed.
            volume_ids_to_be_removed.append(db.read_volume(i['volume_name']).volume_id)
    if not volume_ids_to_be_removed:
        message = "You must select at least one volume to remove."
        return viewgateway(request, g_name, message)
    old_vids = set(db.read_acquisition_gateway(g_name).volume_ids)
    new_vids = list(old_vids - set(volume_ids_to_be_removed))
    fields = {'volume_ids':new_vids}
    try:
        db.update_acquisition_gateway(g_name, **fields)
    except Exception as e:
        logging.error("Unable to update acquisition gateway %s. Exception %s" % (g_name, e))
        message = "Unable to update gateway."
        return viewgateway(request, g_name, message)
    session['new_change'] = "We've updated your AG's volumes."
    session['next_url'] = '/syn/AG/viewgateway/' + g_name
    session['next_message'] = "Click here to go back to your gateway."
    return HttpResponseRedirect('/syn/thanks')


@authenticate
def changelocation(request, g_name):
    session = request.session
    username = session['login_email']
    user = db.read_user(username)

    if request.POST:
        failure = precheck(request, g_name)
        if failure:
            return failure

        form = gatewayforms.ModifyGatewayLocation(request.POST)
        if form.is_valid():
            new_host = form.cleaned_data['host']
            new_port = form.cleaned_data['port']
            fields = {'host':new_host, 'port':new_port}
            try:
                db.update_acquisition_gateway(g_name, **fields)
            except Exception as e:
                logging.error("Unable to update AG: %s. Error was %s." % (g_name, e))
                message = "Error. Unable to change acquisition gateway."
                return viewgateway(request, g_name, message)
            session['new_change'] = "We've updated your AG."
            session['next_url'] = '/syn/AG/viewgateway/' + g_name
            session['next_message'] = "Click here to go back to your gateway."
            return HttpResponseRedirect('/syn/thanks')
        else:
            message = "Invalid form entries for gateway location."
            return viewgateway(request, g_name, message)
    else:
        return redirect('django_ag.views.viewgateway', g_name=g_name)

@authenticate
def viewgateway(request, g_name="", message="", initial_data=[]):
    session = request.session
    username = session['login_email']
    user = db.read_user(username)
    try:
        g = db.read_acquisition_gateway(g_name)
        if not g:
            raise Exception("No gateway exists.")
    except Exception as e:
        logging.error("Error reading gateway %s : Exception: %s" % (g_name, e))
        message = "No acquisition gateway by the name of %s exists." % g_name
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
        attrs = {'Volume.volume_id':"== %s" % v_id}
        vol = db.get_volume(**attrs)
        if not vol:
            logging.error("Volume ID in gateways volume_ids does not map to volume. Gateway: %s" % g_name)
        vols.append(vol)
        attrs = {"SyndicateUser.owner_id":"== %s" % vol.volume_id}
        owners.append(db.get_user(**attrs))

    if not initial_data:
        for v in vols:
            initial_data.append({'volume_name':v.name,
                             'remove':False})

    vol_owners = zip(vols, owners)
    VolumeFormSet = formset_factory(gatewayforms.GatewayRemoveVolume, extra=0)
    if initial_data:
        formset = VolumeFormSet(initial=initial_data)
    else:
        formset = []
    session['initial_data'] = initial_data
    password_form = libforms.Password()
    change_password_form = libforms.ChangePassword()

    t = loader.get_template("gateway_templates/viewacquisitiongateway.html")
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
def allgateways(request):
    session = request.session
    username = session['login_email']
    user = db.read_user(username)

    try:
        qry = db.list_acquisition_gateways()
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
            #logging.info(v)
            attrs = {"Volume.volume_id":"== " + str(v)}
            volset.append(db.get_volume(**attrs))
            #logging.info(volset)
        vols.append(volset)
        attrs = {"SyndicateUser.owner_id":"== " + str(g.owner_id)}
        g_owners.append(db.get_user(**attrs))
    gateway_vols_owners = zip(gateways, vols, g_owners)
    t = loader.get_template('gateway_templates/allacquisitiongateways.html')
    c = RequestContext(request, {'username':username, 'gateway_vols_owners':gateway_vols_owners})
    return HttpResponse(t.render(c))

'''
@authenticate
def mygateways(request):
    session = request.session
    username = session['login_email']
    user = db.read_user(username)

    # should change this
    try:
        qry = AG.query(AG.owner_id == user.owner_id)
    except:
        qry = []
    gateways = []
    for g in qry:
        gateways.append(g)
    vols = []
    for g in gateways:
        attrs = {"Volume.volume_id":"== " + str(g.volume_id)}
        volumequery = db.list_volumes(**attrs) # should be one
        for v in volumequery:
            vols.append(v)
            logging.info(v)
    gateway_vols = zip(gateways, vols)
    t = loader.get_template('gateway_templates/myacquisitiongateways.html')
    c = RequestContext(request, {'username':username, 'gateway_vols':gateway_vols})
    return HttpResponse(t.render(c))
'''

@csrf_exempt
@authenticate
def create(request):

    session = request.session
    username = session['login_email']
    user = db.read_user(username)
    message = ""

    def give_create_form(username, message):
        form = gatewayforms.CreateAG()
        t = loader.get_template('gateway_templates/create_acquisition_gateway.html')
        c = RequestContext(request, {'username':username,'form':form, 'message':message})
        return HttpResponse(t.render(c))

    if request.POST:
        # Validate input forms
        form = gatewayforms.CreateAG(request.POST, request.FILES)
        if form.is_valid():
            kwargs = {}

            # Try and load JSON config file/data, if present
            if "json_config" in request.FILES:
                if request.FILES['json_config'].multiple_chunks():
                    message = "Uploaded file too large; please make smaller than 2.5M"
                    return give_create_form(username, message)
                config = request.FILES['json_config'].read()
                try:
                    kwargs['json_config'] = json.loads(config)
                except Exception as e:
                    logging.info("Possible JSON load error: %s" % e)
                    try:
                        kwargs['json_config'] = json.loads("\"" + config + "\"")
                    except Exception as e:
                        logging.error("Definite JSON load error %s" % e)
                        message = "Error parsing given JSON text."
                        return give_create_form(username, message)
            elif "json_config_text" in form.cleaned_data:
                try:
                    kwargs['json_config'] = json.loads(form.cleaned_data['json_config_text'])
                except Exception as e:
                    logging.info("Possible JSON load error: %s" % e)
                    try:
                        kwargs['json_config'] = json.loads("\"" + str(form.cleaned_data['json_config_text']) + "\"")
                    except Exception as e:
                        logging.error("Definite JSON load error %s" % e)
                        message = "Error parsing given JSON text."
                        return give_create_form(username, message)


            try:
                kwargs['ms_username'] = form.cleaned_data['g_name']
                kwargs['port'] = form.cleaned_data['port']
                kwargs['host'] = form.cleaned_data['host']
                kwargs['ms_password'] = form.cleaned_data['g_password']
                new_ag = db.create_acquisition_gateway(user, **kwargs)
            except Exception as E:
                message = "AG creation error: %s" % E
                return give_create_form(username, message)

            session['new_change'] = "Your new gateway is ready."
            session['next_url'] = '/syn/AG/allgateways'
            session['next_message'] = "Click here to see your acquisition gateways."
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
            form = gatewayforms.CreateAG(initial={'g_name': oldname,
                                       'host': oldhost,
                                       'port': oldport,
                                       'json_config_text':oldjson,
                                       })
            t = loader.get_template('gateway_templates/create_acquisition_gateway.html')
            c = RequestContext(request, {'username':username,'form':form, 'message':message})
            return HttpResponse(t.render(c))

    else:
        # Not a POST, give them blank form
        return give_create_form(username, message)

@csrf_exempt
@authenticate
def delete(request, g_name):

    def give_delete_form(username, g_name, message):
        form = gatewayforms.DeleteGateway()
        t = loader.get_template('gateway_templates/delete_acquisition_gateway.html')
        c = RequestContext(request, {'username':username, 'g_name':g_name, 'form':form, 'message':message})
        return HttpResponse(t.render(c))

    session = request.session
    username = session['login_email']
    user = db.read_user(username)
    message = ""

    ag = db.read_acquisition_gateway(g_name)
    if not ag:
        t = loader.get_template('gateway_templates/delete_acquisition_gateway_failure.html')
        c = RequestContext(request, {'username':username, 'g_name':g_name})
        return HttpResponse(t.render(c))

    '''
    if ag.owner_id != user.owner_id:
                t = loader.get_template('gateway_templates/delete_acquisition_gateway_failure.html')
                c = RequestContext(request, {'username':username, 'g_name':g_name})
                return HttpResponse(t.render(c))
    '''
    if request.POST:
        # Validate input forms
        form = gatewayforms.DeleteGateway(request.POST)
        if form.is_valid():
            if not AG.authenticate(ag, form.cleaned_data['g_password']):
                message = "Incorrect Acquisition Gateway password"
                return give_delete_form(username, g_name, message)
            if not form.cleaned_data['confirm_delete']:
                message = "You must tick the delete confirmation box."
                return give_delete_form(user, g_name, message)
            
            db.delete_acquisition_gateway(g_name)
            session['new_change'] = "Your gateway has been deleted."
            session['next_url'] = '/syn/AG/allgateways'
            session['next_message'] = "Click here to see all acquisition gateways."
            return HttpResponseRedirect('/syn/thanks/')

        # invalid forms
        else:  
            # Prep error message
            message = "Invalid form entry: "

            for k, v in form.errors.items():
                message = message + "\"" + k + "\"" + " -> " 
                for m in v:
                    message = message + m + " "

            return give_delete_form(username, g_name, message)
    else:
        # Not a POST, give them blank form
        return give_delete_form(username, g_name, message)

@csrf_exempt
@authenticate
def urlcreate(request, g_name, g_password, host, port,volume_name="",):
    session = request.session
    username = session['login_email']
    user = db.read_user(username)

    kwargs = {}

    kwargs['port'] = int(port)
    kwargs['host'] = host
    kwargs['ms_username'] = g_name
    kwargs['ms_password'] = g_password
    if volume_name:
        vol = db.read_volume(volume_name)
        if not vol:
            return HttpResponse("No volume %s exists." % volume_name)
        if (vol.volume_id not in user.volumes_r) and (vol.volume_id not in user.volumes_rw):
            return HttpResponse("Must have read rights to volume %s to create AG for it." % volume_name)
        kwargs['volume_ids'] = [vol.volume_id]

    try:
        new_ag = db.create_acquisition_gateway(user, **kwargs)
    except Exception as E:
        return HttpResponse("AG creation error: %s" % E)

    return HttpResponse("AG succesfully created: " + str(new_ag))

@csrf_exempt
@authenticate
def urldelete(request, g_name, g_password):
    session = request.session
    username = session['login_email']
    user = db.read_user(username)

    ag = db.read_acquisition_gateway(g_name)
    if not ag:
        return HttpResponse("AG %s does not exist." % g_name)
    #if ag.owner_id != user.owner_id:
    #    return HttpResponse("You must own this AG to delete it.")
    if not AG.authenticate(ag, g_password):
        return HttpResponse("Incorrect AG password.")
    db.delete_acquisition_gateway(g_name)
    return HttpResponse("Gateway succesfully deleted.")