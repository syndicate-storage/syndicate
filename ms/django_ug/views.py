'''
We're going to assume you need to be logged in already and have a valid session
for this to work.
'''
import logging

from django_lib.decorators import precheck
from django_lib.auth import authenticate
from django_lib import gatewayforms
from django_lib import forms as libforms
from django.http import HttpResponse, HttpResponseRedirect
from django.template import Context, loader, RequestContext


from django.views.decorators.csrf import csrf_exempt


from django.shortcuts import redirect

from storage.storagetypes import transactional
import storage.storage as db
from MS.volume import Volume
from MS.user import SyndicateUser as User
from MS.gateway import UserGateway as UG

PRECHECK_REDIRECT = 'django_ug.views.viewgateway'

@authenticate
def viewgateway(request, g_name=""):
    session = request.session
    username = session['login_email']
    user = db.read_user(username)
    message = session.pop('message', "")
    logging.info("JSDKFJDLFJDLSFJSDLK")
    logging.info(message)

    try:
        g = db.read_user_gateway(g_name)
        if not g:
            raise Exception("No gateway exists.")
    except Exception as e:
        logging.error("Error reading gateway %s : Exception: %s" % (g_name, e))
        message = "No user gateway by the name of %s exists." % g_name
        t = loader.get_template("gateway_templates/viewgateway_failure.html")
        c = Context({'message':message, 'username':username})
        return HttpResponse(t.render(c))

    location_form = gatewayforms.ModifyGatewayLocation(initial={'host':g.host,
                                                                'port':g.port})
    change_form = gatewayforms.ChangeVolume()

    vol = db.get_volume(g.volume_id)
    if not vol:
        logging.error("Volume ID in gateways volume_ids does not map to volume. Gateway: %s" % g_name)
        return redirect('django_ug.views.allgateways')
    
    attrs = {"SyndicateUser.owner_id ==": vol.owner_id}
    owner = db.get_user(attrs)
    logging.info(owner)
    logging.info(vol.owner_id)

    password_form = libforms.Password()
    change_password_form = libforms.ChangePassword()

    t = loader.get_template("gateway_templates/viewusergateway.html")
    c = RequestContext(request, {'username':username,
                        'gateway':g,
                        'message':message,
                        'vol':vol,
                        'owner':owner,
                        'location_form':location_form,
                        'change_form':change_form,
                        'password_form':password_form,
                        'change_password_form':change_password_form})
    return HttpResponse(t.render(c))

# Doesn't use precheck() because doesn't use Password() form, just ChangePassword() form.
@authenticate
def changepassword(request, g_name):
    session = request.session
    username = session['login_email']
    user = db.read_user(username)

    if request.method != "POST":
        return HttpResponseRedirect('/syn/UG/viewgateway' + g_name)

    try:
        g = db.read_user_gateway(g_name)
        if not g:
            raise Exception("No gateway exists.")
    except Exception as e:
        logging.error("Error reading gateway %s : Exception: %s" % (g_name, e))
        message = "No user gateway by the name of %s exists." % g_name
        t = loader.get_template("gateway_templates/viewgateway_failure.html")
        c = Context({'message':message, 'username':username})
        return HttpResponse(t.render(c))

    form = libforms.ChangePassword(request.POST)
    if not form.is_valid():
        session['message'] = "You must fill out all password fields."
        return redirect('django_ug.views.viewgateway', g_name)
    else:
        # Check password hash
        if not UG.authenticate(g, form.cleaned_data['oldpassword']):
            session['message'] = "Incorrect password."
            return redirect('django_ug.views.viewgateway', g_name)
        elif form.cleaned_data['newpassword_1'] != form.cleaned_data['newpassword_2']:
            session['message'] = "Your new passwords did not match each other."
            return redirect('django_ug.views.viewgateway', g_name)
        # Ok to change password
        else:
            new_hash = UG.generate_password_hash(form.cleaned_data['newpassword_1'])
            fields = {'ms_password_hash':new_hash}
            try:
                db.update_user_gateway(g_name, **fields)
            except Exception as e:
                logging.error("Unable to update user gateway %s. Exception %s" % (g_name, e))
                session['message'] = "Unable to update gateway."
                return redirect('django_ug.views.viewgateway', g_name)

            session['new_change'] = "We've changed your gateways's password."
            session['next_url'] = '/syn/UG/viewgateway/' + g_name
            session['next_message'] = "Click here to go back to your volume."
            return HttpResponseRedirect('/syn/thanks')

@authenticate
@precheck("UG", PRECHECK_REDIRECT)
def changelocation(request, g_name):
    session = request.session

    form = gatewayforms.ModifyGatewayLocation(request.POST)
    if form.is_valid():
        new_host = form.cleaned_data['host']
        new_port = form.cleaned_data['port']
        fields = {'host':new_host, 'port':new_port}
        try:
            db.update_user_gateway(g_name, **fields)
        except Exception as e:
            logging.error("Unable to update UG: %s. Error was %s." % (g_name, e))
            session['message'] = "Error. Unable to change user gateway."
            return redirect('django_ug.views.viewgateway', g_name)
        session['new_change'] = "We've updated your UG."
        session['next_url'] = '/syn/UG/viewgateway/' + g_name
        session['next_message'] = "Click here to go back to your gateway."
        return HttpResponseRedirect('/syn/thanks')
    else:
        session['message'] = "Invalid form entries for gateway location."
        return redirect('django_ug.views.viewgateway', g_name)

@authenticate
@precheck("UG", PRECHECK_REDIRECT)
def changevolume(request, g_name):

    session = request.session

    form = gatewayforms.ChangeVolume(request.POST)
    if form.is_valid():
        new_vol = db.read_volume(form.cleaned_data['volume_name'])
        if not new_vol:
            session['message'] = "No volume %s exists." % form.cleaned_data['volume_name']
            return redirect('django_ug.views.viewgateway', g_name)
        if (new_vol.volume_id not in user.volumes_r) and (new_vol.volume_id not in user.volumes_rw):
            session['message'] = "Must have read rights to volume %s to assign UG to it." % form.cleaned_data['volume_name']
            return redirect('django_ug.views.viewgateway', g_name)
        fields = {"volume_id":new_vol.volume_id}
        try:
            db.update_user_gateway(g_name, **fields)
        except Exception, e:
            logging.error("Unable to update UG: %s. Error was %s." % (g_name, e))
            session['message'] = "Error. Unable to change user gateway."
            return redirect('django_ug.views.viewgateway', g_name)
        session['new_change'] = "We've updated your UG."
        session['next_url'] = '/syn/UG/viewgateway/' + g_name
        session['next_message'] = "Click here to go back to your gateway."
        return HttpResponseRedirect('/syn/home')

@authenticate
@precheck("UG", PRECHECK_REDIRECT)
def changewrite(request, g_name):
    session = request.session
    

    g = db.read_user_gateway(g_name)
    if g.read_write:
        attrs = {'read_write':False}
    else:
        attrs = {'read_write':True}
    try:
        db.update_user_gateway(g_name, **attrs)
    except Exception as e:
        logging.error("Unable to update UG: %s. Error was %s." % (g_name, e))
        session['message'] = "Error. Unable to change user gateway."
        return redirect('django_ug.views.viewgateway', g_name)
    session['new_change'] = "We've updated your UG."
    session['next_url'] = '/syn/UG/viewgateway/' + g_name
    session['next_message'] = "Click here to go back to your gateway."
    return HttpResponseRedirect('/syn/thanks')


@authenticate
def allgateways(request):
    session = request.session
    username = session['login_email']

    try:
        qry = db.list_user_gateways()
    except:
        qry = []
    gateways = []
    for g in qry:
        gateways.append(g)
    vols = []
    for g in gateways:
        vols.append(db.get_volume(g.volume_id))
    owners = []
    for v in vols:
        volume_owner = v.owner_id
        attrs = {"SyndicateUser.owner_id ==":volume_owner}
        owners.append(db.get_user(attrs))
    gateway_vols_owners = zip(gateways, vols, owners)
    t = loader.get_template('gateway_templates/allusergateways.html')
    c = RequestContext(request, {'username':username, 'gateway_vols_owners':gateway_vols_owners})
    return HttpResponse(t.render(c))

@authenticate
def mygateways(request):
    session = request.session
    username = session['login_email']
    user = db.read_user(username)

    # should change this
    try:
        attrs = {"UserGateway.owner_id ==":user.owner_id}
        qry = db.list_user_gateways(attrs)
    except:
        qry = []
    gateways = []
    for g in qry:
        gateways.append(g)
    vols = []
    for g in gateways:
        vols.append(db.get_volume(g.volume_id))
    gateway_vols = zip(gateways, vols)
    t = loader.get_template('gateway_templates/myusergateways.html')
    c = RequestContext(request, {'username':username, 'gateway_vols':gateway_vols})
    return HttpResponse(t.render(c))


@csrf_exempt
@authenticate
def create(request):

    session = request.session
    username = session['login_email']
    user = db.read_user(username)

    def give_create_form(username, session):
        message = session.pop('message', "")
        form = gatewayforms.CreateUG()
        t = loader.get_template('gateway_templates/create_user_gateway.html')
        c = RequestContext(request, {'username':username,'form':form, 'message':message})
        return HttpResponse(t.render(c))

    if request.POST:
        # Validate input forms
        form = gatewayforms.CreateUG(request.POST)
        if form.is_valid():
            vol = db.read_volume(form.cleaned_data['volume_name'])
            if not vol:
                session['message'] = "No volume %s exists." % form.cleaned_data['volume_name']
                return give_create_form(username, session)
            if (vol.volume_id not in user.volumes_r) and (vol.volume_id not in user.volumes_rw):
                session['message'] = "Must have read rights to volume %s to create UG for it." % form.cleaned_data['volume_name']
                return give_create_form(username, session)
            try:
                kwargs = {}
                kwargs['read_write'] = form.cleaned_data['read_write']
                kwargs['ms_username'] = form.cleaned_data['g_name']
                kwargs['port'] = form.cleaned_data['port']
                kwargs['host'] = form.cleaned_data['host']
                kwargs['ms_password'] = form.cleaned_data['g_password']
                new_ug = db.create_user_gateway(user, vol, **kwargs)
            except Exception as E:
                session['message'] = "UG creation error: %s" % E
                return give_create_form(username, session)

            session['new_change'] = "Your new gateway is ready."
            session['next_url'] = '/syn/UG/mygateways'
            session['next_message'] = "Click here to see your gateways."
            return HttpResponseRedirect('/syn/thanks/')
        else:
            # Prep returned form values (so they don't have to re-enter stuff)

            if 'g_name' in form.errors:
                oldname = ""
            else:
                oldname = request.POST['g_name']
            if 'volume_name' in form.errors:
                oldvolume_name = ""
            else:
                oldvolume_name = request.POST['volume_name']
            if 'host' in form.errors:
                oldhost = ""
            else:
                oldhost = request.POST['host']
            if 'port' in form.errors:
                oldport = ""
            else:
                oldport = request.POST['port']

            # Prep error message
            message = "Invalid form entry: "

            for k, v in form.errors.items():
                message = message + "\"" + k + "\"" + " -> " 
                for m in v:
                    message = message + m + " "

            # Give then the form again
            form = gatewayforms.CreateUG(initial={'g_name': oldname,
                                       'volume_name': oldvolume_name,
                                       'host': oldhost,
                                       'port': oldport,
                                       })
            t = loader.get_template('gateway_templates/create_user_gateway.html')
            c = RequestContext(request, {'username':username,'form':form, 'message':message})
            return HttpResponse(t.render(c))

    else:
        # Not a POST, give them blank form
        return give_create_form(username, session)

@csrf_exempt
@authenticate
def delete(request, g_name):

    def give_delete_form(username, g_name, session):
        message = session.pop('message' "")
        form = gatewayforms.DeleteGateway()
        t = loader.get_template('gateway_templates/delete_user_gateway.html')
        c = RequestContext(request, {'username':username, 'g_name':g_name, 'form':form, 'message':message})
        return HttpResponse(t.render(c))

    session = request.session
    username = session['login_email']

    ug = db.read_user_gateway(g_name)
    if not ug:
        t = loader.get_template('gateway_templates/delete_user_gateway_failure.html')
        c = RequestContext(request, {'username':username, 'g_name':g_name})
        return HttpResponse(t.render(c))

    if ug.owner_id != user.owner_id:
                t = loader.get_template('gateway_templates/delete_user_gateway_failure.html')
                c = RequestContext(request, {'username':username, 'g_name':g_name})
                return HttpResponse(t.render(c))

    if request.POST:
        # Validate input forms
        form = gatewayforms.DeleteGateway(request.POST)
        if form.is_valid():
            if not UG.authenticate(ug, form.cleaned_data['g_password']):
                session['message'] = "Incorrect User Gateway password"
                return give_delete_form(username, g_name, session)
            if not form.cleaned_data['confirm_delete']:
                session['message'] = "You must tick the delete confirmation box."
                return give_delete_form(username, g_name, session)
            
            db.delete_user_gateway(g_name)
            session['new_change'] = "Your gateway has been deleted."
            session['next_url'] = '/syn/UG/mygateways'
            session['next_message'] = "Click here to see your gateways."
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
def urlcreate(request, volume_name, g_name, g_password, host, port, read_write):
    session = request.session
    username = session['login_email']
    user = db.read_user(username)

    kwargs = {}

    kwargs['port'] = int(port)
    kwargs['host'] = host
    kwargs['ms_username'] = g_name
    kwargs['ms_password'] = g_password
    kwargs['read_write'] = read_write
    vol = db.read_volume(volume_name)
    if not vol:
        return HttpResponse("No volume %s exists." % volume_name)
    if (vol.volume_id not in user.volumes_r) and (vol.volume_id not in user.volumes_rw):
        return HttpResponse("Must have read rights to volume %s to create UG for it." % volume_name)

    try:
        new_ug = db.create_user_gateway(user, vol, **kwargs)
    except Exception as E:
        return HttpResponse("UG creation error: %s" % E)

    return HttpResponse("UG succesfully created: " + str(new_ug))

@csrf_exempt
@authenticate
def urldelete(request, g_name, g_password):
    session = request.session
    username = session['login_email']
    user = db.read_user(username)

    ug = db.read_user_gateway(g_name)
    if not ug:
        return HttpResponse("UG %s does not exist." % g_name)
    if ug.owner_id != user.owner_id:
        return HttpResponse("You must own this UG to delete it.")
    if not UG.authenticate(ug, g_password):
        return HttpResponse("Incorrect UG password.")
    db.delete_user_gateway(g_name)
    return HttpResponse("Gateway succesfully deleted.")
