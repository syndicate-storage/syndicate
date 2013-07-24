import logging

from django.http import HttpResponseRedirect, HttpResponse
import storage.storage as db
from django.template import Context, loader
from django.shortcuts import redirect

from MS.user import SyndicateUser as User
from MS.gateway import AcquisitionGateway as AG
from MS.gateway import UserGateway as UG
from MS.gateway import ReplicaGateway as RG

from django_lib import forms as libforms

def precheck(g_type, redirect_view):

    '''
    Wrapper function to simplify verifying existence of gateways
    and correct passwords when modifying gateways in all gateway views.

    All wrappend functions need to take the following args:

    + g_type is the type of gateway, either 'AG' 'UG' or 'RG'
    + redirect_view is the location to be redirected

    - request
    - g_id
    '''

    # Three decorator types

    def ag_gateway_precheck(f):

        def ag_wrapper(request, g_id):

            if not request.POST:
                return redirect('django_ag.views.viewgateway', g_id=g_id)

            session = request.session
            username = session['login_email']
            try:
                g = db.read_acquisition_gateway(g_id)
                if not g:
                    raise Exception("No gateway exists.")
            except Exception as e:
                logging.error("Error reading gateway %s : Exception: %s" % (g_id, e))
                message = "No acquisition gateway by the name of %s exists." % g_id
                t = loader.get_template("gateway_templates/viewgateway_failure.html")
                c = Context({'message':message, 'username':username})
                return HttpResponse(t.render(c))

            form = libforms.Password(request.POST)
            if not form.is_valid():
                session['message'] = "Password required."
                return redirect(redirect_view, g_id)
            # Check password hash
            if not AG.authenticate(g, form.cleaned_data['password']):
                session['message'] = "Incorrect password."
                return redirect(redirect_view, g_id)

            return f(request, g_id)

        return ag_wrapper

    def ug_gateway_precheck(f):
        
        def ug_wrapper(request, g_id):
            
            if not request.POST:
                return redirect('django_ug.views.viewgateway', g_id=g_id)

            session = request.session
            username = session['login_email']

            try:
                g = db.read_user_gateway(g_id)
                if not g:
                    raise Exception("No gateway exists.")
            except Exception as e:
                logging.error("Error reading gateway %s : Exception: %s" % (g_id, e))
                message = "No user gateway by the name of %s exists." % g_id
                t = loader.get_template("gateway_templates/viewgateway_failure.html")
                c = Context({'message':message, 'username':username})
                return HttpResponse(t.render(c))

            form = libforms.Password(request.POST)
            if not form.is_valid():
                session['message'] = "Password required."
                return redirect(redirect_view, g_id)
            # Check password hash
            if not UG.authenticate(g, form.cleaned_data['password']):
                session['message'] = "Incorrect password."
                return redirect(redirect_view, g_id)

            return f(request, g_id)

        return ug_wrapper


    def rg_gateway_precheck(f):
        
        def rg_wrapper(request, g_id):
            
            if not request.POST:
                return redirect('django_rg.views.viewgateway', g_id=g_id)

            session = request.session
            username = session['login_email']
            try:
                g = db.read_replica_gateway(g_id)
                if not g:
                    raise Exception("No gateway exists.")
            except Exception as e:
                logging.error("Error reading gateway %s : Exception: %s" % (g_id, e))
                message = "No replica gateway by the name of %s exists." % g_id
                t = loader.get_template("gateway_templates/viewgateway_failure.html")
                c = Context({'message':message, 'username':username})
                return HttpResponse(t.render(c))

            form = libforms.Password(request.POST)
            if not form.is_valid():
                session['message'] = "Password required."
                return redirect(redirect_view, g_id)
            # Check password hash
            if not RG.authenticate(g, form.cleaned_data['password']):
                session['message'] = "Incorrect password."
                return redirect(redirect_view, g_id)

            return f(request, g_id)

        return rg_wrapper


    # Pythonesque case statement to determine what type of decorator to return.

    decorators = {"AG": ag_gateway_precheck,
               "RG": rg_gateway_precheck,
               "UG": ug_gateway_precheck
    }


    # Executed code

    try:
        return decorators[g_type]
    except KeyError:
        logging.error("Gatway type argument %s for decorators.precheck doesn't exist." % g_type)
        return HttpResponseRedirect('/syn/home')