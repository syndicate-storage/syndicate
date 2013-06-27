from django.http import HttpResponse, HttpResponseRedirect
from django.template import Context, loader, RequestContext
from django.forms.formsets import formset_factory

from django_lib.auth import authenticate
import django_home.forms as forms
from django_volume.forms import ChangePassword, Password

from storage.storagetypes import transactional

import storage.storage as db
from MS.volume import Volume
from MS.user import SyndicateUser as User
from MS.gateway import UserGateway as UG,  ReplicaGateway as RG, AcquisitionGateway as AG

@authenticate
def thanks(request):
    session = request.session
    username = session['login_email']
    new_change = session['new_change']
    next_url = session['next_url']
    next_message = session['next_message']
    t = loader.get_template('thanks.html')
    c = Context({'username':username, 'new_change':new_change, 'next_url':next_url, 'next_message':next_message})
    return HttpResponse(t.render(c))


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

    attrs = {}
    attrs['Volume.volume_id'] = ".IN(%s)" % str(user.volumes_o)
    myvols = db.list_volumes(**attrs)

    all_users = []

    for v in myvols:
        uattrs = {}
        users_set = []
        uattrs['SyndicateUser.volumes_rw'] = "== %s" % v.volume_id 
        q = db.list_users(**uattrs)
        for u in q:
            users_set.append(u)
        uattrs = {}
        uattrs['SyndicateUser.volumes_r'] = "== %s" % v.volume_id 
        q = db.list_users(**uattrs)
        for u in q:
            users_set.append(u)
        all_users.append(users_set)
            

    vols_users =zip(myvols, all_users)
    t = loader.get_template('myvolumes.html')
    c = Context({'username':username, 'vols':vols_users})
    return HttpResponse(t.render(c))

@authenticate
def settings(request, message=""):
    session = request.session
    username = session['login_email']

    pass_form = ChangePassword()
    password = Password()

    t = loader.get_template('settings.html')
    c = RequestContext(request, {'username':username,
                                 'pass_form':pass_form,
                                 'message':message,
                                 } )
    return HttpResponse(t.render(c))

@authenticate
def changepassword(request, username):
    session = request.session
    # should be made 'fresh'
    user = db.read_user(username)

    if not request.POST:
        return HttpResponseRedirect('/syn/settings')

    form = ChangePassword(request.POST)
    if not form.is_valid():
        message = "You must fill out all password fields."
        return settings(request, message)
    else:
        # Check password hash
        hash_check = Volume.generate_password_hash(form.cleaned_data['oldpassword'], vol.volume_secret_salt)
        if hash_check != vol.volume_secret_salted_hash:
            message = "Incorrect password."
            return settings(request, message)
        elif form.cleaned_data['newpassword_1'] != form.cleaned_data['newpassword_2']:
            message = "Your new passwords did not match each other."
            return settings(request, message)

    # Ok change password 
    '''
    kwargs = {}
    new_volume_secret_salt, new_volume_secret_salted_hash = Volume.generate_volume_secret(form.cleaned_data['newpassword_1'])
    kwargs['volume_secret_salted_hash'] = new_volume_secret_salted_hash
    kwargs['volume_secret_salt'] = new_volume_secret_salt
    db.update_volume(volume_name, **kwargs)'''
    session['new_change'] = "We've changed your volume's password."
    session['next_url'] = '/syn/volume/' + volume_name
    session['next_message'] = "Click here to go back to your volume."
    return HttpResponseRedirect('/syn/thanks')




@authenticate
def downloads(request):
    session = request.session
    username = session['login_email']
    t = loader.get_template('downloads.html')
    c = Context({'username':username})
    return HttpResponse(t.render(c))

@transactional(xg=True)
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

            session['new_change'] = "Your new volume is ready."
            session['next_url'] = '/syn/myvolumes/'
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
