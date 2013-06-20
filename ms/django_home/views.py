from django.http import HttpResponse, HttpResponseRedirect
from django.template import Context, loader


import forms

from storage.storage import create_volume, create_user, create_user_gateway, create_msentry
from MS.volume import Volume
from MS.user import SyndicateUser as User
from MS.gateway import UserGateway as UG,  ReplicaGateway as RG, AcquisitionGateway as AG


# We might want to remove all the redirec_fiel_name things...
def start(request):
    t = loader.get_template('start.html')
    c = Context({})
    return HttpResponse(t.render(c))

#@login_required
def home(request):
    session = request.session

    t = loader.get_template('loggedin/home.html')
    c = Context({'session':session})
    return HttpResponse(t.render(c))

#@login_required
def allvolumes(request):
    vols = Volume.query()
    t = loader.get_template('loggedin/allvolumes.html')
    # myvol = Queryset.getvols(user=user)
#    myvol = ['vol1', 'vol2']
    c = Context({'vols':vols})
    return HttpResponse(t.render(c))

#@login_required
def myvolumes(request):
    vols = Volume.query()
    #vols = vols.filter(user=user)
    t = loader.get_template('loggedin/myvolumes.html')
    c = Context({'vols':vols})
    return HttpResponse(t.render(c))

#@login_required
def settings(request):
    t = loader.get_template('loggedin/settings.html')
    # myvol = Queryset.getvols(user=user)
    c = Context({})
    return HttpResponse(t.render(c))

#@login_required
def downloads(request):
    t = loader.get_template('loggedin/downloads.html')
    c = Context({})
    return HttpResponse(t.render(c))

def createvolume(request):
    message = ""

    if request.method == "POST":

        form = forms.CreateVolume(request.POST)
        if form.is_valid():

            name = form.cleaned_data['name']
            blocksize = form.cleaned_data['blocksize']
            description = form.cleaned_data['description']
            password = form.cleaned_data['password']
            salt = "I'M NOT SURE WHAT SHOULD GO HERE"

            try:
                session = request.session
                user = session['user']
            except:
                pass

            vol = create_volume(user, blocksize, description, password, salt)

            return HttpResponseRedirect('/syn/home/myvolumes/' + str(vol['id']))

        else:
            message = "Invalid Form. Please try again."

    form = forms.CreateVolume()
    t = loader.get_template('loggedin/createvolume.html')
    c = Context({'form':form, 'message':message})
    return HttpResponse(t.render(c))
