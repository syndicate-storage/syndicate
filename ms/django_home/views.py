from django.http import HttpResponse
from django.template import Context, loader
from django.contrib.auth.decorators import login_required
from MS import volume, user, gateway
# We might want to remove all the redirec_fiel_name things...
def start(request):
    t = loader.get_template('start.html')
    c = Context({})
    return HttpResponse(t.render(c))

#@login_required
def home(request):
    t = loader.get_template('loggedin/home.html')
    c = Context({})
    return HttpResponse(t.render(c))

#@login_required
def allvolumes(request):
    t = loader.get_template('loggedin/allvolumes.html')
    # myvol = Queryset.getvols(user=user)
    myvol = ['vol1', 'vol2']
    c = Context({'myvols':myvol})
    return HttpResponse(t.render(c))

#@login_required
def myvolumes(request):
    t = loader.get_template('loggedin/myvolumes.html')
    # myvol = Queryset.getvols(user=user)
    myvol = ['vol1', 'vol2']
    c = Context({'myvols':myvol})
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

