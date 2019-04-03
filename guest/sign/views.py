from django.shortcuts import render
from django.http import HttpResponse,HttpResponseRedirect
from django.contrib import auth
from django.contrib.auth.decorators import  login_required


# Create your views here.
# def index(request123):
#    return HttpResponse("Hello Django!")

def index(request123):
    return render(request123, "index.html")

def login_action(request123):
    if request123.method == 'POST':
        username = request123.POST.get('username','')
        password = request123.POST.get('password','')
        '''
		if username == 'admin' and password == 'admin123':
            #return HttpResponseRedirect('/event_manage/')
            response = HttpResponseRedirect('/event_manage')
            #response.set_cookie('user',username,3600) # add cookie to browser
            request123.session['user'] = username
            return response
		'''

        user = auth.authenticate(username=username,password=password)
        if user is not None:
            auth.login(request123,user) # login
            request123.session['user'] = username
            response = HttpResponseRedirect('/event_manage/')
            return response
		
        else :
            return render(request123,'index.html',{'error':'username or password error!'})

@login_required
def event_manage(request123):
    #return render(request123,"event_manage.html")
    #username = request123.COOKIES.get('user','')
    username = request123.session.get('user','')
    return render(request123,"event_manage.html",{"user":username})